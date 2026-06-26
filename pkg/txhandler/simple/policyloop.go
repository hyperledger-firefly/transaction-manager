// Copyright © 2024 - 2025 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simple

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs" // replace with your own messages if you are developing a customized transaction handler
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

const metricsGaugeTransactionsInflightUsed = "tx_in_flight_used_total"
const metricsGaugeTransactionsInflightUsedDescription = "Number of transactions currently in flight"

const metricsGaugeTransactionsInflightFree = "tx_in_flight_free_total"
const metricsGaugeTransactionsInflightFreeDescription = "Number of transactions left in the in flight queue"

type policyEngineAPIRequestType int

const (
	ActionNone policyEngineAPIRequestType = iota
	ActionDelete
	ActionSuspend
	ActionResume
	ActionUpdate
)

type policyEngineAPIRequest struct {
	requestType policyEngineAPIRequestType
	txUpdates   apitypes.TXUpdatesExternal
	txID        string
	startTime   time.Time
	response    chan policyEngineAPIResponse
}

// policyEngineAPIRequest requests are queued to the policy engine thread for processing against a given Transaction
type policyEngineAPIResponse struct {
	tx     *apitypes.ManagedTX
	err    error
	status int // http status code (200 Ok vs. 202 Accepted) - only set for success cases
}

func (sth *simpleTransactionHandler) policyLoop() {
	defer close(sth.policyLoopDone)
	ctx := log.WithLogFields(sth.ctx, "role", "policyloop")
	ticker := time.NewTicker(sth.policyLoopInterval)

	for {
		// Wait to be notified, or timeout to run
		select {
		case <-sth.inflightUpdate:
		case <-ticker.C:
		case <-ctx.Done():
			ticker.Stop()
			log.L(ctx).Infof("Receipt poller exiting")
			return
		}
		// Pop whether we were marked stale
		stale := false
		select {
		case <-sth.inflightStale:
			stale = true
		default:
		}
		sth.policyLoopCycle(ctx, stale)
	}
}

func (sth *simpleTransactionHandler) markInflightStale() {
	// First mark that we're stale
	select {
	case sth.inflightStale <- true:
	default:
	}
	// Then ensure we queue a loop that picks up the stale marker
	sth.markInflightUpdate()
}

func (sth *simpleTransactionHandler) markInflightUpdate() {
	select {
	case sth.inflightUpdate <- true:
	default:
	}
}

func (sth *simpleTransactionHandler) updateInflightSet(ctx context.Context) bool {
	sth.inflightRWMux.Lock()
	defer sth.inflightRWMux.Unlock()

	oldInflight := sth.inflight
	sth.inflight = make([]*pendingState, 0, len(oldInflight))

	// Run through removing those that are removed
	for _, p := range oldInflight {
		if !p.remove {
			sth.inflight = append(sth.inflight, p)
		} else {
			sth.incTransactionOperationCounter(ctx, p.mtx.Namespace(ctx), "removed")
		}
	}

	// If we are not at maximum, then query if there are more candidates now
	spaces := sth.maxInFlight - len(sth.inflight)
	log.L(sth.ctx).Tracef("Number of spaces left '%v'", spaces)
	if spaces > 0 {
		var after string
		if len(sth.inflight) > 0 {
			after = sth.inflight[len(sth.inflight)-1].mtx.SequenceID
		}
		var additional []*apitypes.ManagedTX
		// We retry the get from persistence indefinitely (until the context cancels)
		err := sth.retry.Do(ctx, "get pending transactions", func(_ int) (retry bool, err error) {
			additional, err = sth.toolkit.TXPersistence.ListTransactionsPending(ctx, after, spaces, 0)
			return true, err
		})
		if err != nil {
			log.L(ctx).Infof("Policy loop context cancelled while retrying")
			return false
		}
		for _, mtx := range additional {
			sth.incTransactionOperationCounter(ctx, mtx.Namespace(ctx), "polled")
			// Note at this point we don't have a receipt, even if one is already stored.
			// So if this was already submitted, we rely on the transactionHash and go
			// re-request the receipt.
			var info simplePolicyInfo
			_ = json.Unmarshal(mtx.PolicyInfo.Bytes(), &info)
			inflight := &pendingState{
				mtx:       mtx,
				info:      &info,
				subStatus: apitypes.TxSubStatusTracking,
			}
			if mtx.TransactionHash == "" {
				inflight.subStatus = apitypes.TxSubStatusReceived
			}
			sth.inflight = append(sth.inflight, inflight)
		}
		newLen := len(sth.inflight)
		if newLen > 0 {
			log.L(ctx).Debugf("Inflight set updated with %d additional transactions, length is now %d head-id:%s head-seq=%s tail-id:%s tail-seq=%s old-tail=%s", len(additional), len(sth.inflight), sth.inflight[0].mtx.ID, sth.inflight[0].mtx.SequenceID, sth.inflight[newLen-1].mtx.ID, sth.inflight[newLen-1].mtx.SequenceID, after)
		}
	}
	sth.setTransactionInflightQueueMetrics(ctx)
	return true

}

func (sth *simpleTransactionHandler) policyLoopCycle(ctx context.Context, inflightStale bool) {
	log.L(ctx).Tracef("policyLoopCycle triggered inflightStatle=%v", inflightStale)

	// Process any synchronous commands first - these might not be in our inflight set
	sth.processPolicyAPIRequests(ctx)

	if inflightStale {
		if !sth.updateInflightSet(ctx) {
			return
		}
	}

	sth.inflightRWMux.RLock()
	defer sth.inflightRWMux.RUnlock()
	// Go through executing the policy engine against them
	for _, pending := range sth.inflight {
		log.L(ctx).Tracef("Executing policy against tx-id=%v", pending.mtx.ID)
		err := sth.execPolicy(ctx, pending, nil)
		if err != nil {
			log.L(ctx).Errorf("Failed policy cycle transaction=%s operation=%s: %s", pending.mtx.TransactionHash, pending.mtx.ID, err)
		}
	}

}

func (sth *simpleTransactionHandler) getTransactionByID(ctx context.Context, txID string) (transaction *apitypes.ManagedTX, err error) {
	tx, err := sth.toolkit.TXPersistence.GetTransactionByID(ctx, txID)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, i18n.NewError(ctx, tmmsgs.MsgTransactionNotFound, txID)
	}
	return tx, nil
}

// processPolicyAPIRequests executes any API calls requested that require policy engine involvement - such as transaction deletions
func (sth *simpleTransactionHandler) processPolicyAPIRequests(ctx context.Context) {

	sth.mux.Lock()
	requests := sth.policyEngineAPIRequests
	if len(requests) > 0 {
		sth.policyEngineAPIRequests = []*policyEngineAPIRequest{}
	}
	sth.mux.Unlock()

	for _, request := range requests {
		var pending *pendingState

		sth.inflightRWMux.RLock()
		// If this transaction is in-flight, we use that record
		for _, inflight := range sth.inflight {
			if inflight != nil && inflight.mtx != nil && inflight.mtx.ID == request.txID {
				pending = inflight
				break
			}
		}
		sth.inflightRWMux.RUnlock()
		// If this transaction is in-flight, we use that record
		if pending == nil {
			mtx, err := sth.getTransactionByID(ctx, request.txID)
			if err != nil {
				request.response <- policyEngineAPIResponse{err: err, status: http.StatusInternalServerError}
				continue
			}
			// This transaction was valid, but outside of our in-flight set - we still evaluate the policy engine in-line for it.
			// This does NOT cause it to be added to the in-flight set
			pending = &pendingState{mtx: mtx, subStatus: apitypes.TxSubStatusReceived}
		}

		switch request.requestType {
		case ActionDelete, ActionSuspend, ActionResume, ActionUpdate:
			if err := sth.execPolicy(ctx, pending, request); err != nil {
				request.response <- policyEngineAPIResponse{err: err, status: http.StatusInternalServerError}
			} else {
				res := policyEngineAPIResponse{tx: pending.mtx, status: http.StatusAccepted}
				if pending.remove || request.requestType == ActionResume || request.requestType == ActionUpdate /* always sync */ {
					res.status = http.StatusOK // synchronously completed
				}
				request.response <- res
			}
		default:
			request.response <- policyEngineAPIResponse{
				err: i18n.NewError(ctx, tmmsgs.MsgTransactionHandlerRequestInvalid, request.requestType),
			}
		}
	}

}

func (sth *simpleTransactionHandler) pendingToRunContext(baseCtx context.Context, pending *pendingState, syncRequest *policyEngineAPIRequest) (ctx *RunContext, err error) {

	// Take a snapshot of the pending state under the lock
	pending.mux.Lock()
	defer pending.mux.Unlock()

	mtx := pending.mtx
	ctx = &RunContext{
		Context:       baseCtx,
		TX:            mtx,
		Confirmed:     pending.confirmed,
		Confirmations: pending.confirmations,
		Receipt:       pending.receipt,
		Info:          pending.info,
	}
	confirmNotify := pending.confirmNotify
	receiptNotify := pending.receiptNotify
	if syncRequest != nil {
		ctx.SyncAction = syncRequest.requestType
		if syncRequest.requestType == ActionUpdate {
			if syncRequest.txUpdates.GasPrice != nil {
				return nil, i18n.NewError(ctx, tmmsgs.MsgTxHandlerUnsupportedFieldForUpdate, "gasPrice")
			}
			txUpdates, updated, valueChangeMap := mtx.ApplyExternalTxUpdates(syncRequest.txUpdates)
			if updated {
				// persist the updated transaction information
				// and process the transaction in the policy loop cycle
				ctx.TXUpdates = txUpdates
				ctx.UpdateType = Update
				// Record the valueChangeMap as the info json
				infoJSON, _ := json.Marshal(valueChangeMap)
				ctx.AddSubStatusAction(apitypes.TxActionExternalUpdate, fftypes.JSONAnyPtr(string(infoJSON)), nil, fftypes.Now())
				ctx.ProcessTx = true
			}
		}
	}

	if ctx.SyncAction == ActionDelete && mtx.DeleteRequested == nil {
		mtx.DeleteRequested = fftypes.Now()
		ctx.UpdateType = Update // might change to delete later
		ctx.TXUpdates.DeleteRequested = mtx.DeleteRequested
		ctx.ProcessTx = true
	}

	// Process any state updates that were queued to us from notifications from the confirmation manager
	if receiptNotify != nil {
		log.L(ctx).Debugf("Receipt received for transaction %s at nonce %s / %d - hash: %s", pending.mtx.ID, pending.mtx.TransactionHeaders.From, pending.mtx.Nonce.Int64(), pending.mtx.TransactionHash)
		if err := sth.toolkit.TXPersistence.SetTransactionReceipt(ctx, mtx.ID, ctx.Receipt); err != nil {
			return nil, err
		}
		ctx.AddSubStatusAction(apitypes.TxActionReceiveReceipt, fftypes.JSONAnyPtr(`{"protocolId":"`+ctx.Receipt.ProtocolID+`"}`), nil, fftypes.Now())
		sth.incTransactionOperationCounter(ctx, pending.mtx.Namespace(ctx), "received_receipt")

		// Clear the notification (as long as no other came through)
		if pending.receiptNotify == receiptNotify {
			pending.receiptNotify = nil
		}
	}

	if confirmNotify != nil && ctx.Confirmations != nil {
		log.L(ctx).Debugf("Confirmed transaction %s at nonce %s / %d - hash: %s", pending.mtx.ID, pending.mtx.TransactionHeaders.From, pending.mtx.Nonce.Int64(), pending.mtx.TransactionHash)
		if err := sth.toolkit.TXPersistence.AddTransactionConfirmations(ctx, mtx.ID, ctx.Confirmations.NewFork, ctx.Confirmations.Confirmations...); err != nil {
			return nil, err
		}
		if ctx.Confirmed {
			ctx.AddSubStatusAction(apitypes.TxActionConfirmTransaction, nil, nil, fftypes.Now())
			ctx.SetSubStatus(apitypes.TxSubStatusConfirmed)
		}

		// Clear the notification (as long as no other came through)
		if pending.confirmNotify == confirmNotify {
			pending.confirmNotify = nil
		}
	}

	return ctx, nil
}

func (sth *simpleTransactionHandler) execPolicy(baseCtx context.Context, pending *pendingState, syncRequest *policyEngineAPIRequest) (err error) {

	ctx, err := sth.pendingToRunContext(baseCtx, pending, syncRequest)
	if err != nil {
		return err
	}
	mtx := ctx.TX

	completed := false
	switch {
	case ctx.Confirmed && ctx.SyncAction != ActionDelete:
		log.L(sth.ctx).Tracef("Transaction '%s' confirmed", ctx.TX.ID)
		completed = true
		ctx.UpdateType = Update
		if ctx.Receipt != nil && ctx.Receipt.Success {
			mtx.Status = apitypes.TxStatusSucceeded
			ctx.TXUpdates.Status = &mtx.Status
		} else {
			mtx.Status = apitypes.TxStatusFailed
			ctx.TXUpdates.Status = &mtx.Status
		}
	case ctx.SyncAction == ActionSuspend:
		// Whole cycle is a no-op if we're not pending
		if mtx.Status == apitypes.TxStatusPending {
			ctx.UpdateType = Update
			completed = true // drop it out of the loop
			mtx.Status = apitypes.TxStatusSuspended
			ctx.TXUpdates.Status = &mtx.Status
		}
	case ctx.SyncAction == ActionResume:
		// Whole cycle is a no-op if we're not suspended
		if mtx.Status == apitypes.TxStatusSuspended {
			ctx.UpdateType = Update
			mtx.Status = apitypes.TxStatusPending
			ctx.TXUpdates.Status = &mtx.Status
		}
	default:
		// We get woken for lots of reasons to go through the policy loop, but we only want
		// to drive the policy engine at regular intervals.
		// So we track the last time we ran the policy engine against each pending item.
		// We always call the policy engine on every loop, when transaction update affects
		// the in-flight transaction process. (ctx.ProcessTx is set to true)
		if ctx.ProcessTx || time.Since(pending.lastPolicyCycle) > sth.policyLoopInterval {
			// Pass the state to the pluggable policy engine to potentially perform more actions against it,
			// such as submitting for the first time, or raising the gas etc.

			policyError := sth.processTransaction(ctx)
			if policyError != nil {
				log.L(ctx).Errorf("Policy engine returned error for transaction %s: %s", mtx.ID, policyError)
				ctx.UpdateType = Update
				errMsg := policyError.Error()
				// Keep storing the latest error message onto the TX (sub-status updates handled in the processTransaction handler)
				mtx.ErrorMessage = errMsg
				ctx.TXUpdates.ErrorMessage = &errMsg
			} else {
				log.L(ctx).Debugf("Policy engine executed for tx %s (update=%d,status=%s,hash=%s)", mtx.ID, ctx.UpdateType, mtx.Status, mtx.TransactionHash)
				if mtx.FirstSubmit != nil && pending.trackingTransactionHash != mtx.TransactionHash {

					if pending.trackingTransactionHash != "" {
						// if had a previous transaction hash, emit an event to for transaction hash removal
						eventTX := *mtx
						eventTX.TransactionHash = pending.trackingTransactionHash // using the previous hash
						if err = sth.toolkit.EventHandler.HandleEvent(ctx, apitypes.ManagedTransactionEvent{
							Type: apitypes.ManagedTXTransactionHashRemoved,
							Tx:   &eventTX,
						}); err != nil {
							log.L(ctx).Infof("Error detected notifying confirmation manager to remove old transaction hash: %s", err.Error())
						}
					}

					// If now submitted, add to confirmations manager for receipt checking
					err = sth.toolkit.EventHandler.HandleEvent(ctx, apitypes.ManagedTransactionEvent{
						Type: apitypes.ManagedTXTransactionHashAdded,
						Tx:   mtx,
					})
					if err != nil {
						log.L(ctx).Infof("Error detected notifying confirmation manager to add new transaction hash: %s", err.Error())

						sth.incTransactionOperationCounter(ctx, mtx.Namespace(ctx), "tracking_failed")
					} else {
						pending.trackingTransactionHash = mtx.TransactionHash
						sth.incTransactionOperationCounter(ctx, mtx.Namespace(ctx), "tracking")
					}
				}
				pending.lastPolicyCycle = time.Now()
			}
		}
	}
	return sth.flushChanges(ctx, pending, completed)
}

func (sth *simpleTransactionHandler) flushChanges(ctx *RunContext, pending *pendingState, completed bool) (err error) {
	// flush any sub-status changes
	pending.subStatus = ctx.SubStatus
	for _, historyUpdate := range ctx.HistoryUpdates {
		if err := historyUpdate(sth.toolkit.TXHistory); err != nil {
			return err
		}
	}
	mtx := ctx.TX

	// flush any transaction update
	switch ctx.UpdateType {
	case Update:
		if ctx.UpdatedInfo {
			infoBytes, _ := json.Marshal(ctx.Info)
			ctx.TXUpdates.PolicyInfo = fftypes.JSONAnyPtrBytes(infoBytes)
		}
		err := sth.toolkit.TXPersistence.UpdateTransaction(ctx, ctx.TX.ID, &ctx.TXUpdates)
		if err != nil {
			log.L(ctx).Errorf("Failed to update transaction %s (status=%s): %s", mtx.ID, mtx.Status, err)
			return err
		}
		if ctx.SyncAction == ActionResume {
			log.L(ctx).Infof("Transaction %s resumed", mtx.ID)
			sth.markInflightStale() // this won't be in the in-flight set, so we need to pull it in if there's space
		} else if completed {
			pending.remove = true // for the next time round the loop
			log.L(ctx).Infof("Transaction %s removed from tracking (status=%s): %s", mtx.ID, mtx.Status, err)
			sth.markInflightStale()

			// if and only if the transaction is now resolved dispatch an event to event handler
			// and discard any handling errors.
			// Note that TxStatusSuspended has no action here.
			if mtx.Status == apitypes.TxStatusSucceeded {
				_ = sth.toolkit.EventHandler.HandleEvent(ctx, apitypes.ManagedTransactionEvent{
					Type:    apitypes.ManagedTXProcessSucceeded,
					Tx:      mtx,
					Receipt: ctx.Receipt,
				})
			} else if mtx.Status == apitypes.TxStatusFailed {
				_ = sth.toolkit.EventHandler.HandleEvent(ctx, apitypes.ManagedTransactionEvent{
					Type:    apitypes.ManagedTXProcessFailed,
					Tx:      mtx,
					Receipt: ctx.Receipt,
				})
			}
		}
	case Delete:
		err := sth.toolkit.TXPersistence.DeleteTransaction(ctx, mtx.ID)
		if err != nil {
			log.L(ctx).Errorf("Failed to delete transaction %s (status=%s): %s", mtx.ID, mtx.Status, err)
			return err
		}
		pending.remove = true // for the next time round the loop
		sth.markInflightStale()
		// dispatch an event to event handler
		// and discard any handling errors
		_ = sth.toolkit.EventHandler.HandleEvent(ctx, apitypes.ManagedTransactionEvent{
			Type: apitypes.ManagedTXDeleted,
			Tx:   mtx,
		})
	}

	return nil
}

func (sth *simpleTransactionHandler) policyEngineAPIRequest(ctx context.Context, req *policyEngineAPIRequest) policyEngineAPIResponse {
	sth.mux.Lock()
	sth.policyEngineAPIRequests = append(sth.policyEngineAPIRequests, req)
	sth.mux.Unlock()
	sth.markInflightUpdate()
	req.response = make(chan policyEngineAPIResponse, 1)
	req.startTime = time.Now()
	select {
	case res := <-req.response:
		return res
	case <-ctx.Done():
		return policyEngineAPIResponse{
			err: i18n.NewError(ctx, tmmsgs.MsgTransactionHandlerRequestTimeout, time.Since(req.startTime).Seconds()),
		}
	}
}

func (sth *simpleTransactionHandler) HandleTransactionConfirmations(ctx context.Context, txID string, notification *apitypes.ConfirmationsNotification) (err error) {
	// Will be picked up on the next policy loop cycle
	sth.inflightRWMux.RLock()
	var pending *pendingState
	for _, p := range sth.inflight {
		if p != nil && p.mtx != nil && p.mtx.ID == txID {
			pending = p
			break
		}
	}
	sth.inflightRWMux.RUnlock()
	if pending == nil {
		err = i18n.NewError(ctx, tmmsgs.MsgTransactionNotFound, txID)
		return
	}
	pending.mux.Lock()
	pending.confirmed = notification.Confirmed
	pending.confirmNotify = fftypes.Now()
	pending.confirmations = notification
	pending.mux.Unlock()
	log.L(ctx).Infof("Received %d confirmations (resync=%t)", len(notification.Confirmations), notification.NewFork)

	sth.markInflightUpdate()
	return
}
func (sth *simpleTransactionHandler) HandleTransactionReceiptReceived(ctx context.Context, txID string, receipt *ffcapi.TransactionReceiptResponse) (err error) {
	log.L(ctx).Tracef("Handle transaction receipt received %s", txID)
	sth.inflightRWMux.RLock()
	var pending *pendingState
	for _, p := range sth.inflight {
		if p != nil && p.mtx != nil && p.mtx.ID == txID {
			pending = p
			break
		}
	}
	sth.inflightRWMux.RUnlock()
	if pending == nil {
		err = i18n.NewError(ctx, tmmsgs.MsgTransactionNotFound, txID)
		return
	}
	pending.mux.Lock()
	// Will be picked up on the next policy loop cycle - guaranteed to occur before Confirmed
	pending.receiptNotify = fftypes.Now()
	pending.receipt = receipt
	pending.mux.Unlock()
	// Will be picked up on the next policy loop cycle - guaranteed to occur before Confirmed
	sth.markInflightUpdate()
	return
}
