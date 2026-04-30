// Copyright © 2025 Kaleido, Inc.
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

package confirmations

import (
	"container/list"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/retry"
	"github.com/hyperledger/firefly-transaction-manager/internal/metrics"
	"github.com/hyperledger/firefly-transaction-manager/internal/tmconfig"
	"github.com/hyperledger/firefly-transaction-manager/internal/tmmsgs"
	"github.com/hyperledger/firefly-transaction-manager/pkg/apitypes"
	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
)

// Manager listens to the blocks on the chain, and attributes confirmations to
// pending events. Once those events meet a threshold they are considered final and
// dispatched to the relevant listener.
type Manager interface {
	Notify(n *Notification) error
	Start()
	Stop()
	GetReceiveChannel() chan<- *ffcapi.BlockHashEvent
	CheckInFlight(listenerID *fftypes.UUID) bool
	StartConfirmedBlockListener(ctx context.Context, id *fftypes.UUID, fromBlock string, checkpoint *ffcapi.BlockListenerCheckpoint, eventStream chan<- *ffcapi.ListenerEvent) error
	StopConfirmedBlockListener(ctx context.Context, id *fftypes.UUID) error
}

type NotificationType string

const (
	NewEventLog        NotificationType = "newEventLog"
	RemovedEventLog    NotificationType = "removedEventLog"
	NewTransaction     NotificationType = "newTransaction"
	RemovedTransaction NotificationType = "removedTransaction"
	ListenerRemoved    NotificationType = "listenerRemoved"
	receiptArrived     NotificationType = "receiptArrived"
)

type Notification struct {
	NotificationType NotificationType
	Event            *EventInfo                         // NewEventLog, RemovedEventLog
	Transaction      *TransactionInfo                   // NewTransaction, RemovedTransaction
	RemovedListener  *RemovedListenerInfo               // ListenerRemoved
	pending          *pendingItem                       // receiptArrived
	receipt          *ffcapi.TransactionReceiptResponse // receiptArrived
}

type EventInfo struct {
	ID            *ffcapi.EventID
	Confirmations func(ctx context.Context, notification *apitypes.ConfirmationsNotification)
}

type TransactionInfo struct {
	TransactionHash string
	Receipt         func(ctx context.Context, receipt *ffcapi.TransactionReceiptResponse)
	Confirmations   func(ctx context.Context, notification *apitypes.ConfirmationsNotification)
}

type RemovedListenerInfo struct {
	ListenerID *fftypes.UUID
	Completed  chan struct{}
}

type blockConfirmationManager struct {
	baseContext           context.Context
	ctx                   context.Context
	cancelFunc            func()
	newBlockHashEvents    chan *ffcapi.BlockHashEvent
	connector             ffcapi.API
	blockListenerStale    bool
	metricsEmitter        metrics.ConfirmationMetricsEmitter
	requiredConfirmations uint64
	chainTrackingMode     ffcapi.ChainTrackingMode
	staleReceiptTimeout   time.Duration
	bcmNotifications      chan *Notification
	highestBlockSeen      uint64
	headBlockNumber       uint64 // used for confirmation when the connector is running in light mode
	pending               map[string]*pendingItem
	pendingMux            sync.Mutex
	receiptChecker        *receiptChecker
	retry                 *retry.Retry
	cblLock               sync.Mutex
	cbls                  map[fftypes.UUID]*confirmedBlockListener
	fetchReceiptUponEntry bool
	done                  chan struct{}
}

func NewBlockConfirmationManager(baseContext context.Context, connector ffcapi.API, desc string,
	cme metrics.ConfirmationMetricsEmitter) Manager {
	bcm := &blockConfirmationManager{
		baseContext:           baseContext,
		connector:             connector,
		cbls:                  make(map[fftypes.UUID]*confirmedBlockListener),
		blockListenerStale:    true,
		requiredConfirmations: config.GetUint64(tmconfig.ConfirmationsRequired),
		chainTrackingMode:     connector.GetChainTrackingMode(baseContext),
		staleReceiptTimeout:   config.GetDuration(tmconfig.ConfirmationsStaleReceiptTimeout),
		bcmNotifications:      make(chan *Notification, config.GetInt(tmconfig.ConfirmationsNotificationQueueLength)),
		pending:               make(map[string]*pendingItem),
		newBlockHashEvents:    make(chan *ffcapi.BlockHashEvent, config.GetInt(tmconfig.ConfirmationsBlockQueueLength)),
		metricsEmitter:        cme,
		retry: &retry.Retry{
			InitialDelay: config.GetDuration(tmconfig.ConfirmationsRetryInitDelay),
			MaximumDelay: config.GetDuration(tmconfig.ConfirmationsRetryMaxDelay),
			Factor:       config.GetFloat64(tmconfig.ConfirmationsRetryFactor),
		},
		fetchReceiptUponEntry: config.GetBool(tmconfig.ConfirmationsFetchReceiptUponEntry),
	}
	bcm.ctx, bcm.cancelFunc = context.WithCancel(baseContext)
	// add a log context for this specific confirmation manager (as there are many within the )
	bcm.ctx = log.WithLogField(bcm.ctx, "role", fmt.Sprintf("confirmations_%s", desc))
	return bcm
}

type pendingType int

const (
	pendingTypeEvent pendingType = iota
	pendingTypeTransaction
)

// pendingItem could be a specific event that has been detected, but not confirmed yet.
// Or it could be a transaction
type pendingItem struct {
	pType                     pendingType
	added                     time.Time
	notifiedConfirmations     []*apitypes.Confirmation
	confirmations             []*apitypes.Confirmation
	scheduledAtLeastOnce      bool
	confirmed                 bool
	previousConfirmationCount *uint64       // headBlockNumber mode: last dispatched CurrentConfirmationCount
	queuedStale               *list.Element // protected by receiptChecker mux
	lastReceiptCheck          time.Time     // protected by receiptChecker mux
	receiptCallback           func(ctx context.Context, receipt *ffcapi.TransactionReceiptResponse)
	confirmationsCallback     func(ctx context.Context, notification *apitypes.ConfirmationsNotification)
	transactionHash           string
	blockHash                 string        // can be notified of changes to this for receipts
	blockNumber               uint64        // known at creation time for event logs
	transactionIndex          uint64        // known at creation time for event logs
	logIndex                  uint64        // events only
	listenerID                *fftypes.UUID // events only
}

func pendingKeyForTX(txHash string) string {
	return fmt.Sprintf("TX:th=%s", txHash)
}

func (pi *pendingItem) getKey() string {
	switch pi.pType {
	case pendingTypeEvent:
		// For events they are identified by their hash, blockNumber, transactionIndex and logIndex
		// If any of those change, it's a new new event - and as such we should get informed of it separately by the blockchain connector.
		return fmt.Sprintf("Event:%.12d/%.6d/%.6d,l=%s,th=%s,bh=%s", pi.blockNumber, pi.transactionIndex, pi.logIndex, pi.listenerID, pi.transactionHash, pi.blockHash)
	case pendingTypeTransaction:
		// For transactions, it's simply the transaction hash that identifies it. It can go into any block
		return pendingKeyForTX(pi.transactionHash)
	default:
		panic("invalid pending item type")
	}
}

func (n *Notification) eventPendingItem() *pendingItem {
	return &pendingItem{
		pType:                 pendingTypeEvent,
		listenerID:            n.Event.ID.ListenerID,
		blockNumber:           n.Event.ID.BlockNumber.Uint64(),
		blockHash:             n.Event.ID.BlockHash,
		transactionHash:       n.Event.ID.TransactionHash,
		transactionIndex:      n.Event.ID.TransactionIndex.Uint64(),
		logIndex:              n.Event.ID.LogIndex.Uint64(),
		confirmationsCallback: n.Event.Confirmations,
	}
}

func (n *Notification) transactionPendingItem() *pendingItem {
	return &pendingItem{
		pType:                 pendingTypeTransaction,
		lastReceiptCheck:      time.Now(),
		transactionHash:       n.Transaction.TransactionHash,
		receiptCallback:       n.Transaction.Receipt,
		confirmationsCallback: n.Transaction.Confirmations,
	}
}

type pendingItems []*pendingItem

func (pi pendingItems) Len() int      { return len(pi) }
func (pi pendingItems) Swap(i, j int) { pi[i], pi[j] = pi[j], pi[i] }
func (pi pendingItems) Less(i, j int) bool {
	// At the point we emit the confirmations, we ensure to sort them by:
	// - Block number
	// - Transaction index within the block
	// - Log index within the transaction (only for events)
	return pi[i].blockNumber < pi[j].blockNumber ||
		(pi[i].blockNumber == pi[j].blockNumber && (pi[i].transactionIndex < pi[j].transactionIndex ||
			(pi[i].transactionIndex == pi[j].transactionIndex && pi[i].logIndex < pi[j].logIndex)))
}

type blockState struct {
	bcm       *blockConfirmationManager
	blocks    map[uint64]*apitypes.BlockInfo
	lowestNil uint64
}

func (bcm *blockConfirmationManager) Start() {
	bcm.done = make(chan struct{})
	bcm.receiptChecker = newReceiptChecker(bcm, config.GetInt(tmconfig.ConfirmationsReceiptWorkers), bcm.metricsEmitter)
	go bcm.confirmationsListener()
}

func (bcm *blockConfirmationManager) Stop() {
	if bcm.done != nil {
		bcm.cancelFunc()
		bcm.receiptChecker.close()
		bcm.receiptChecker = nil
		<-bcm.done
		bcm.done = nil
		// Reset context ready for restart
		bcm.ctx, bcm.cancelFunc = context.WithCancel(bcm.baseContext)
	}
	for _, cbl := range bcm.copyCBLsList() {
		_ = bcm.StopConfirmedBlockListener(bcm.ctx, cbl.id)
	}
}

func (bcm *blockConfirmationManager) GetReceiveChannel() chan<- *ffcapi.BlockHashEvent {
	return bcm.newBlockHashEvents
}

// Notify is used to notify the confirmation manager of detection of a new logEntry addition or removal
func (bcm *blockConfirmationManager) Notify(n *Notification) error {
	startTime := time.Now()
	switch n.NotificationType {
	case NewEventLog, RemovedEventLog:
		if n.Event == nil || n.Event.ID.ListenerID == nil || n.Event.ID.TransactionHash == "" || n.Event.ID.BlockHash == "" {
			log.L(bcm.ctx).Errorf("Invalid event notification: %+v", n)
			return i18n.NewError(bcm.ctx, tmmsgs.MsgInvalidConfirmationRequest, n)
		}
	case NewTransaction, RemovedTransaction:
		if n.Transaction == nil || n.Transaction.TransactionHash == "" {
			log.L(bcm.ctx).Errorf("Invalid transaction notification: %+v", n)
			return i18n.NewError(bcm.ctx, tmmsgs.MsgInvalidConfirmationRequest, n)
		}
	case ListenerRemoved:
		if n.RemovedListener == nil || n.RemovedListener.Completed == nil {
			log.L(bcm.ctx).Errorf("Invalid listener notification: %+v", n)
			return i18n.NewError(bcm.ctx, tmmsgs.MsgInvalidConfirmationRequest, n)
		}
	case receiptArrived:
		if n.pending == nil || n.receipt == nil {
			log.L(bcm.ctx).Errorf("Invalid receipt notification: %+v", n)
			return i18n.NewError(bcm.ctx, tmmsgs.MsgInvalidConfirmationRequest, n)
		}
	}
	select {
	case bcm.bcmNotifications <- n:
		bcm.metricsEmitter.RecordNotificationQueueingMetrics(bcm.ctx, string(n.NotificationType), time.Since(startTime).Seconds())
	case <-bcm.ctx.Done():
		log.L(bcm.ctx).Debugf("Shut down while queuing notification")
		return nil
	}
	return nil
}

func (bcm *blockConfirmationManager) CheckInFlight(listenerID *fftypes.UUID) bool {
	bcm.pendingMux.Lock()
	defer bcm.pendingMux.Unlock()
	for _, p := range bcm.pending {
		if listenerID.Equals(p.listenerID) {
			return true
		}
	}
	return false
}

func (bcm *blockConfirmationManager) getBlockByHash(blockHash string) (*apitypes.BlockInfo, error) {
	res, reason, err := bcm.connector.BlockInfoByHash(bcm.ctx, &ffcapi.BlockInfoByHashRequest{
		BlockHash: blockHash,
	})
	if err != nil {
		if reason == ffcapi.ErrorReasonNotFound {
			return nil, nil
		}
		return nil, err
	}
	blockInfo := transformBlockInfo(&res.BlockInfo)
	log.L(bcm.ctx).Debugf("Downloaded block header by hash: %d / %s parent=%s", blockInfo.BlockNumber, blockInfo.BlockHash, blockInfo.ParentHash)

	return blockInfo, nil
}

func (bcm *blockConfirmationManager) getBlockByNumber(blockNumber uint64, allowCache bool, expectedParentHash string) (*apitypes.BlockInfo, error) {
	res, reason, err := bcm.connector.BlockInfoByNumber(bcm.ctx, &ffcapi.BlockInfoByNumberRequest{
		//nolint:gosec
		BlockNumber:        fftypes.NewFFBigInt(int64(blockNumber)),
		AllowCache:         allowCache,
		ExpectedParentHash: expectedParentHash,
	})
	if err != nil {
		if reason == ffcapi.ErrorReasonNotFound {
			return nil, nil
		}
		return nil, err
	}
	blockInfo := transformBlockInfo(&res.BlockInfo)
	log.L(bcm.ctx).Debugf("Downloaded block header by number: %d / %s parent=%s", blockInfo.BlockNumber, blockInfo.BlockHash, blockInfo.ParentHash)
	return blockInfo, nil
}

func transformBlockInfo(res *ffcapi.BlockInfo) *apitypes.BlockInfo {
	return &apitypes.BlockInfo{
		BlockNumber:       fftypes.FFuint64(res.BlockNumber.Uint64()),
		BlockHash:         res.BlockHash,
		ParentHash:        res.ParentHash,
		TransactionHashes: res.TransactionHashes,
	}
}

func (bcm *blockConfirmationManager) copyCBLsList() []*confirmedBlockListener {
	bcm.cblLock.Lock()
	defer bcm.cblLock.Unlock()
	cbls := make([]*confirmedBlockListener, 0, len(bcm.cbls))
	for _, cbl := range bcm.cbls {
		cbls = append(cbls, cbl)
	}
	return cbls
}

func (bcm *blockConfirmationManager) propagateBlockHashToCBLs(bhe *ffcapi.BlockHashEvent) {
	bcm.cblLock.Lock()
	defer bcm.cblLock.Unlock()
	for _, cbl := range bcm.cbls {
		select {
		case cbl.newBlockHashes <- bhe:
		case <-cbl.processorDone:
		}
	}
}

func (bcm *blockConfirmationManager) confirmationsListener() {
	defer close(bcm.done)
	notifications := make([]*Notification, 0)
	blockHashes := make([]string, 0)
	triggerType := ""
	receivedFirstBlock := false
	for {
		select {
		case bhe := <-bcm.newBlockHashEvents:
			if bhe.GapPotential {
				bcm.blockListenerStale = true
			}
			blockHashes = append(blockHashes, bhe.BlockHashes...)
			bcm.headBlockNumber = bhe.HeadBlockNumber // always update the head block number, NOTE: the number can decrease during a re-org
			// Need to also pass this event to any confirmed block listeners
			// (they promise to always be efficient in handling these, having a go-routine
			// dedicated to spinning fast just processing those separate to dispatching them)
			bcm.propagateBlockHashToCBLs(bhe)

			if bhe.Created != nil {
				for i := 0; i < len(bhe.BlockHashes); i++ {
					bcm.metricsEmitter.RecordBlockHashQueueingMetrics(bcm.ctx, time.Since(*bhe.Created.Time()).Seconds())
				}
				log.L(bcm.ctx).Tracef("[TimeTrace] Confirmation listener added %d block hashes after they were queued for %s", len(bhe.BlockHashes), time.Since(*bhe.Created.Time()))
			}
			triggerType = "newBlockHashes"
		case <-bcm.ctx.Done():
			log.L(bcm.ctx).Debugf("Block confirmation listener stopping")
			return
		case notification := <-bcm.bcmNotifications:
			if notification.NotificationType == ListenerRemoved {
				// Handle listener notifications immediately
				bcm.listenerRemoved(notification)
				triggerType = "removeNotification"
			} else {
				// Defer until after we've got new logs
				notifications = append(notifications, notification)
				triggerType = "otherNotification"
			}
		}
		startTime := time.Now()

		// Each time round the loop we need to have a consistent view of the chain.
		// This view must not add later blocks (by number) in, or change the hash of blocks,
		// otherwise we could potentially deliver events out of order (receipts do not
		// have ordering assurances).
		blocks := bcm.newBlockState()

		if bcm.blockListenerStale {
			if bcm.chainTrackingMode == ffcapi.ChainTrackingModeFull {
				// only need to build the in memory partial chain when the block details are available
				if err := bcm.walkChain(blocks); err != nil {
					log.L(bcm.ctx).Errorf("Failed to walk chain after restoring blockListener: %s", err)
					continue
				}
			}
			bcm.blockListenerStale = false
		}

		blockHashCount := len(blockHashes)
		// Process each new block
		bcm.processBlockHashes(blockHashes)
		// Truncate the block hashes now we've processed them
		blockHashes = blockHashes[:0]

		notificationCount := len(notifications)

		// Process any new notifications - we do this at the end, so it can benefit
		// from knowing the latest highestBlockSeen
		// This returns the notifications that were not processed successfully so we store them
		var err error
		notifications, err = bcm.processNotifications(notifications, blocks)
		if err != nil {
			log.L(bcm.ctx).Errorf("Confirmation listener processed %d block hashes and failed processing notifications: %d/%d due to error %s, trigger type: %s", blockHashCount, len(notifications), notificationCount, err, triggerType)
			continue
		}
		scheduleAllTxReceipts := !receivedFirstBlock && blockHashCount > 0
		// Mark receipts stale after duration
		bcm.scheduleReceiptChecks(scheduleAllTxReceipts)
		receivedFirstBlock = receivedFirstBlock || blockHashCount > 0
		log.L(bcm.ctx).Tracef("[TimeTrace] Confirmation listener processed %d block hashes and %d notifications in %s, trigger type: %s", blockHashCount, notificationCount, time.Since(startTime), triggerType)

	}

}

func (bcm *blockConfirmationManager) scheduleReceiptChecks(receivedBlocksFirstTime bool) {
	now := time.Now()
	for _, pending := range bcm.pending {
		// For efficiency we do a dirty read on the receipt check time before going into the locking
		// check within the receipt checker
		if pending.pType == pendingTypeTransaction {
			if receivedBlocksFirstTime && !pending.scheduledAtLeastOnce {
				bcm.receiptChecker.schedule(pending, false)
			} else if now.Sub(pending.lastReceiptCheck) > bcm.staleReceiptTimeout {
				// schedule stale receipt checks
				bcm.receiptChecker.schedule(pending, true /* suspected timeout - prompts re-check in the lock */)
			}
		}
	}
}

// Returns the notifications that were not processed successfully so that the calling function can remove them
func (bcm *blockConfirmationManager) processNotifications(notifications []*Notification, blocks *blockState) ([]*Notification, error) {
	for i, n := range notifications {
		startTime := time.Now()
		switch n.NotificationType {
		case NewEventLog:
			newItem := n.eventPendingItem()
			bcm.addOrReplaceItem(newItem)
			if err := bcm.walkChainForItem(newItem, blocks); err != nil {
				// If we error, we should return the remaining notifications to be processed later
				// so that the calling function can remove the ones that were processed successfully
				// This still guarantees ordering of the notifications that were processed successfully
				return notifications[i:], err
			}
		case NewTransaction:
			newItem := n.transactionPendingItem()
			bcm.addOrReplaceItem(newItem)
			if bcm.fetchReceiptUponEntry {
				bcm.receiptChecker.schedule(newItem, false)
			}
		case RemovedEventLog:
			bcm.removeItem(n.eventPendingItem(), true)
		case RemovedTransaction:
			bcm.removeItem(n.transactionPendingItem(), true)
		case receiptArrived:
			bcm.dispatchReceipt(n.pending, n.receipt, blocks)
		default:
			// Note that streamStopped is handled in the polling loop directly
			log.L(bcm.ctx).Warnf("Unexpected notification type: %s", n.NotificationType)
		}
		bcm.metricsEmitter.RecordNotificationProcessMetrics(bcm.ctx, string(n.NotificationType), time.Since(startTime).Seconds())
	}

	// Clear the notifications slice
	return notifications[:0], nil
}

func (bcm *blockConfirmationManager) dispatchReceipt(pending *pendingItem, receipt *ffcapi.TransactionReceiptResponse, blocks *blockState) {
	pending.blockNumber = receipt.BlockNumber.Uint64()
	pending.blockHash = receipt.BlockHash
	log.L(bcm.ctx).Infof("Receipt for transaction %s downloaded. BlockNumber=%d BlockHash=%s", pending.transactionHash, pending.blockNumber, pending.blockHash)
	// Notify of the receipt
	if pending.receiptCallback != nil {
		pending.receiptCallback(bcm.ctx, receipt)
		bcm.metricsEmitter.RecordReceiptMetrics(bcm.ctx, time.Since(pending.added).Seconds())
		log.L(bcm.ctx).Tracef("[TimeTrace] Confirmation manager dispatched receipt for transaction %s after %s", pending.transactionHash, time.Since(pending.added))
	}

	// Need to walk the chain for this new receipt
	if err := bcm.walkChainForItem(pending, blocks); err != nil {
		log.L(bcm.ctx).Debugf("Failed to walk chain for transaction %s: %s", pending.transactionHash, err)
		return
	}
}

// listenerRemoved removes all pending work for a given listener, and notifies once done
func (bcm *blockConfirmationManager) listenerRemoved(notification *Notification) {
	bcm.pendingMux.Lock()
	defer bcm.pendingMux.Unlock()
	for pendingKey, pending := range bcm.pending {
		if notification.RemovedListener.ListenerID.Equals(pending.listenerID) {
			delete(bcm.pending, pendingKey)
		}
	}
	close(notification.RemovedListener.Completed)
}

// addEvent is called by the goroutine on receipt of a new event/transaction notification
func (bcm *blockConfirmationManager) addOrReplaceItem(pending *pendingItem) {
	bcm.pendingMux.Lock()
	defer bcm.pendingMux.Unlock()
	pending.added = time.Now()
	pending.confirmations = make([]*apitypes.Confirmation, 0, bcm.requiredConfirmations)
	pendingKey := pending.getKey()
	bcm.pending[pendingKey] = pending
	log.L(bcm.ctx).Infof("Added pending item %s", pendingKey)
}

// removeEvent is called by the goroutine on receipt of a remove event notification
func (bcm *blockConfirmationManager) removeItem(pending *pendingItem, stale bool) {
	pendingKey := pending.getKey()
	log.L(bcm.ctx).Debugf("Removing pending item %s (stale=%t)", pendingKey, stale)
	// note lock hierarchy is pendingMux->receiptChecker.mux
	bcm.pendingMux.Lock()
	delete(bcm.pending, pendingKey)
	if pending.pType == pendingTypeTransaction {
		bcm.receiptChecker.remove(pending)
	}
	bcm.pendingMux.Unlock()
}

func (bcm *blockConfirmationManager) processBlockHashes(blockHashes []string) {
	if bcm.chainTrackingMode == ffcapi.ChainTrackingModeLight {
		// for light chain tracking mode, no block details are available, only need to calculate the number of confirmations using head block number
		bcm.checkAndDispatchConfirmationsUsingBlockHeight()
		return
	}
	batchSize := len(blockHashes)
	if batchSize > 0 {
		log.L(bcm.ctx).Debugf("New block notifications %v", blockHashes)
		bcm.metricsEmitter.RecordBlockHashBatchSizeMetric(bcm.ctx, float64(batchSize))
	}

	for _, blockHash := range blockHashes {
		startTime := time.Now()
		// Get the block header
		block, err := bcm.getBlockByHash(blockHash)
		if err != nil || block == nil {
			log.L(bcm.ctx).Errorf("Failed to retrieve block %s: %v", blockHash, err)
			continue
		}

		// Process the block for confirmations
		bcm.processBlock(block)

		// Update the highest block (used for efficiency in chain walks)
		if block.BlockNumber.Uint64() > bcm.highestBlockSeen {
			bcm.highestBlockSeen = block.BlockNumber.Uint64()
		}
		bcm.metricsEmitter.RecordBlockHashProcessMetrics(bcm.ctx, time.Since(startTime).Seconds())

	}
}

func (bcm *blockConfirmationManager) processBlock(block *apitypes.BlockInfo) {

	// For any transactions in the block that are known to us, we need to mark them
	// stale to go query the receipt
	l := log.L(bcm.ctx)
	l.Debugf("Transactions mined in block %d / %s: %v", block.BlockNumber, block.BlockHash, block.TransactionHashes)
	bcm.pendingMux.Lock()
	for _, txHash := range block.TransactionHashes {
		txKey := pendingKeyForTX(txHash)
		if pending, ok := bcm.pending[txKey]; ok {
			if pending.blockHash != block.BlockHash {
				l.Infof("Detected transaction %s added to block %d / %s - receipt check scheduled", txHash, block.BlockNumber, block.BlockHash)
				bcm.receiptChecker.schedule(pending, false)
			}
		}
	}
	bcm.pendingMux.Unlock()

	// Go through all the events, adding in the confirmations, and popping any out
	// that have reached their threshold. Then drop the log before logging/processing them.
	blockNumber := block.BlockNumber.Uint64()
	var notifications pendingItems
	for pendingKey, pending := range bcm.pending {
		if pending.blockHash != "" {
			// The block might appear at any point in the confirmation list
			newConfirmations := false
			expectedParentHash := pending.blockHash
			expectedBlockNumber := pending.blockNumber + 1
			for i := 0; i < (len(pending.confirmations) + 1); i++ {
				l.Tracef("Comparing block number=%d parent=%s to %d / %s for %s", blockNumber, block.ParentHash, expectedBlockNumber, expectedParentHash, pendingKey)
				if block.ParentHash == expectedParentHash && blockNumber == expectedBlockNumber {
					pending.confirmations = append(pending.confirmations[0:i], &apitypes.Confirmation{
						BlockNumber: block.BlockNumber,
						BlockHash:   block.BlockHash,
						ParentHash:  block.ParentHash,
					})
					newConfirmations = true
					l.Infof("Confirmation %d at block %d / %s item=%s",
						len(pending.confirmations), block.BlockNumber, block.BlockHash, pending.getKey())
					break
				}
				if i < len(pending.confirmations) {
					expectedParentHash = pending.confirmations[i].BlockHash
				}
				expectedBlockNumber++
			}
			if uint64(len(pending.confirmations)) >= bcm.requiredConfirmations {
				pending.confirmed = true
			}
			if bcm.requiredConfirmations > 0 && (pending.confirmed || newConfirmations) {
				notifications = append(notifications, pending)
			}
		}
	}

	// Sort the events to dispatch them in the correct order
	sort.Sort(notifications)
	for _, item := range notifications {
		bcm.dispatchConfirmations(item)
	}

}

// dispatchConfirmed drive the event stream for any events that are confirmed, and prunes the state
func (bcm *blockConfirmationManager) dispatchConfirmations(item *pendingItem) {
	if item.confirmed {
		bcm.removeItem(item, false)
	}

	// We keep track of the last confirmation we sent, to allow the target code to be efficient
	// on how it stores the confirmations as they arrive. If it's just additive, it can just
	// write the additive confirmations as they arrive. If we've gone down a different fork,
	// or this is the first notification, then it needs to replace any previously persisted
	// list of confirmations with the new set.
	newFork := len(item.notifiedConfirmations) == 0 // first confirmation notification is always marked newFork
	notificationConfirmations := make([]*apitypes.Confirmation, 0, len(item.confirmations))
	for i, c := range item.confirmations {
		if !newFork && i < len(item.notifiedConfirmations) {
			newFork = c.BlockHash != item.notifiedConfirmations[i].BlockHash
			if newFork {
				// Notify of the full set
				notificationConfirmations = append([]*apitypes.Confirmation{}, item.confirmations...)
				break
			}
		} else {
			// Only notify of the additional ones
			notificationConfirmations = append(notificationConfirmations, c)
		}
	}

	confirmationCount := uint64(len(item.confirmations))
	if confirmationCount > bcm.requiredConfirmations {
		confirmationCount = bcm.requiredConfirmations
	}

	// Possible for us to re-dispatch the same confirmations, if we are notified about a block later after
	// after we previously did a crawl for blocks.
	// So we protect here against dispatching an empty array
	if len(notificationConfirmations) > 0 || item.confirmed {
		notification := &apitypes.ConfirmationsNotification{
			Confirmed:                item.confirmed,
			CurrentConfirmationCount: confirmationCount,
			TargetConfirmationCount:  bcm.requiredConfirmations,
			NewFork:                  newFork,
			Confirmations:            notificationConfirmations,
		}
		// Take a copy of the notification confirmations so we know what we have previously notified next time round
		// (not safe to keep a reference, in case it's modified by the callback).
		previouslyNotified := len(item.notifiedConfirmations)
		if newFork {
			item.notifiedConfirmations = append([]*apitypes.Confirmation{}, notificationConfirmations...)
		} else {
			item.notifiedConfirmations = append(item.notifiedConfirmations, notificationConfirmations...)
		}
		log.L(bcm.ctx).Infof("Confirmation notification item=%s confirmed=%t confirmations=%d newFork=%t previouslyNotified=%d",
			item.getKey(), notification.Confirmed, len(item.confirmations),
			notification.NewFork, previouslyNotified)
		item.confirmationsCallback(bcm.ctx, notification)
		bcm.metricsEmitter.RecordConfirmationMetrics(bcm.ctx, time.Since(item.added).Seconds())
		log.L(bcm.ctx).Tracef("[TimeTrace] Confirmation manager dispatched confirm event for transaction %s after %s", item.transactionHash, time.Since(item.added))
	}

}

// walkChain goes through each event and sees whether it's valid,
// purging any stale confirmations - or whole events if the blockListener is invalid
// We do this each time our blockListener is invalidated
func (bcm *blockConfirmationManager) walkChain(blocks *blockState) error {

	// Grab a copy of all the pending in order
	bcm.pendingMux.Lock()
	pendingItems := make(pendingItems, 0, len(bcm.pending))
	for _, pending := range bcm.pending {
		pendingItems = append(pendingItems, pending)
	}
	bcm.pendingMux.Unlock()
	sort.Sort(pendingItems)

	// Go through them in order, as we must deliver them in the order on the chain.
	// For the same reason we use a map _including misses_ of blocks:
	// Without this map we could deliver out of order:
	//  If a new block were to be mined+detected while we were traversing a long list,
	//  then only walking the chain for later events in the list would find the block.
	//  This means those later events would be delivered, but the earlier ones would not.
	for _, pending := range pendingItems {
		if err := bcm.walkChainForItem(pending, blocks); err != nil {
			return err
		}
	}

	return nil

}

func (bcm *blockConfirmationManager) newBlockState() *blockState {
	return &blockState{
		bcm:    bcm,
		blocks: make(map[uint64]*apitypes.BlockInfo),
	}
}

func (bs *blockState) getByNumber(blockNumber uint64, expectedParentHash string) (*apitypes.BlockInfo, error) {
	// blockState gives a consistent view of the chain throughout a cycle, where we perform a carefully ordered
	// set of actions against our pending items.
	// - We never return newer blocks after a query has been made that found a nil result at a lower block number
	// - We never change the hash of a block
	// If these changes happen during a cycle, we will pick them up on the next cycle rather than risk out-of-order
	// delivery of events by detecting them half way through.
	if bs.lowestNil > 0 && blockNumber >= bs.lowestNil {
		log.L(bs.bcm.ctx).Debugf("Block %d is after chain head (cached)", blockNumber)
		return nil, nil
	}
	block := bs.blocks[blockNumber]
	if block != nil {
		return block, nil
	}
	block, err := bs.bcm.getBlockByNumber(blockNumber, true, expectedParentHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		if bs.lowestNil == 0 || blockNumber <= bs.lowestNil {
			log.L(bs.bcm.ctx).Debugf("Block %d is after chain head", blockNumber)
			bs.lowestNil = blockNumber
		}
		return nil, nil
	}
	bs.blocks[blockNumber] = block
	return block, nil
}

func (bcm *blockConfirmationManager) walkChainForItem(pending *pendingItem, blocks *blockState) (err error) {

	if pending.blockHash == "" {
		// This is a transaction that we don't yet have the receipt for
		log.L(bcm.ctx).Debugf("Transaction %s still awaiting receipt", pending.transactionHash)
		return nil
	}

	if bcm.chainTrackingMode == ffcapi.ChainTrackingModeLight {
		return bcm.confirmationCheckUsingHeadBlockNumber(pending)
	}

	pendingKey := pending.getKey()

	blockNumber := pending.blockNumber + 1
	expectedParentHash := pending.blockHash
	pending.confirmations = pending.confirmations[:0]
	if bcm.requiredConfirmations == 0 {
		pending.confirmed = true
	} else {
		for {
			// No point in walking past the highest block we've seen via the notifier
			if bcm.highestBlockSeen > 0 && blockNumber > bcm.highestBlockSeen {
				log.L(bcm.ctx).Debugf("Waiting for confirmation after block %d event=%s", bcm.highestBlockSeen, pendingKey)
				break
			}
			block, err := blocks.getByNumber(blockNumber, expectedParentHash)
			if err != nil {
				return err
			}
			if block == nil {
				log.L(bcm.ctx).Infof("Block %d unavailable walking chain event=%s", blockNumber, pendingKey)
				break
			}
			candidateParentHash := block.ParentHash
			if candidateParentHash != expectedParentHash {
				log.L(bcm.ctx).Infof("Block mismatch in confirmations: block=%d expected=%s actual=%s confirmations=%d event=%s", blockNumber, expectedParentHash, candidateParentHash, len(pending.confirmations), pendingKey)
				break
			}
			pending.confirmations = append(pending.confirmations, apitypes.ConfirmationFromBlock(block))
			if uint64(len(pending.confirmations)) >= bcm.requiredConfirmations {
				pending.confirmed = true
				break
			}
			blockNumber++
			expectedParentHash = block.BlockHash
		}
	}
	// Notify the new confirmation set
	if pending.confirmed || len(pending.confirmations) > 0 {
		bcm.dispatchConfirmations(pending)
	}
	return nil

}

func (bcm *blockConfirmationManager) checkAndDispatchConfirmationsUsingBlockHeight() {
	bcm.pendingMux.Lock()
	items := make([]*pendingItem, 0, len(bcm.pending))
	for _, p := range bcm.pending {
		items = append(items, p)
	}
	bcm.pendingMux.Unlock()
	for _, p := range items {
		if err := bcm.confirmationCheckUsingHeadBlockNumber(p); err != nil {
			log.L(bcm.ctx).Errorf("Block height confirmation refresh failed for %s: %s", p.getKey(), err)
		}
	}
}

func (bcm *blockConfirmationManager) confirmationCheckUsingHeadBlockNumber(pending *pendingItem) error {
	if pending.blockHash == "" {
		// no receipt yet, so no confirmation check
		return nil
	}

	currentConfirmationCount := uint64(0)
	if bcm.headBlockNumber > pending.blockNumber {
		currentConfirmationCount = bcm.headBlockNumber - pending.blockNumber
	}

	return bcm.dispatchBlockHeightConfirmations(pending, currentConfirmationCount)

}

func (bcm *blockConfirmationManager) dispatchBlockHeightConfirmations(pending *pendingItem, count uint64) error {
	if pending.previousConfirmationCount != nil && count == *pending.previousConfirmationCount {
		// no ops as there isn't any changes detected
		return nil
	}

	confirmationCount := count
	if confirmationCount > bcm.requiredConfirmations {
		confirmationCount = bcm.requiredConfirmations
	}

	confirmed := confirmationCount == bcm.requiredConfirmations
	if confirmed {
		// do confirmation check here to ensure the transaction receipt is still valid
		res, reason, err := bcm.connector.TransactionReceipt(bcm.ctx, &ffcapi.TransactionReceiptRequest{
			TransactionHash: pending.transactionHash,
		})
		if err != nil {
			if reason == ffcapi.ErrorReasonNotFound {
				// need to schedule the receipt check again as the receipt is no longer valid
				pending.blockHash = ""
				pending.blockNumber = 0
				pending.previousConfirmationCount = nil
				bcm.receiptChecker.schedule(pending, true)
			}
			return err
		}
		if res == nil || res.BlockHash == "" {
			return i18n.NewError(bcm.ctx, tmmsgs.MsgTransactionReceiptMissingBlockHash)
		}
		if !strings.EqualFold(strings.ToLower(res.BlockHash), strings.ToLower(pending.blockHash)) {
			// set the new block hash and number and requeue for confirmation check
			pending.blockHash = res.BlockHash
			pending.blockNumber = res.BlockNumber.Uint64()
			pending.previousConfirmationCount = nil
			return i18n.NewError(bcm.ctx, tmmsgs.MsgTransactionReceiptBlockHashMismatch, pending.blockHash, res.BlockHash)
		}
	}
	// when not confirmed, we assume the receipt is still valid and return the actual confirmation count

	notification := &apitypes.ConfirmationsNotification{
		Confirmed:                confirmed,
		NewFork:                  pending.previousConfirmationCount != nil && count < *pending.previousConfirmationCount, // the only fork situation we can detect is a reduction in confirmation count
		CurrentConfirmationCount: confirmationCount,                                                                      // NOTE: we returns the actual confirmation count, which can be higher than the required confirmation count
		TargetConfirmationCount:  bcm.requiredConfirmations,
	}
	pending.previousConfirmationCount = &count

	log.L(bcm.ctx).Infof("Block height confirmation notification item=%s confirmed=%t count=%d/%d newFork=%t",
		pending.getKey(), notification.Confirmed, notification.CurrentConfirmationCount, notification.TargetConfirmationCount, notification.NewFork)
	pending.confirmationsCallback(bcm.ctx, notification)
	bcm.metricsEmitter.RecordConfirmationMetrics(bcm.ctx, time.Since(pending.added).Seconds())

	if confirmed {
		bcm.removeItem(pending, false)
	}
	return nil
}
