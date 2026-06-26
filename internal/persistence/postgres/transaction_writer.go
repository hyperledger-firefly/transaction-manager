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

package postgres

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

type transactionOperation struct {
	txID         string
	sentConflict bool
	done         chan error

	opID               string
	isShutdown         bool
	txInsert           *apitypes.ManagedTX
	noncePreAssigned   bool
	nextNonceCB        txhandler.NextNonceCallback
	txUpdate           *apitypes.TXUpdates
	txDelete           *string
	clearConfirmations bool
	confirmation       *apitypes.ConfirmationRecord
	receipt            *apitypes.ReceiptRecord
	historyRecord      *apitypes.TXHistoryRecord
}

type txCacheEntry struct {
	lastCompacted *fftypes.FFTime
}

type nonceCacheEntry struct {
	cachedTime *fftypes.FFTime
	nextNonce  uint64
}

type transactionWriter struct {
	p                   *sqlPersistence
	txMetaCache         *lru.Cache[string, *txCacheEntry]
	nextNonceCache      *lru.Cache[string, *nonceCacheEntry]
	compressionInterval time.Duration
	bgCtx               context.Context
	cancelCtx           context.CancelFunc
	batchTimeout        time.Duration
	batchMaxSize        int
	workerCount         uint32
	workQueues          []chan *transactionOperation
	workersDone         []chan struct{}
}

type transactionWriterBatch struct {
	id             string
	opened         time.Time
	ops            []*transactionOperation
	timeoutContext context.Context
	timeoutCancel  func()

	txInsertsByFrom     map[string][]*transactionOperation
	txUpdates           []*transactionOperation
	txDeletes           []string
	receiptInserts      map[string]*apitypes.ReceiptRecord
	historyInserts      []*apitypes.TXHistoryRecord
	compressionChecks   map[string]bool
	confirmationInserts []*apitypes.ConfirmationRecord
	confirmationResets  map[string]bool
}

func newTransactionWriter(bgCtx context.Context, p *sqlPersistence, conf config.Section) (tw *transactionWriter, err error) {
	workerCount := conf.GetInt(ConfigTXWriterCount)
	batchMaxSize := conf.GetInt(ConfigTXWriterBatchSize)
	cacheSlots := conf.GetInt(ConfigTXWriterCacheSlots)
	tw = &transactionWriter{
		p: p,
		//nolint:gosec // Safe conversion as workerCount is always positive
		workerCount:         uint32(workerCount),
		batchTimeout:        conf.GetDuration(ConfigTXWriterBatchTimeout),
		batchMaxSize:        batchMaxSize,
		workersDone:         make([]chan struct{}, workerCount),
		workQueues:          make([]chan *transactionOperation, workerCount),
		compressionInterval: conf.GetDuration(ConfigTXWriterHistoryCompactionInterval),
	}
	tw.txMetaCache, err = lru.New[string, *txCacheEntry](cacheSlots)
	if err == nil {
		tw.nextNonceCache, err = lru.New[string, *nonceCacheEntry](cacheSlots)
	}
	if err != nil {
		return nil, err
	}
	tw.bgCtx, tw.cancelCtx = context.WithCancel(bgCtx)
	for i := 0; i < workerCount; i++ {
		tw.workersDone[i] = make(chan struct{})
		tw.workQueues[i] = make(chan *transactionOperation, batchMaxSize)
		go tw.worker(i)
	}
	return tw, nil
}

func newTransactionOperation(txID string) *transactionOperation {
	return &transactionOperation{
		opID: fftypes.ShortID(),
		txID: txID,
		done: make(chan error, 1), // 1 slot to ensure we don't block the writer
	}
}

func (op *transactionOperation) flush(ctx context.Context) error {
	select {
	case err := <-op.done:
		log.L(ctx).Debugf("Flushed write operation %s (err=%v)", op.opID, err)
		return err
	case <-ctx.Done():
		return i18n.NewError(ctx, i18n.MsgContextCanceled)
	}
}

func (tw *transactionWriter) queue(ctx context.Context, op *transactionOperation) {
	// All insert/nonce-allocation requests for the same signer go to the same work - allowing nonce
	// allocation to function deterministically, while still allowing batch insertion of many
	// transaction object within a single DB transaction.
	//
	// After insert update operations for a given txID address, deterministically go to the same worker.
	// This ensures that all sequenced items (like history records) are written in the right order.
	//
	// NOTE: This requires that all transaction inserts operations wait for completion before doing updates.
	//
	// Note the insertion order of the transactions across different signing keys does not matter, as the
	// caller waits for "done" on these inserts before returning to the caller (FireFly Core).
	// So if multiple transactions are queued for insert concurrently with different IDs,
	// then there is no deterministic ordering guarantee possible regardless.

	var hashKey string
	if op.txInsert != nil {
		hashKey = op.txInsert.From
	} else {
		hashKey = op.txID
	}
	if hashKey == "" {
		op.done <- i18n.NewError(ctx, tmmsgs.MsgTransactionOpInvalid)
		return
	}

	h := fnv.New32a() // simple non-cryptographic hash algo
	_, _ = h.Write([]byte(hashKey))
	routine := h.Sum32() % tw.workerCount
	log.L(ctx).Debugf("Queuing write operation %s to worker tx_writer_%.4d", op.opID, routine)
	select {
	case tw.workQueues[routine] <- op: // it's queued
	case <-ctx.Done(): // timeout of caller context
		// Just return, as they are giving up on the request so there's no need to queue it
		// If they flush they will get an error
	case <-tw.bgCtx.Done(): // shutdown
		// Push an error back to the operator before we return (note we allocate a slot to make this safe)
		op.done <- i18n.NewError(ctx, tmmsgs.MsgShuttingDown)
	}
}

func (tw *transactionWriter) worker(i int) {
	defer close(tw.workersDone[i])
	workerID := fmt.Sprintf("tx_writer_%.4d", i)
	ctx := log.WithLogFields(tw.bgCtx, "job", workerID)
	l := log.L(ctx)
	var batch *transactionWriterBatch
	batchCount := 0
	workQueue := tw.workQueues[i]
	var shutdownRequest *transactionOperation
	for shutdownRequest == nil {
		var timeoutContext context.Context
		var timedOut bool
		if batch != nil {
			timeoutContext = batch.timeoutContext
		} else {
			timeoutContext = ctx
		}
		select {
		case op := <-workQueue:
			if op.isShutdown {
				// flush out the queue
				shutdownRequest = op
				timedOut = true
				break
			}
			if batch == nil {
				batch = &transactionWriterBatch{
					id:     fmt.Sprintf("%.4d_%.9d", i, batchCount),
					opened: time.Now(),
				}
				batch.timeoutContext, batch.timeoutCancel = context.WithTimeout(ctx, tw.batchTimeout)
				batchCount++
			}
			batch.ops = append(batch.ops, op)
			l.Debugf("Added write operation %s to batch %s (len=%d)", op.opID, batch.id, len(batch.ops))
		case <-timeoutContext.Done():
			timedOut = true
			select {
			case <-ctx.Done():
				l.Debugf("Transaction writer ending")
				return
			default:
			}
		}

		if batch != nil && (timedOut || (len(batch.ops) >= tw.batchMaxSize)) {
			batch.timeoutCancel()
			l.Debugf("Running batch %s (len=%d,timeout=%t,age=%dms)", batch.id, len(batch.ops), timedOut, time.Since(batch.opened).Milliseconds())
			tw.runBatch(ctx, batch)
			batch = nil
		}

		if shutdownRequest != nil {
			close(shutdownRequest.done)
		}
	}
}

func (tw *transactionWriter) runBatch(ctx context.Context, b *transactionWriterBatch) {
	err := tw.p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		// Build all the batch insert operations
		b.txInsertsByFrom = make(map[string][]*transactionOperation)
		b.confirmationResets = make(map[string]bool)
		b.receiptInserts = make(map[string]*apitypes.ReceiptRecord)
		b.compressionChecks = make(map[string]bool)
		for _, op := range b.ops {
			switch {
			case op.txInsert != nil:
				b.txInsertsByFrom[op.txInsert.From] = append(b.txInsertsByFrom[op.txInsert.From], op)
			case op.txUpdate != nil:
				b.txUpdates = append(b.txUpdates, op)
			case op.txDelete != nil:
				b.txDeletes = append(b.txDeletes, *op.txDelete)
				delete(b.compressionChecks, op.txID)
			case op.receipt != nil:
				// Last one wins in the receipts (can't insert the same TXID twice in one InsertMany)
				b.receiptInserts[op.txID] = op.receipt
			case op.historyRecord != nil:
				b.historyInserts = append(b.historyInserts, op.historyRecord)
				b.compressionChecks[op.txID] = true
			case op.confirmation != nil:
				if op.clearConfirmations {
					// We need to purge any previous confirmation inserts for the same TX,
					// as we will only do one clear operation for this batch (before the insert-many).
					newConfirmationInserts := make([]*apitypes.ConfirmationRecord, 0, len(b.confirmationInserts))
					for _, c := range b.confirmationInserts {
						if c.TransactionID != op.confirmation.TransactionID {
							newConfirmationInserts = append(newConfirmationInserts, c)
						}
					}
					b.confirmationInserts = newConfirmationInserts
					// Add the reset
					b.confirmationResets[op.confirmation.TransactionID] = true
				}
				b.confirmationInserts = append(b.confirmationInserts, op.confirmation)
			}
		}
		return tw.executeBatchOps(ctx, b)
	})
	if err != nil {
		log.L(ctx).Errorf("Transaction persistence batch failed: %s", err)

		// Clear any cached nonces
		tw.clearCachedNonces(ctx, b.txInsertsByFrom)

		// All ops in the batch get a single generic error
		err = i18n.NewError(ctx, tmmsgs.MsgTransactionPersistenceError)
	}
	for _, op := range b.ops {
		if !op.sentConflict {
			op.done <- err
		}
	}

}

func (tw *transactionWriter) assignNonces(ctx context.Context, txInsertsByFrom map[string][]*transactionOperation) error {
	for signer, txs := range txInsertsByFrom {
		cacheEntry, isCached := tw.nextNonceCache.Get(signer)
		cacheExpired := false
		if isCached {
			timeSinceCached := time.Since(*cacheEntry.cachedTime.Time())
			if timeSinceCached > tw.p.nonceStateTimeout {
				log.L(ctx).Infof("Nonce cache expired for signer '%s' after %s", signer, timeSinceCached.String())
				cacheExpired = true
			}
		}
		for _, op := range txs {
			if op.noncePreAssigned {
				continue
			}
			if op.sentConflict {
				// This has been excluded in preInsertIdempotencyCheck, we must not allocate a nonce
				log.L(ctx).Debugf("Skipped nonce assignment to duplicate TX %s", op.txInsert.ID)
				continue
			}
			if cacheEntry == nil || cacheExpired {
				nextNonce, err := op.nextNonceCB(ctx, signer)
				if err != nil {
					return err
				}
				var internalNextNonce uint64
				// keep a record of the internal record of existing nonce
				if cacheEntry != nil {
					// always use the expired cache record first
					// there could be multiple transactions pending to be inserted into the DB as a batch
					// so the nonce value in DB record might be lower than the cached value
					internalNextNonce = cacheEntry.nextNonce
					log.L(ctx).Tracef("Using the cached existing nonce %s / %d to compare with the queried next %d for transaction %s", signer, internalNextNonce, nextNonce, op.txInsert.ID)
				} else {
					// when there is no cached nonce we need to fetch the highest nonce in our DB
					filter := persistence.TransactionFilters.NewFilterLimit(ctx, 1).Eq("from", signer).Sort("-nonce")
					existingTXs, _, err := tw.p.transactions.GetMany(ctx, filter)
					if err != nil {
						log.L(ctx).Errorf("Failed to query highest persisted nonce for '%s': %s", signer, err)
						return err
					}
					if len(existingTXs) > 0 {
						internalNextNonce = existingTXs[0].Nonce.Uint64() + 1
						log.L(ctx).Tracef("Using the next nonce calculated from DB %s / %d to compare with the queried next %d for transaction %s", signer, internalNextNonce, nextNonce, op.txInsert.ID)
					}
				}
				if internalNextNonce > nextNonce {
					log.L(ctx).Infof("Using next nonce %s / %d instead of queried next %d for transaction %s", signer, internalNextNonce, nextNonce, op.txInsert.ID)
					nextNonce = internalNextNonce
				}
				// Now we can cache the newly calculated value, and just increment as we go through all the TX in this batch
				cacheEntry = &nonceCacheEntry{
					cachedTime: fftypes.Now(),
					nextNonce:  nextNonce,
				}
			}
			log.L(ctx).Infof("Assigned nonce %s / %d to %s", signer, cacheEntry.nextNonce, op.txInsert.ID)
			//nolint:gosec
			op.txInsert.Nonce = fftypes.NewFFBigInt(int64(cacheEntry.nextNonce))
			cacheEntry.nextNonce++
			tw.nextNonceCache.Add(signer, cacheEntry)
		}
	}
	return nil
}

func (tw *transactionWriter) clearCachedNonces(ctx context.Context, txInsertsByFrom map[string][]*transactionOperation) {
	for signer := range txInsertsByFrom {
		log.L(ctx).Warnf("Clearing cache for '%s' after insert failure", signer)
		_ = tw.nextNonceCache.Remove(signer)
	}
}

func (tw *transactionWriter) preInsertIdempotencyCheck(ctx context.Context, b *transactionWriterBatch) (validInserts []*apitypes.ManagedTX, err error) {
	// We want to return 409s (not 500s) for idempotency checks, and only fail the individual TX.
	// There should have been a pre-check when the transaction came in on the API, so we're in
	// a small window here where we had multiple API calls running concurrently.
	// So we choose to optimize the check using the txMetaCache we add new inserts to - meaning
	// a very edge case of a 500 in cache expiry, if we somehow expired it from that cache in this
	// small window.
	for _, txOps := range b.txInsertsByFrom {
		for _, txOp := range txOps {
			var existing *apitypes.ManagedTX
			_, inCache := tw.txMetaCache.Get(txOp.txID)
			if inCache {
				existing, err = tw.p.GetTransactionByID(ctx, txOp.txID)
				if err != nil {
					log.L(ctx).Errorf("Pre-insert idempotency check failed for transaction %s: %s", txOp.txID, err)
					return nil, err
				}
			}
			if existing != nil {
				// Send a conflict, and do not add it to the list
				txOp.sentConflict = true
				txOp.done <- i18n.NewError(ctx, tmmsgs.MsgDuplicateID, txOp.txID)
			} else {
				log.L(ctx).Debugf("Adding TX %s from write operation %s to insert idx=%d", txOp.txID, txOp.opID, len(validInserts))
				validInserts = append(validInserts, txOp.txInsert)
			}
		}
	}

	return validInserts, nil
}

func (tw *transactionWriter) mergeTxUpdates(ctx context.Context, b *transactionWriterBatch) (txCompletions []*apitypes.TXCompletion, mergedUpdates map[string]*apitypes.TXUpdates) {
	// We build a list of transaction completions to write, in a lock at the very end of the transaction
	txCompletions = make([]*apitypes.TXCompletion, 0, len(b.txUpdates))
	// Do all the transaction updates
	mergedUpdates = make(map[string]*apitypes.TXUpdates)
	for _, op := range b.txUpdates {
		update, merge := mergedUpdates[op.txID]
		if merge {
			update.Merge(op.txUpdate)
		} else {
			mergedUpdates[op.txID] = op.txUpdate
		}
		if op.txUpdate.Status != nil && (*op.txUpdate.Status == apitypes.TxStatusSucceeded || *op.txUpdate.Status == apitypes.TxStatusFailed) {
			txCompletions = append(txCompletions, &apitypes.TXCompletion{
				ID:     op.txID,
				Time:   fftypes.Now(),
				Status: *op.txUpdate.Status,
			})
		}
		log.L(ctx).Debugf("Updating transaction %s in write operation %s (merged=%t)", op.txID, op.opID, merge)
	}
	return
}

func (tw *transactionWriter) executeBatchOps(ctx context.Context, b *transactionWriterBatch) error {
	txInserts, err := tw.preInsertIdempotencyCheck(ctx, b)
	if err != nil {
		return err
	}

	// Insert all the transactions
	if len(txInserts) > 0 {
		if err := tw.assignNonces(ctx, b.txInsertsByFrom); err != nil {
			log.L(ctx).Errorf("InsertMany transactions (%d) nonce assignment failed: %s", len(b.historyInserts), err)
			return err
		}
		if err := tw.p.transactions.InsertMany(ctx, txInserts, false); err != nil {
			log.L(ctx).Errorf("InsertMany transactions (%d) failed: %s", len(b.historyInserts), err)
			return err
		}
		// Add to our metadata cache, in the fresh new state
		for _, t := range txInserts {
			_ = tw.txMetaCache.Add(t.ID, &txCacheEntry{lastCompacted: fftypes.Now()})
		}
	}
	// We build a list of transaction completions to write, in a lock at the very end of the transaction
	txCompletions, mergedUpdates := tw.mergeTxUpdates(ctx, b)
	for txID, update := range mergedUpdates {
		if err := tw.p.updateTransaction(ctx, txID, update); err != nil {
			log.L(ctx).Errorf("Update transaction %s failed: %s", txID, err)
			return err
		}
	}
	// Then the receipts - which need to be an upsert
	receipts := make([]*apitypes.ReceiptRecord, 0, len(b.receiptInserts))
	for _, r := range b.receiptInserts {
		receipts = append(receipts, r)
	}
	if len(receipts) > 0 {
		// Try optimized insert first, allowing partial success so we can fall back
		err := tw.p.receipts.InsertMany(ctx, receipts, true /* fallback */)
		if err != nil {
			log.L(ctx).Debugf("Batch receipt insert optimization failed: %s", err)
			for _, receipt := range b.receiptInserts {
				// FAll back to individual upserts
				if _, err := tw.p.receipts.Upsert(ctx, receipt, dbsql.UpsertOptimizationExisting); err != nil {
					log.L(ctx).Errorf("Upsert receipt %s failed: %s", receipt.TransactionID, err)
					return err
				}
			}
		}
	}
	// Then do any confirmation clears
	for txID := range b.confirmationResets {
		if err := tw.p.confirmations.DeleteMany(ctx, persistence.ConfirmationFilters.NewFilter(ctx).Eq("transaction", txID)); err != nil {
			log.L(ctx).Errorf("DeleteMany confirmation records for transaction %s failed: %s", txID, err)
			return err
		}
	}
	// Then insert the new confirmation records
	if len(b.confirmationInserts) > 0 {
		if err := tw.p.confirmations.InsertMany(ctx, b.confirmationInserts, false); err != nil {
			log.L(ctx).Errorf("InsertMany confirmation records (%d) failed: %s", len(b.confirmationInserts), err)
			return err
		}
	}
	// Then the history entries
	if len(b.historyInserts) > 0 {
		if err := tw.p.txHistory.InsertMany(ctx, b.historyInserts, false); err != nil {
			log.L(ctx).Errorf("InsertMany history records (%d) failed: %s", len(b.historyInserts), err)
			return err
		}
	}
	// Do the compression checks
	if tw.compressionInterval > 0 {
		for txID := range b.compressionChecks {
			if err := tw.compressionCheck(ctx, txID); err != nil {
				log.L(ctx).Errorf("Compression check for %s failed: %s", txID, err)
				return err
			}
		}
	}
	// Lock the completions table and then insert all the completions (with conflict safety)
	// We do this last as we want to hold the sequencing lock on this table for the shorted amount of time.
	if err := tw.p.writeTransactionCompletions(ctx, txCompletions); err != nil {
		log.L(ctx).Errorf("Inserting %d transaction completions failed: %s", len(txCompletions), err)
		return err
	}
	// Do all the transaction deletes
	for _, txID := range b.txDeletes {
		// Delete any receipt
		if err := tw.p.receipts.Delete(ctx, txID); err != nil && err != fftypes.DeleteRecordNotFound {
			log.L(ctx).Errorf("Delete receipt for transaction %s failed: %s", txID, err)
			return err
		}
		// Clear confirmations
		if err := tw.p.confirmations.DeleteMany(ctx, persistence.ConfirmationFilters.NewFilter(ctx).Eq("transaction", txID)); err != nil {
			log.L(ctx).Errorf("DeleteMany confirmation records for transaction %s failed: %s", txID, err)
			return err
		}
		// Clear history
		if err := tw.p.txHistory.DeleteMany(ctx, persistence.TXHistoryFilters.NewFilter(ctx).Eq("transaction", txID)); err != nil {
			log.L(ctx).Errorf("DeleteMany history records for transaction %s failed: %s", txID, err)
			return err
		}
		// Delete the transaction
		if err := tw.p.transactions.Delete(ctx, txID); err != nil {
			log.L(ctx).Errorf("Delete transaction %s failed: %s", txID, err)
			return err
		}
	}
	return nil
}

func (tw *transactionWriter) compressionCheck(ctx context.Context, txID string) error {
	txMeta, ok := tw.txMetaCache.Get(txID)
	if ok {
		sinceCompaction := time.Since(*txMeta.lastCompacted.Time())
		if sinceCompaction < tw.compressionInterval {
			// Nothing to do
			return nil
		}
		log.L(ctx).Debugf("Compressing history for TX '%s' after %s", txID, sinceCompaction.String())
	} else {
		txMeta = &txCacheEntry{}
		log.L(ctx).Debugf("Compressing history for TX '%s' after cache miss", txID)
	}
	if err := tw.p.compressHistory(ctx, txID); err != nil {
		return err
	}
	txMeta.lastCompacted = fftypes.Now()
	_ = tw.txMetaCache.Add(txID, txMeta)
	return nil
}

func (tw *transactionWriter) stop() {
	for i, workerDone := range tw.workersDone {
		select {
		case <-workerDone:
		case <-tw.bgCtx.Done():
		default:
			// Quiesce the worker
			shutdownOp := &transactionOperation{
				isShutdown: true,
				done:       make(chan error),
			}
			tw.workQueues[i] <- shutdownOp
			<-shutdownOp.done
		}
		<-workerDone
	}
	tw.cancelCtx()
}
