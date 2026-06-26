// Copyright © 2025 - 2026 Kaleido, Inc.
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

package leveldb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type leveldbPersistence struct {
	db                *leveldb.DB
	syncWrites        bool
	maxHistoryCount   int
	nonceMux          sync.Mutex
	lockedNonces      map[string]*lockedNonce
	nonceStateTimeout time.Duration
	txMux             sync.RWMutex // allows us to draw conclusions on the cleanup of indexes
}

func NewLevelDBPersistence(ctx context.Context, nonceStateTimeout time.Duration) (persistence.Persistence, error) {
	dbPath := config.GetString(tmconfig.PersistenceLevelDBPath)
	if dbPath == "" {
		return nil, i18n.NewError(ctx, tmmsgs.MsgLevelDBPathMissing)
	}
	db, err := leveldb.OpenFile(dbPath, &opt.Options{
		OpenFilesCacheCapacity: config.GetInt(tmconfig.PersistenceLevelDBMaxHandles),
	})
	if err != nil {
		return nil, i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceInitFailed, dbPath)
	}
	return &leveldbPersistence{
		db:                db,
		syncWrites:        config.GetBool(tmconfig.PersistenceLevelDBSyncWrites),
		maxHistoryCount:   config.GetInt(tmconfig.TransactionsMaxHistoryCount),
		nonceStateTimeout: nonceStateTimeout,
		lockedNonces:      map[string]*lockedNonce{},
	}, nil
}

const checkpointsPrefix = "checkpoints_0/"
const eventstreamsPrefix = "eventstreams_0/"
const eventstreamsEnd = "eventstreams_1"
const listenersPrefix = "listeners_0/"
const listenersEnd = "listeners_1"
const transactionsPrefix = "tx_0/"
const nonceAllocationPrefix = "nonce_0/"
const txPendingIndexPrefix = "tx_inflight_0/"
const txPendingIndexEnd = "tx_inflight_1"
const txCreatedIndexPrefix = "tx_created_0/"
const txCreatedIndexEnd = "tx_created_1"

func signerNoncePrefix(signer string) string {
	return fmt.Sprintf("%s%s_0/", nonceAllocationPrefix, signer)
}

func signerNonceEnd(signer string) string {
	return fmt.Sprintf("%s%s_1", nonceAllocationPrefix, signer)
}

func txNonceAllocationKey(signer string, nonce *fftypes.FFBigInt) []byte {
	return []byte(fmt.Sprintf("%s%s_0/%.24d", nonceAllocationPrefix, signer, nonce.Int()))
}

func txPendingIndexKey(sequenceID string) []byte {
	return []byte(fmt.Sprintf("%s%s", txPendingIndexPrefix, sequenceID))
}

func txCreatedIndexKey(tx *apitypes.ManagedTX) []byte {
	return []byte(fmt.Sprintf("%s%.19d/%s", txCreatedIndexPrefix, tx.Created.UnixNano(), tx.SequenceID))
}

func txDataKey(k string) []byte {
	return []byte(fmt.Sprintf("%s%s", transactionsPrefix, k))
}

func prefixedKey(prefix string, id fmt.Stringer) []byte {
	return []byte(fmt.Sprintf("%s%s", prefix, id))
}

func (p *leveldbPersistence) writeKeyValue(ctx context.Context, key, value []byte) error {
	err := p.db.Put(key, value, &opt.WriteOptions{Sync: p.syncWrites})
	if err != nil {
		return i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceWriteFailed)
	}
	return nil
}

func (p *leveldbPersistence) writeJSON(ctx context.Context, key []byte, value interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceMarshalFailed)
	}
	log.L(ctx).Debugf("Wrote %s", key)
	return p.writeKeyValue(ctx, key, b)
}

func (p *leveldbPersistence) getKeyValue(ctx context.Context, key []byte) ([]byte, error) {
	b, err := p.db.Get(key, &opt.ReadOptions{})
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceReadFailed, key)
	}
	return b, err
}

func (p *leveldbPersistence) readJSONByIndex(ctx context.Context, idxKey []byte, target interface{}) error {
	valKey, err := p.getKeyValue(ctx, idxKey)
	if err != nil || valKey == nil {
		return err
	}
	return p.readJSON(ctx, valKey, target)
}

func (p *leveldbPersistence) readJSON(ctx context.Context, key []byte, target interface{}) error {
	b, err := p.getKeyValue(ctx, key)
	if err != nil || b == nil {
		return err
	}
	err = json.Unmarshal(b, target)
	if err != nil {
		return i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceUnmarshalFailed)
	}
	log.L(ctx).Debugf("Read %s", key)
	return nil
}

func (p *leveldbPersistence) listJSON(ctx context.Context, collectionPrefix, collectionEnd, after string, limit int,
	dir txhandler.SortDirection,
	val func() interface{}, // return a pointer to a pointer variable, of the type to unmarshal
	add func(interface{}), // passes back the val() for adding to the list, if the filters match
	indexResolver func(ctx context.Context, k []byte) ([]byte, error), // if non-nil then the initial lookup will be passed to this, to lookup the target bytes. Nil skips item
	filters ...func(interface{}) bool, // filters to apply to the val() after unmarshalling
) ([][]byte, error) {
	collectionRange := &util.Range{
		Start: []byte(collectionPrefix),
		Limit: []byte(collectionEnd),
	}
	var it iterator.Iterator
	switch dir {
	case txhandler.SortDirectionAscending:
		afterKey := collectionPrefix + after
		if after != "" {
			collectionRange.Start = []byte(afterKey)
		}
		it = p.db.NewIterator(collectionRange, &opt.ReadOptions{DontFillCache: true})
		if after != "" && it.Next() {
			if !strings.HasPrefix(string(it.Key()), afterKey) {
				it.Prev() // skip back, as the first key was already after the "after" key
			}
		}
	default:
		if after != "" {
			collectionRange.Limit = []byte(collectionPrefix + after) // exclusive for limit, so no need to fiddle here
		}
		it = p.db.NewIterator(collectionRange, &opt.ReadOptions{DontFillCache: true})
	}
	defer it.Release()
	return p.iterateJSON(ctx, it, limit, dir, val, add, indexResolver, filters...)
}

func (p *leveldbPersistence) iterateJSON(ctx context.Context, it iterator.Iterator, limit int,
	dir txhandler.SortDirection, val func() interface{}, add func(interface{}), indexResolver func(ctx context.Context, k []byte) ([]byte, error), filters ...func(interface{}) bool,
) (orphanedIdxKeys [][]byte, err error) {
	count := 0
	next := it.Next // forwards we enter this function before the first key
	if dir == txhandler.SortDirectionDescending {
		next = it.Last // reverse we enter this function
	}
itLoop:
	for next() {
		if dir == txhandler.SortDirectionDescending {
			next = it.Prev
		} else {
			next = it.Next
		}
		v := val()
		b := it.Value()
		if indexResolver != nil {
			valKey := b
			b, err = indexResolver(ctx, valKey)
			if err != nil {
				return nil, err
			}
			if b == nil {
				log.L(ctx).Warnf("Skipping orphaned index key '%s' pointing to '%s'", it.Key(), valKey)
				orphanedIdxKeys = append(orphanedIdxKeys, it.Key())
				continue itLoop
			}
		}
		err := json.Unmarshal(b, v)
		if err != nil {
			return nil, i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceUnmarshalFailed)
		}
		for _, f := range filters {
			if !f(v) {
				continue itLoop
			}
		}
		add(v)
		count++
		if limit > 0 && count >= limit {
			break
		}
	}
	log.L(ctx).Debugf("Listed %d items", count)
	return orphanedIdxKeys, it.Error()
}

func (p *leveldbPersistence) deleteKeys(ctx context.Context, keys ...[]byte) error {
	for _, key := range keys {
		err := p.db.Delete(key, &opt.WriteOptions{Sync: p.syncWrites})
		if err != nil && err != leveldb.ErrNotFound {
			return i18n.WrapError(ctx, err, tmmsgs.MsgPersistenceDeleteFailed)
		}
		log.L(ctx).Debugf("Deleted %s", key)
	}
	return nil
}

func (p *leveldbPersistence) RichQuery() persistence.RichQuery {
	panic("not supported")
}

func (p *leveldbPersistence) TransactionCompletions() persistence.TransactionCompletions {
	panic("not supported")
}

func (p *leveldbPersistence) WriteCheckpoint(ctx context.Context, checkpoint *apitypes.EventStreamCheckpoint) error {
	return p.writeJSON(ctx, prefixedKey(checkpointsPrefix, checkpoint.StreamID), checkpoint)
}

func (p *leveldbPersistence) GetCheckpoint(ctx context.Context, streamID *fftypes.UUID) (cp *apitypes.EventStreamCheckpoint, err error) {
	err = p.readJSON(ctx, prefixedKey(checkpointsPrefix, streamID), &cp)
	return cp, err
}

func (p *leveldbPersistence) DeleteCheckpoint(ctx context.Context, streamID *fftypes.UUID) error {
	return p.deleteKeys(ctx, prefixedKey(checkpointsPrefix, streamID))
}

func (p *leveldbPersistence) ListStreamsByCreateTime(ctx context.Context, after *fftypes.UUID, limit int, dir txhandler.SortDirection) ([]*apitypes.EventStream, error) {
	streams := make([]*apitypes.EventStream, 0)
	if _, err := p.listJSON(ctx, eventstreamsPrefix, eventstreamsEnd, after.String(), limit, dir,
		func() interface{} { var v *apitypes.EventStream; return &v },
		func(v interface{}) { streams = append(streams, *(v.(**apitypes.EventStream))) },
		nil,
	); err != nil {
		return nil, err
	}
	return streams, nil
}

func (p *leveldbPersistence) GetStream(ctx context.Context, streamID *fftypes.UUID) (es *apitypes.EventStream, err error) {
	err = p.readJSON(ctx, prefixedKey(eventstreamsPrefix, streamID), &es)
	return es, err
}

func (p *leveldbPersistence) WriteStream(ctx context.Context, spec *apitypes.EventStream) error {
	return p.writeJSON(ctx, prefixedKey(eventstreamsPrefix, spec.ID), spec)
}

func (p *leveldbPersistence) DeleteStream(ctx context.Context, streamID *fftypes.UUID) error {
	return p.deleteKeys(ctx, prefixedKey(eventstreamsPrefix, streamID))
}

func (p *leveldbPersistence) ListListenersByCreateTime(ctx context.Context, after *fftypes.UUID, limit int, dir txhandler.SortDirection) ([]*apitypes.Listener, error) {
	listeners := make([]*apitypes.Listener, 0)
	if _, err := p.listJSON(ctx, listenersPrefix, listenersEnd, after.String(), limit, dir,
		func() interface{} { var v *apitypes.Listener; return &v },
		func(v interface{}) { listeners = append(listeners, *(v.(**apitypes.Listener))) },
		nil,
	); err != nil {
		return nil, err
	}
	return listeners, nil
}

func (p *leveldbPersistence) ListStreamListenersByCreateTime(ctx context.Context, after *fftypes.UUID, limit int, dir txhandler.SortDirection, streamID *fftypes.UUID) ([]*apitypes.Listener, error) {
	listeners := make([]*apitypes.Listener, 0)
	if _, err := p.listJSON(ctx, listenersPrefix, listenersEnd, after.String(), limit, dir,
		func() interface{} { var v *apitypes.Listener; return &v },
		func(v interface{}) { listeners = append(listeners, *(v.(**apitypes.Listener))) },
		nil,
		func(v interface{}) bool { return (*(v.(**apitypes.Listener))).StreamID.Equals(streamID) },
	); err != nil {
		return nil, err
	}
	return listeners, nil
}

func (p *leveldbPersistence) GetListener(ctx context.Context, listenerID *fftypes.UUID) (l *apitypes.Listener, err error) {
	err = p.readJSON(ctx, prefixedKey(listenersPrefix, listenerID), &l)
	return l, err
}

func (p *leveldbPersistence) WriteListener(ctx context.Context, spec *apitypes.Listener) error {
	return p.writeJSON(ctx, prefixedKey(listenersPrefix, spec.ID), spec)
}

func (p *leveldbPersistence) DeleteListener(ctx context.Context, listenerID *fftypes.UUID) error {
	return p.deleteKeys(ctx, prefixedKey(listenersPrefix, listenerID))
}

func (p *leveldbPersistence) indexLookupCallback(ctx context.Context, key []byte) ([]byte, error) {
	b, err := p.getKeyValue(ctx, key)
	switch {
	case err != nil:
		return nil, err
	case b == nil:
		return nil, nil
	}
	return b, err
}

func (p *leveldbPersistence) cleanupOrphanedTXIdxKeys(ctx context.Context, orphanedIdxKeys [][]byte) {
	p.txMux.Lock()
	defer p.txMux.Unlock()
	err := p.deleteKeys(ctx, orphanedIdxKeys...)
	if err != nil {
		log.L(ctx).Warnf("Failed to clean up orphaned index keys: %s", err)
	}
}

// listTransactionsByIndexLocked performs the actual list operation without acquiring locks.
// The caller must already hold a lock (read lock is sufficient)
func (p *leveldbPersistence) listTransactionsByIndexLocked(ctx context.Context, collectionPrefix, collectionEnd, afterStr string, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, [][]byte, error) {
	transactions := make([]*apitypes.ManagedTX, 0)
	orphanedIdxKeys, err := p.listJSON(ctx, collectionPrefix, collectionEnd, afterStr, limit, dir,
		func() interface{} { var v *apitypes.ManagedTX; return &v },
		func(v interface{}) {
			tx := *(v.(**apitypes.ManagedTX))
			migrateTX(tx)
			transactions = append(transactions, tx)
		},
		p.indexLookupCallback,
	)
	if err != nil {
		return nil, nil, err
	}
	return transactions, orphanedIdxKeys, nil
}

func (p *leveldbPersistence) listTransactionsByIndex(ctx context.Context, collectionPrefix, collectionEnd, afterStr string, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {

	p.txMux.RLock()
	transactions, orphanedIdxKeys, err := p.listTransactionsByIndexLocked(ctx, collectionPrefix, collectionEnd, afterStr, limit, dir)
	p.txMux.RUnlock()
	if err != nil {
		return nil, err
	}
	// If we find orphaned index keys we clean them up - which requires the write lock (hence dropping read-lock first)
	if len(orphanedIdxKeys) > 0 {
		p.cleanupOrphanedTXIdxKeys(ctx, orphanedIdxKeys)
	}
	return transactions, nil
}

func (p *leveldbPersistence) ListTransactionsByCreateTime(ctx context.Context, after *apitypes.ManagedTX, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {
	afterStr := ""
	if after != nil {
		afterStr = fmt.Sprintf("%.19d/%s", after.Created.UnixNano(), after.SequenceID)
	}
	return p.listTransactionsByIndex(ctx, txCreatedIndexPrefix, txCreatedIndexEnd, afterStr, limit, dir)
}

func (p *leveldbPersistence) ListTransactionsByNonce(ctx context.Context, signer string, after *fftypes.FFBigInt, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {
	afterStr := ""
	if after != nil {
		afterStr = fmt.Sprintf("%.24d", after.Int())
	}
	return p.listTransactionsByIndex(ctx, signerNoncePrefix(signer), signerNonceEnd(signer), afterStr, limit, dir)
}

func (p *leveldbPersistence) ListTransactionsPending(ctx context.Context, afterSequenceID string, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {
	return p.listTransactionsByIndex(ctx, txPendingIndexPrefix, txPendingIndexEnd, afterSequenceID, limit, dir)
}

func (p *leveldbPersistence) GetTransactionByID(ctx context.Context, txID string) (tx *apitypes.ManagedTX, err error) {
	txh, err := p.GetTransactionByIDWithStatus(ctx, txID, false)
	if err != nil || txh == nil {
		return nil, err
	}
	return txh.ManagedTX, err
}

func (p *leveldbPersistence) GetTransactionReceipt(ctx context.Context, txID string) (receipt *ffcapi.TransactionReceiptResponse, err error) {
	txh, err := p.GetTransactionByIDWithStatus(ctx, txID, false)
	if err != nil || txh == nil {
		return nil, err
	}
	return txh.Receipt, err
}

func (p *leveldbPersistence) GetTransactionConfirmations(ctx context.Context, txID string) (confirmations []*apitypes.Confirmation, err error) {
	txh, err := p.GetTransactionByIDWithStatus(ctx, txID, false)
	if err != nil || txh == nil {
		return nil, err
	}
	return txh.Confirmations, err
}

func migrateTX(tx *apitypes.ManagedTX) {
	// For historical reasons we had some fields stored twice in V1.2
	// We now consistently tread the top level objects as the source of truth, but for any
	// read of an old object (LevelDB problem only) we need to do a migration to the new place.
	if tx.DeprecatedTransactionHeaders != nil && tx.DeprecatedTransactionHeaders.From != "" && tx.From == "" {
		tx.From = tx.DeprecatedTransactionHeaders.From
	}
	if tx.DeprecatedTransactionHeaders != nil && tx.DeprecatedTransactionHeaders.To != "" && tx.To == "" {
		tx.To = tx.DeprecatedTransactionHeaders.To
	}
	if tx.DeprecatedTransactionHeaders != nil && tx.DeprecatedTransactionHeaders.Nonce != nil && tx.Nonce == nil {
		tx.Nonce = tx.DeprecatedTransactionHeaders.Nonce
	}
	if tx.DeprecatedTransactionHeaders != nil && tx.DeprecatedTransactionHeaders.Gas != nil && tx.Gas == nil {
		tx.Gas = tx.DeprecatedTransactionHeaders.Gas
	}
	if tx.DeprecatedTransactionHeaders != nil && tx.DeprecatedTransactionHeaders.Value != nil && tx.Value == nil {
		tx.Value = tx.DeprecatedTransactionHeaders.Value
	}
	// We then for API queries copy them all for read - but never stored again here.
	tx.DeprecatedTransactionHeaders = &tx.TransactionHeaders
}

func (p *leveldbPersistence) GetTransactionByIDWithStatus(ctx context.Context, txID string, history bool) (tx *apitypes.TXWithStatus, err error) {
	p.txMux.RLock()
	defer p.txMux.RUnlock()
	err = p.readJSON(ctx, txDataKey(txID), &tx)
	if tx != nil {
		migrateTX(tx.ManagedTX)
		if !history {
			tx.History = nil
		}
	}
	return tx, err
}

func (p *leveldbPersistence) GetTransactionByNonce(ctx context.Context, signer string, nonce *fftypes.FFBigInt) (tx *apitypes.ManagedTX, err error) {
	p.txMux.RLock()
	defer p.txMux.RUnlock()
	err = p.readJSONByIndex(ctx, txNonceAllocationKey(signer, nonce), &tx)
	return tx, err
}

func (p *leveldbPersistence) InsertTransactionWithNextNonce(ctx context.Context, tx *apitypes.ManagedTX, nextNonceCB txhandler.NextNonceCallback) (err error) {
	// First job is to assign the next nonce to this request.
	// We block any further sends on this nonce until we've got this one successfully into the node, or
	// fail deterministically in a way that allows us to return it.
	lockedNonce, err := p.assignAndLockNonce(ctx, tx.ID, tx.From, nextNonceCB)
	if err != nil {
		return err
	}
	// We will call markSpent() once we reach the point the nonce has been used
	defer lockedNonce.complete(ctx)

	// Next we update FireFly core with the pre-submitted record pending record, with the allocated nonce.
	// From this point on, we will guide this transaction through to submission.
	// We return an "ack" at this point, and dispatch the work of getting the transaction submitted
	// to the background worker.
	//nolint:gosec // Safe conversion as nonce is always positive
	tx.Nonce = fftypes.NewFFBigInt(int64(lockedNonce.nonce))

	if err = p.writeTransaction(ctx, &apitypes.TXWithStatus{
		ManagedTX: tx,
	}, true); err != nil {
		return err
	}

	// Ok - we've spent it. The rest of the processing will be triggered off of lockedNonce
	// completion adding this transaction to the pool (and/or the change event that comes in from
	// FireFly core from the update to the transaction)
	lockedNonce.spent = true
	return nil

}

// InsertTransactionsWithNextNonce inserts multiple transactions with their next nonce assigned.
func (p *leveldbPersistence) InsertTransactionsWithNextNonce(ctx context.Context, txs []*apitypes.ManagedTX, nextNonceCB txhandler.NextNonceCallback) []error {
	if len(txs) == 0 {
		return nil
	}
	errs := make([]error, len(txs))

	// for leveldb, we process transactions sequentially but group by signer
	// to optimize nonce assignment. All writes happen within the same lock.
	p.txMux.Lock()
	defer p.txMux.Unlock()

	// group by signer for efficient nonce assignment, tracking indices
	type txWithIndex struct {
		tx    *apitypes.ManagedTX
		index int
	}
	txsBySigner := make(map[string][]txWithIndex)
	for i, tx := range txs {
		if tx.From == "" {
			errs[i] = i18n.NewError(ctx, tmmsgs.MsgTransactionOpInvalid)
			continue
		}
		txsBySigner[tx.From] = append(txsBySigner[tx.From], txWithIndex{tx: tx, index: i})
	}

	// assign nonces and write all transactions, tracking errors per transaction
	for signer, signerTxs := range txsBySigner {
		for _, txWithIdx := range signerTxs {
			// use assignAndLockNonceLocked since we already hold the write lock
			lockedNonce, err := p.assignAndLockNonceLocked(ctx, txWithIdx.tx.ID, signer, nextNonceCB)
			if err != nil {
				errs[txWithIdx.index] = err
				continue
			}

			//nolint:gosec // Safe conversion as nonce is always positive
			txWithIdx.tx.Nonce = fftypes.NewFFBigInt(int64(lockedNonce.nonce))

			// use writeTransactionLocked since we already hold the write lock
			if err = p.writeTransactionLocked(ctx, &apitypes.TXWithStatus{
				ManagedTX: txWithIdx.tx,
			}, true); err != nil {
				lockedNonce.complete(ctx)
				errs[txWithIdx.index] = err
				continue
			}

			lockedNonce.spent = true
			lockedNonce.complete(ctx)
			errs[txWithIdx.index] = nil
		}
	}

	return errs
}

func (p *leveldbPersistence) InsertTransactionPreAssignedNonce(ctx context.Context, tx *apitypes.ManagedTX) (err error) {
	return p.writeTransaction(ctx, &apitypes.TXWithStatus{
		ManagedTX: tx,
	}, true)
}

func (p *leveldbPersistence) getPersistedTX(ctx context.Context, txID string) (tx *apitypes.TXWithStatus, err error) {
	tx, err = p.GetTransactionByIDWithStatus(ctx, txID, true)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, i18n.NewError(ctx, tmmsgs.MsgTransactionNotFound, txID)
	}
	return tx, nil
}

func (p *leveldbPersistence) SetTransactionReceipt(ctx context.Context, txID string, receipt *ffcapi.TransactionReceiptResponse) error {
	tx, err := p.getPersistedTX(ctx, txID)
	if err != nil {
		return err
	}
	tx.Receipt = receipt
	return p.writeTransaction(ctx, tx, false)
}

func (p *leveldbPersistence) AddTransactionConfirmations(ctx context.Context, txID string, clearExisting bool, confirmations ...*apitypes.Confirmation) error {
	tx, err := p.getPersistedTX(ctx, txID)
	if err != nil {
		return err
	}
	if clearExisting {
		tx.Confirmations = nil
	}
	tx.Confirmations = append(tx.Confirmations, confirmations...)
	return p.writeTransaction(ctx, tx, false)
}

func (p *leveldbPersistence) UpdateTransaction(ctx context.Context, txID string, updates *apitypes.TXUpdates) (err error) {
	// In LevelDB we bundle the transaction history (up to a certain limit) into the single nested JSON object.
	// So when the base transaction is updated we need to load back in the history from LDB
	// with a cached query before we do the update.
	tx, err := p.getPersistedTX(ctx, txID)
	if err != nil {
		return err
	}
	if updates.Status != nil {
		tx.Status = *updates.Status
	}
	if updates.DeleteRequested != nil {
		tx.DeleteRequested = updates.DeleteRequested
	}
	if updates.From != nil {
		tx.From = *updates.From
	}
	if updates.To != nil {
		tx.To = *updates.To
	}
	if updates.Nonce != nil {
		tx.Nonce = updates.Nonce
	}
	if updates.Gas != nil {
		tx.Gas = updates.Gas
	}
	if updates.Value != nil {
		tx.Value = updates.Value
	}
	if updates.TransactionData != nil {
		tx.TransactionData = *updates.TransactionData
	}
	if updates.TransactionHash != nil {
		tx.TransactionHash = *updates.TransactionHash
	}
	if updates.GasPrice != nil {
		tx.GasPrice = updates.GasPrice
	}
	if updates.PolicyInfo != nil {
		tx.PolicyInfo = updates.PolicyInfo
	}
	if updates.FirstSubmit != nil {
		tx.FirstSubmit = updates.FirstSubmit
	}
	if updates.LastSubmit != nil {
		tx.LastSubmit = updates.LastSubmit
	}
	if updates.ErrorMessage != nil {
		tx.ErrorMessage = *updates.ErrorMessage
	}
	tx.Updated = fftypes.Now()
	return p.writeTransaction(ctx, tx, false)
}

// writeTransactionLocked performs the actual write operation without acquiring locks.
// The caller must already hold the write lock
func (p *leveldbPersistence) writeTransactionLocked(ctx context.Context, tx *apitypes.TXWithStatus, newTx bool) (err error) {
	// We don't double store these values.
	// Would be great to reconcile out this historical oddity, once the only place it's available is on the
	// API, and FireFly core and any other consumers of the API have had time to update to use the base fields
	// consistently.
	tx.DeprecatedTransactionHeaders = nil

	if tx.From == "" ||
		tx.Nonce == nil ||
		tx.Created == nil ||
		tx.ID == "" ||
		tx.Status == "" {
		return i18n.NewError(ctx, tmmsgs.MsgPersistenceTXIncomplete)
	}
	idKey := txDataKey(tx.ID)
	if newTx {
		if tx.SequenceID != "" {
			// for new transactions sequence ID should always be generated by persistence layer
			// as the format of its value is persistence service specific
			log.L(ctx).Errorf("Sequence ID is not allowed for new transaction %s", tx.ID)
			return i18n.NewError(ctx, tmmsgs.MsgPersistenceSequenceIDNotAllowed)
		}
		tx.SequenceID = apitypes.NewULID().String()
		// This must be a unique ID, otherwise we return a conflict.
		// Note we use the final record we write at the end for the conflict check, and also that we're write locked here
		if existing, err := p.getKeyValue(ctx, idKey); err != nil {
			return err
		} else if existing != nil {
			return i18n.NewError(ctx, tmmsgs.MsgDuplicateID, idKey)
		}

		// We write the index records first - because if we crash, we need to be able to know if the
		// index records are valid or not. When reading under the read lock, if there is an index key
		// that does not have a corresponding managed TX available, we will clean up the
		// orphaned index (after swapping the read lock for the write lock)
		// See listTransactionsByIndex() for the other half of this logic.
		err = p.writeKeyValue(ctx, txCreatedIndexKey(tx.ManagedTX), idKey)
		if err == nil && tx.Status == apitypes.TxStatusPending {
			err = p.writeKeyValue(ctx, txPendingIndexKey(tx.SequenceID), idKey)
		}
		if err == nil {
			err = p.writeKeyValue(ctx, txNonceAllocationKey(tx.From, tx.Nonce), idKey)
		}
	}
	// If we are creating/updating a record that is not pending, we need to ensure there is no pending index associated with it
	if err == nil && tx.Status != apitypes.TxStatusPending {
		err = p.deleteKeys(ctx, txPendingIndexKey(tx.SequenceID))
	}
	if err == nil {
		err = p.writeJSON(ctx, idKey, tx)
	}
	return err
}

func (p *leveldbPersistence) writeTransaction(ctx context.Context, tx *apitypes.TXWithStatus, newTx bool) (err error) {
	p.txMux.Lock()
	defer p.txMux.Unlock()

	return p.writeTransactionLocked(ctx, tx, newTx)
}

func (p *leveldbPersistence) DeleteTransaction(ctx context.Context, txID string) error {
	var tx *apitypes.ManagedTX
	err := p.readJSON(ctx, txDataKey(txID), &tx)
	if err != nil || tx == nil {
		return err
	}
	return p.deleteKeys(ctx,
		txDataKey(txID),
		txCreatedIndexKey(tx),
		txPendingIndexKey(tx.SequenceID),
		txNonceAllocationKey(tx.TransactionHeaders.From, tx.TransactionHeaders.Nonce),
	)
}

func (p *leveldbPersistence) setSubStatusInStruct(ctx context.Context, tx *apitypes.TXWithStatus, subStatus apitypes.TxSubStatus, actionOccurred *fftypes.FFTime) {

	// See if the status being transitioned to is the same as the current status.
	// If so, there's nothing to do.
	if len(tx.History) > 0 {
		if tx.History[len(tx.History)-1].Status == subStatus {
			return
		}
		log.L(ctx).Debugf("State transition to sub-status %s", subStatus)
	}

	// If this is a change in status add a new record
	newStatus := &apitypes.TxHistoryStateTransitionEntry{
		Time:    actionOccurred,
		Status:  subStatus,
		Actions: make([]*apitypes.TxHistoryActionEntry, 0),
	}
	tx.History = append(tx.History, newStatus)

	if len(tx.History) > p.maxHistoryCount {
		// Need to trim the oldest record
		tx.History = tx.History[1:]
	}

	// As we have a possibly indefinite list of sub-status records (which might be a long list and early entries
	// get purged at some point) we keep a separate list of all the discrete types of sub-status
	// and action we've we've ever seen for this transaction along with a count of them. This means an early sub-status
	// (e.g. "queued") followed by 100s of different sub-status types will still be recorded
	for _, statusType := range tx.DeprecatedHistorySummary {
		if statusType.Status == subStatus {
			// Just increment the counter and last timestamp
			statusType.LastOccurrence = actionOccurred
			statusType.Count++
			return
		}
	}

	tx.DeprecatedHistorySummary = append(tx.DeprecatedHistorySummary, &apitypes.TxHistorySummaryEntry{Status: subStatus, Count: 1, FirstOccurrence: actionOccurred, LastOccurrence: actionOccurred})
}

func (p *leveldbPersistence) AddSubStatusAction(ctx context.Context, txID string, subStatus apitypes.TxSubStatus, action apitypes.TxAction, info *fftypes.JSONAny, errInfo *fftypes.JSONAny, actionOccurred *fftypes.FFTime) error {

	tx, err := p.getPersistedTX(ctx, txID)
	if err != nil {
		return err
	}
	if p.maxHistoryCount <= 0 {
		// if history is turned off, it's a no op
		return nil
	}

	// Ensure structure is updated to latest sub status
	p.setSubStatusInStruct(ctx, tx, subStatus, actionOccurred)

	// See if this action exists in the list already since we only want to update the single entry, not
	// add a new one
	currentSubStatus := tx.History[len(tx.History)-1]
	alreadyRecordedAction := false
	for _, entry := range currentSubStatus.Actions {
		if entry.Action == action {
			alreadyRecordedAction = true
			entry.OccurrenceCount++
			entry.LastOccurrence = actionOccurred

			if errInfo != nil {
				entry.LastError = persistence.JSONOrString(errInfo)
				entry.LastErrorTime = actionOccurred
			}

			if info != nil {
				entry.LastInfo = persistence.JSONOrString(info)
			}
			break
		}
	}

	if !alreadyRecordedAction {
		// If this is an entirely new action for this status entry, add it to the list
		newAction := &apitypes.TxHistoryActionEntry{
			Time:            actionOccurred,
			Action:          action,
			LastOccurrence:  actionOccurred,
			OccurrenceCount: 1,
		}

		if errInfo != nil {
			newAction.LastError = persistence.JSONOrString(errInfo)
			newAction.LastErrorTime = actionOccurred
		}

		if info != nil {
			newAction.LastInfo = persistence.JSONOrString(info)
		}

		currentSubStatus.Actions = append(currentSubStatus.Actions, newAction)
	}

	// Check if the history summary needs updating
	found := false
	for _, actionType := range tx.DeprecatedHistorySummary {
		if actionType.Action == action {
			// Just increment the counter and last timestamp
			actionType.LastOccurrence = actionOccurred
			actionType.Count++
			found = true
			break
		}
	}
	if !found {
		tx.DeprecatedHistorySummary = append(tx.DeprecatedHistorySummary, &apitypes.TxHistorySummaryEntry{Action: action, Count: 1, FirstOccurrence: actionOccurred, LastOccurrence: actionOccurred})
	}
	return p.writeTransaction(ctx, tx, false)
}

func (p *leveldbPersistence) Close(ctx context.Context) {
	err := p.db.Close()
	if err != nil {
		log.L(ctx).Warnf("Error closing leveldb: %s", err)
	}
}
