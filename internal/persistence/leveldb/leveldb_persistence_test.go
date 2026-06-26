// Copyright © 2024 Kaleido, Inc.
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
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func newTestLevelDBPersistence(t *testing.T) (context.Context, *leveldbPersistence, func()) {

	ctx, cancelCtx := context.WithCancel(context.Background())

	dir := t.TempDir()

	tmconfig.Reset()
	config.Set(tmconfig.PersistenceLevelDBPath, dir)

	pp, err := NewLevelDBPersistence(ctx, 1*time.Hour)
	assert.NoError(t, err)

	// Write some random stuff to the DB
	p := pp.(*leveldbPersistence)
	for i := 0; i < 26; i++ {
		letter := (byte)('a' + i)
		key := make([]byte, 10)
		for i := range key {
			key[i] = letter
		}
		err := p.db.Put(key, key, &opt.WriteOptions{})
		assert.NoError(t, err)
	}

	return ctx, p, func() {
		p.Close(ctx)
		cancelCtx()
	}

}

func strPtr(s string) *string { return &s }

type badJSONCheckpointType map[bool]bool

func (cp *badJSONCheckpointType) LessThan(b ffcapi.EventListenerCheckpoint) bool {
	return false
}

func TestLevelDBInitMissingPath(t *testing.T) {

	tmconfig.Reset()

	_, err := NewLevelDBPersistence(context.Background(), 1*time.Hour)
	assert.Regexp(t, "FF21050", err)

}

func TestLevelDBInitFail(t *testing.T) {
	file, err := ioutil.TempFile("", "ldb_*")
	assert.NoError(t, err)
	ioutil.WriteFile(file.Name(), []byte("not a leveldb"), 0777)
	defer os.Remove(file.Name())

	tmconfig.Reset()
	config.Set(tmconfig.PersistenceLevelDBPath, file.Name())

	_, err = NewLevelDBPersistence(context.Background(), 1*time.Hour)
	assert.Error(t, err)

}

func TestRichQueryNotSupported(t *testing.T) {

	_, p, done := newTestLevelDBPersistence(t)
	defer done()

	assert.Panics(t, func() {
		p.RichQuery()
	})

	assert.Panics(t, func() {
		p.TransactionCompletions()
	})

}

func TestReadWriteStreams(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	s1 := &apitypes.EventStream{
		ID:   apitypes.NewULID(), // ensure we get sequentially ascending IDs
		Name: strPtr("stream1"),
	}
	p.WriteStream(ctx, s1)
	s2 := &apitypes.EventStream{
		ID:   apitypes.NewULID(),
		Name: strPtr("stream2"),
	}
	p.WriteStream(ctx, s2)
	s3 := &apitypes.EventStream{
		ID:   apitypes.NewULID(),
		Name: strPtr("stream3"),
	}
	p.WriteStream(ctx, s3)

	streams, err := p.ListStreamsByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, streams, 3)

	assert.Equal(t, s3.ID, streams[0].ID)
	assert.Equal(t, s2.ID, streams[1].ID)
	assert.Equal(t, s1.ID, streams[2].ID)

	// Test pagination

	streams, err = p.ListStreamsByCreateTime(ctx, nil, 2, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, streams, 2)
	assert.Equal(t, s3.ID, streams[0].ID)
	assert.Equal(t, s2.ID, streams[1].ID)

	streams, err = p.ListStreamsByCreateTime(ctx, streams[1].ID, 2, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, streams, 1)
	assert.Equal(t, s1.ID, streams[0].ID)

	// Test delete

	err = p.DeleteStream(ctx, s2.ID)
	assert.NoError(t, err)
	streams, err = p.ListStreamsByCreateTime(ctx, nil, 2, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, streams, 2)
	assert.Equal(t, s3.ID, streams[0].ID)
	assert.Equal(t, s1.ID, streams[1].ID)

	// Test get direct

	s, err := p.GetStream(ctx, s3.ID)
	assert.NoError(t, err)
	assert.Equal(t, s3.ID, s.ID)
	assert.Equal(t, s3.Name, s.Name)

	s, err = p.GetStream(ctx, s2.ID)
	assert.NoError(t, err)
	assert.Nil(t, s)
}

func TestReadWriteListeners(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	sID1 := apitypes.NewULID()
	sID2 := apitypes.NewULID()

	s1l1 := &apitypes.Listener{
		ID:       apitypes.NewULID(),
		StreamID: sID1,
	}
	err := p.WriteListener(ctx, s1l1)
	assert.NoError(t, err)

	s2l1 := &apitypes.Listener{
		ID:       apitypes.NewULID(),
		StreamID: sID2,
	}
	err = p.WriteListener(ctx, s2l1)
	assert.NoError(t, err)

	s1l2 := &apitypes.Listener{
		ID:       apitypes.NewULID(),
		StreamID: sID1,
	}
	err = p.WriteListener(ctx, s1l2)
	assert.NoError(t, err)

	listeners, err := p.ListListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, listeners, 3)

	assert.Equal(t, s1l2.ID, listeners[0].ID)
	assert.Equal(t, s2l1.ID, listeners[1].ID)
	assert.Equal(t, s1l1.ID, listeners[2].ID)

	// Test stream filter

	listeners, err = p.ListStreamListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending, sID1)
	assert.NoError(t, err)
	assert.Len(t, listeners, 2)
	assert.Equal(t, s1l2.ID, listeners[0].ID)
	assert.Equal(t, s1l1.ID, listeners[1].ID)

	// Test delete

	err = p.DeleteListener(ctx, s2l1.ID)
	assert.NoError(t, err)
	listeners, err = p.ListStreamListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending, sID2)
	assert.NoError(t, err)
	assert.Len(t, listeners, 0)

	// Test get direct

	l, err := p.GetListener(ctx, s1l2.ID)
	assert.NoError(t, err)
	assert.Equal(t, s1l2.ID, l.ID)

	l, err = p.GetListener(ctx, s2l1.ID)
	assert.NoError(t, err)
	assert.Nil(t, l)
}

func TestReadWriteCheckpoints(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	cp1 := &apitypes.EventStreamCheckpoint{
		StreamID: apitypes.NewULID(),
	}
	cp2 := &apitypes.EventStreamCheckpoint{
		StreamID: apitypes.NewULID(),
	}

	err := p.WriteCheckpoint(ctx, cp1)
	assert.NoError(t, err)

	err = p.WriteCheckpoint(ctx, cp2)
	assert.NoError(t, err)

	err = p.DeleteCheckpoint(ctx, cp1.StreamID)
	assert.NoError(t, err)

	err = p.DeleteCheckpoint(ctx, cp1.StreamID)
	assert.NoError(t, err) // No-op

	cp, err := p.GetCheckpoint(ctx, cp1.StreamID)
	assert.NoError(t, err)
	assert.Nil(t, cp)

	cp, err = p.GetCheckpoint(ctx, cp2.StreamID)
	assert.NoError(t, err)
	assert.Equal(t, cp2.StreamID, cp.StreamID)
}

func newTestTX(signer string, status apitypes.TxStatus) *apitypes.ManagedTX {
	return &apitypes.ManagedTX{
		ID:      fmt.Sprintf("ns1/%s", fftypes.NewUUID()),
		Created: fftypes.Now(),
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: signer,
		},
		Status: status,
	}
}

func TestReadWriteManagedTransactions(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	submitNewTX := func(signer string, nonce int64, status apitypes.TxStatus) *apitypes.ManagedTX {
		tx := newTestTX(signer, status)
		tx.Nonce = fftypes.NewFFBigInt(nonce)
		err := p.writeTransaction(ctx, &apitypes.TXWithStatus{ManagedTX: tx}, true)
		assert.NoError(t, err)
		return tx
	}

	s1t1 := submitNewTX("0xaaaaa", 10001, apitypes.TxStatusSucceeded)
	s2t1 := submitNewTX("0xbbbbb", 10001, apitypes.TxStatusFailed)
	s1t2 := submitNewTX("0xaaaaa", 10002, apitypes.TxStatusPending)
	s1t3 := submitNewTX("0xaaaaa", 10003, apitypes.TxStatusPending)

	// Check new transaction should not have a sequenceID
	err := p.writeTransaction(ctx, &apitypes.TXWithStatus{ManagedTX: s1t1}, true)
	assert.Regexp(t, "FF21075", err)

	// Check dup
	s1t1.SequenceID = ""
	err = p.writeTransaction(ctx, &apitypes.TXWithStatus{ManagedTX: s1t1}, true)
	assert.Regexp(t, "FF21065", err)

	txns, err := p.ListTransactionsByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, txns, 4)

	assert.Equal(t, s1t3.ID, txns[0].ID)
	assert.Equal(t, s1t2.ID, txns[1].ID)
	assert.Equal(t, s2t1.ID, txns[2].ID)
	assert.Equal(t, s1t1.ID, txns[3].ID)

	// Only list pending

	txns, err = p.ListTransactionsPending(ctx, "", 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, txns, 2)

	assert.Equal(t, s1t3.ID, txns[0].ID)
	assert.Equal(t, s1t2.ID, txns[1].ID)

	// List with time range

	txns, err = p.ListTransactionsByCreateTime(ctx, s1t2, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, txns, 2)
	assert.Equal(t, s2t1.ID, txns[0].ID)
	assert.Equal(t, s1t1.ID, txns[1].ID)

	// Test delete, and querying by nonce to limit TX returned

	err = p.DeleteTransaction(ctx, s1t2.ID)
	assert.NoError(t, err)
	txns, err = p.ListTransactionsByNonce(ctx, "0xaaaaa", s1t1.Nonce, 0, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, txns, 1)
	assert.Equal(t, s1t3.ID, txns[0].ID)

	// Check we can use after with the deleted nonce, and not skip the one after
	txns, err = p.ListTransactionsByNonce(ctx, "0xaaaaa", s1t2.Nonce, 0, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, txns, 1)
	assert.Equal(t, s1t3.ID, txns[0].ID)

	// Test get direct

	v, err := p.GetTransactionByID(ctx, s1t3.ID)
	assert.NoError(t, err)
	assert.Equal(t, s1t3.ID, v.ID)
	assert.Equal(t, s1t3.Nonce, v.Nonce)

	v, err = p.GetTransactionByNonce(ctx, "0xbbbbb", s2t1.Nonce)
	assert.NoError(t, err)
	assert.Equal(t, s2t1.ID, v.ID)
	assert.Equal(t, s2t1.Nonce, v.Nonce)

	v, err = p.GetTransactionByID(ctx, s1t2.ID)
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestInsertTransactionsDupCheckFail(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	done()

	tx := newTestTX("0x12345", apitypes.TxStatusPending)
	tx.Nonce = fftypes.NewFFBigInt(12345)
	err := p.InsertTransactionPreAssignedNonce(ctx, tx)
	assert.Regexp(t, "FF21055", err)

}

func TestListStreamsBadJSON(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	sID := apitypes.NewULID()
	err := p.db.Put(prefixedKey(eventstreamsPrefix, sID), []byte("{! not json"), &opt.WriteOptions{})
	assert.NoError(t, err)

	_, err = p.ListStreamsByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.Error(t, err)

}

func TestListListenersBadJSON(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	lID := apitypes.NewULID()
	err := p.db.Put(prefixedKey(listenersPrefix, lID), []byte("{! not json"), &opt.WriteOptions{})
	assert.NoError(t, err)

	_, err = p.ListListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.Error(t, err)

	_, err = p.ListStreamListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending, apitypes.NewULID())
	assert.Error(t, err)

}

func TestDeleteStreamFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	err := p.DeleteStream(ctx, apitypes.NewULID())
	assert.Error(t, err)

}

func TestReadWriteTXFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	tx := newTestTX("0x1234", apitypes.TxStatusPending)

	err := p.InsertTransactionWithNextNonce(ctx, tx, func(ctx context.Context, signer string) (uint64, error) { return 1000, nil })
	assert.Error(t, err)

	_, err = p.GetTransactionByID(ctx, tx.ID)
	assert.Error(t, err)

	_, err = p.GetTransactionReceipt(ctx, tx.ID)
	assert.Error(t, err)

	_, err = p.GetTransactionConfirmations(ctx, tx.ID)
	assert.Error(t, err)

}

func TestUpdateTXFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	tx := newTestTX("0x1234", apitypes.TxStatusPending)

	err := p.UpdateTransaction(ctx, tx.ID, &apitypes.TXUpdates{})
	assert.Error(t, err)

}

func TestAddSubStatusActionFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	tx := newTestTX("0x1234", apitypes.TxStatusPending)

	err := p.AddSubStatusAction(ctx, tx.ID, apitypes.TxSubStatusTracking, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
	assert.Error(t, err)

}

func TestWriteCheckpointFailMarshal(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	id1 := apitypes.NewULID()
	err := p.WriteCheckpoint(ctx, &apitypes.EventStreamCheckpoint{
		Listeners: map[fftypes.UUID]json.RawMessage{
			*id1: json.RawMessage([]byte(`{"bad": "json"!`)),
		},
	})
	assert.Error(t, err)

}

func TestWriteCheckpointFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	id1 := apitypes.NewULID()
	err := p.WriteCheckpoint(ctx, &apitypes.EventStreamCheckpoint{
		Listeners: map[fftypes.UUID]json.RawMessage{
			*id1: json.RawMessage([]byte(`{}`)),
		},
	})
	assert.Error(t, err)

}

func TestReadListenerFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	p.db.Close()

	_, err := p.GetListener(ctx, apitypes.NewULID())
	assert.Error(t, err)

}

func TestReadCheckpointFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	sID := apitypes.NewULID()
	err := p.db.Put(prefixedKey(checkpointsPrefix, sID), []byte("{! not json"), &opt.WriteOptions{})
	assert.NoError(t, err)

	_, err = p.GetCheckpoint(ctx, sID)
	assert.Error(t, err)

}

func TestListManagedTransactionFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	tx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", apitypes.NewULID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
	}
	err := p.writeKeyValue(ctx, txCreatedIndexKey(tx), txDataKey(tx.ID))
	assert.NoError(t, err)
	err = p.db.Put(txDataKey(tx.ID), []byte("{! not json"), &opt.WriteOptions{})
	assert.NoError(t, err)

	_, err = p.ListTransactionsByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.Error(t, err)

}

func TestListManagedTransactionCleanupOrphans(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	tx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", apitypes.NewULID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
	}
	err := p.writeKeyValue(ctx, txCreatedIndexKey(tx), txDataKey(tx.ID))
	assert.NoError(t, err)

	txns, err := p.ListTransactionsByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Empty(t, txns)

	cleanedUpIndex, err := p.getKeyValue(ctx, txCreatedIndexKey(tx))
	assert.NoError(t, err)
	assert.Nil(t, cleanedUpIndex)

}

func TestListNonceAllocationsFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	txID := fmt.Sprintf("ns1:%s", apitypes.NewULID())
	err := p.writeKeyValue(ctx, txNonceAllocationKey("0xaaa", fftypes.NewFFBigInt(12345)), txDataKey(txID))
	assert.NoError(t, err)
	err = p.db.Put(txDataKey(txID), []byte("{! not json"), &opt.WriteOptions{})
	assert.NoError(t, err)

	_, err = p.ListTransactionsByNonce(ctx, "0xaaa", nil, 0, txhandler.SortDirectionDescending)
	assert.Error(t, err)

}

func TestListInflightTransactionFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	txID := fmt.Sprintf("ns1:%s", apitypes.NewULID())
	err := p.writeKeyValue(ctx, txPendingIndexKey(apitypes.NewULID().String()), txDataKey(txID))
	assert.NoError(t, err)
	err = p.db.Put(txDataKey(txID), []byte("{! not json"), &opt.WriteOptions{})
	assert.NoError(t, err)

	_, err = p.ListTransactionsPending(ctx, "", 0, txhandler.SortDirectionDescending)
	assert.Error(t, err)

}

func TestIndexLookupCallbackErr(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	p.Close(ctx)

	_, err := p.indexLookupCallback(ctx, ([]byte("any key")))
	assert.NotNil(t, err)

}

func TestIndexLookupCallbackNotFound(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	b, err := p.indexLookupCallback(ctx, ([]byte("any key")))
	assert.Nil(t, err)
	assert.Nil(t, b)

}

func TestGetTransactionByNonceFail(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	p.Close(ctx)

	_, err := p.GetTransactionByNonce(ctx, "0xaaa", fftypes.NewFFBigInt(12345))
	assert.Regexp(t, "FF21055", err)

}

func TestIterateReverseJSONFailIdxResolve(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	err := p.writeKeyValue(ctx, []byte(`test_0/key`), []byte(`test/value`))
	assert.NoError(t, err)
	_, err = p.listJSON(ctx,
		"test_0/",
		"test_1",
		"",
		0,
		txhandler.SortDirectionAscending,
		func() interface{} { return make(map[string]interface{}) },
		func(i interface{}) {},
		func(ctx context.Context, k []byte) ([]byte, error) {
			return nil, fmt.Errorf("pop")
		},
	)
	assert.Regexp(t, "pop", err)

}

func TestIterateReverseJSONSkipIdxResolve(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	err := p.writeKeyValue(ctx, []byte(`test_0/key`), []byte(`test/value`))
	assert.NoError(t, err)
	orphans, err := p.listJSON(ctx,
		"test_0/",
		"test_1",
		"",
		0,
		txhandler.SortDirectionAscending,
		func() interface{} { return make(map[string]interface{}) },
		func(_ interface{}) {
			assert.Fail(t, "Should not be called")
		},
		func(ctx context.Context, k []byte) ([]byte, error) {
			return nil, nil
		},
	)
	assert.NoError(t, err)
	assert.Len(t, orphans, 1)

}

func TestCleanupOrphanedTXIdxKeysSwallowError(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	p.Close(ctx)

	p.cleanupOrphanedTXIdxKeys(ctx, [][]byte{[]byte("test")})

}

func TestWriteTransactionIncomplete(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	err := p.InsertTransactionWithNextNonce(ctx, &apitypes.ManagedTX{}, func(ctx context.Context, signer string) (uint64, error) { return 1000, nil })
	assert.Regexp(t, "FF21059", err)

}

func TestDeleteTransactionMissing(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	err := p.DeleteTransaction(ctx, "missing")
	assert.NoError(t, err)

}

func TestManagedTXDeprecatedTransactionHeadersMigration(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	txh := apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{
			DeprecatedTransactionHeaders: &ffcapi.TransactionHeaders{
				From:  "0x12345",
				To:    "0x23456",
				Nonce: fftypes.NewFFBigInt(11111),
				Gas:   fftypes.NewFFBigInt(22222),
				Value: fftypes.NewFFBigInt(33333),
			},
			ID:      fftypes.NewUUID().String(),
			Created: fftypes.Now(),
			Status:  apitypes.TxStatusPending,
		},
	}

	err := p.writeJSON(ctx, txDataKey(txh.ID), txh)
	assert.NoError(t, err)

	tx, err := p.GetTransactionByID(ctx, txh.ID)
	assert.NoError(t, err)
	assert.Equal(t, "0x12345", tx.From)
	assert.Equal(t, "0x23456", tx.To)
	assert.Equal(t, int64(11111), tx.Nonce.Int64())
	assert.Equal(t, int64(22222), tx.Gas.Int64())
	assert.Equal(t, int64(33333), tx.Value.Int64())

	assert.Equal(t, "0x12345", tx.DeprecatedTransactionHeaders.From)
	assert.Equal(t, "0x23456", tx.DeprecatedTransactionHeaders.To)
	assert.Equal(t, int64(11111), tx.DeprecatedTransactionHeaders.Nonce.Int64())
	assert.Equal(t, int64(22222), tx.DeprecatedTransactionHeaders.Gas.Int64())
	assert.Equal(t, int64(33333), tx.DeprecatedTransactionHeaders.Value.Int64())
}

func TestManagedTXUpdate(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	mtx := &apitypes.ManagedTX{
		ID: fftypes.NewUUID().String(),
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x11111",
			To:    "0x22222",
			Gas:   fftypes.NewFFBigInt(22222),
			Value: fftypes.NewFFBigInt(33333),
		},
		Created:    fftypes.Now(),
		PolicyInfo: fftypes.JSONAnyPtr(`{"some":"data to clear"}`),
		Status:     apitypes.TxStatusPending,
	}

	// Error if not stored at all
	err := p.UpdateTransaction(ctx, mtx.ID, &apitypes.TXUpdates{})
	assert.Regexp(t, "FF21067", err)

	err = p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 11111, nil })
	assert.NoError(t, err)

	err = p.UpdateTransaction(ctx, mtx.ID, &apitypes.TXUpdates{}) // no-op
	assert.NoError(t, err)

	tx, err := p.GetTransactionByID(ctx, mtx.ID)
	assert.NoError(t, err)
	tx.Updated = nil // for compare
	tx.DeprecatedTransactionHeaders = nil
	assert.Equal(t, mtx, tx)

	newStatus := apitypes.TxStatusFailed
	delTime := fftypes.Now()
	newFrom := "0x33333"
	newTo := "0x44444"
	newTXData := "new data"
	newTXHash := "new hash"
	firstSubmitTime := fftypes.Now()
	lastSubmitTime := fftypes.Now()
	var nullString fftypes.JSONAny = fftypes.NullString
	newError := "new error"
	err = p.UpdateTransaction(ctx, mtx.ID, &apitypes.TXUpdates{
		Status:          &newStatus,
		DeleteRequested: delTime,
		From:            &newFrom,
		To:              &newTo,
		Nonce:           fftypes.NewFFBigInt(44444),
		Gas:             fftypes.NewFFBigInt(55555),
		Value:           fftypes.NewFFBigInt(66666),
		GasPrice:        fftypes.JSONAnyPtr(`"gas price"`),
		TransactionData: &newTXData,
		TransactionHash: &newTXHash,
		PolicyInfo:      &nullString,
		FirstSubmit:     firstSubmitTime,
		LastSubmit:      lastSubmitTime,
		ErrorMessage:    &newError,
	})
	assert.NoError(t, err)

	tx, err = p.GetTransactionByID(ctx, mtx.ID)
	assert.NoError(t, err)
	assert.Equal(t, newStatus, tx.Status)
	assert.Equal(t, delTime, tx.DeleteRequested)
	assert.Equal(t, newFrom, tx.From)
	assert.Equal(t, newTo, tx.To)
	assert.Equal(t, fftypes.NewFFBigInt(44444), tx.Nonce)
	assert.Equal(t, fftypes.NewFFBigInt(55555), tx.Gas)
	assert.Equal(t, fftypes.NewFFBigInt(66666), tx.Value)
	assert.Equal(t, []byte(`"gas price"`), tx.GasPrice.Bytes())
	assert.Equal(t, newTXData, tx.TransactionData)
	assert.Equal(t, newTXHash, tx.TransactionHash)
	assert.Nil(t, tx.PolicyInfo)
	assert.Equal(t, firstSubmitTime, tx.FirstSubmit)
	assert.Equal(t, lastSubmitTime, tx.LastSubmit)
	assert.Equal(t, newError, tx.ErrorMessage)

}

func TestManagedTXSubStatus(t *testing.T) {
	mtx := &apitypes.ManagedTX{
		ID: fftypes.NewUUID().String(),
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x12345",
			Nonce: fftypes.NewFFBigInt(12345),
		},
		Created: fftypes.Now(),
		Status:  apitypes.TxStatusPending,
	}
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	err := p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	// Adding the same sub-status lots of times in succession should only result
	// in a single entry for that instance
	for i := 0; i < 100; i++ {
		err := p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
		assert.NoError(t, err)
	}

	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(txh.History))
	assert.Equal(t, "Received", string(txh.History[0].Status))

	// Adding a different type of sub-status should result in
	// a new entry in the list
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusTracking, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
	assert.NoError(t, err)

	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(txh.History))

	// Even if many new types are added we shouldn't go over the
	// configured upper limit
	for i := 0; i < 100; i++ {
		err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusStale, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
		assert.NoError(t, err)
		err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusTracking, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
		assert.NoError(t, err)
	}

	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 50, len(txh.History))

}

func TestManagedTXSubStatusRepeat(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)
	err := p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	// Add a sub-status
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(txh.History))
	assert.Equal(t, 2, len(txh.DeprecatedHistorySummary))

	// Add another sub-status
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusTracking, apitypes.TxActionSubmitTransaction, nil, nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.Equal(t, 2, len(txh.History))
	assert.Equal(t, 4, len(txh.DeprecatedHistorySummary))

	// Add another that we've seen before
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, nil, nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.Equal(t, 3, len(txh.History))                  // This goes up
	assert.Equal(t, 4, len(txh.DeprecatedHistorySummary)) // This doesn't
}

func TestManagedTXSubStatusAction(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)

	err := p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
	assert.Regexp(t, "FF21067", err)

	err = p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	// Add an action
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(txh.History[0].Actions))
	assert.Nil(t, txh.History[0].Actions[0].LastErrorTime)

	// Add another action
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionRetrieveGasPrice, nil, fftypes.JSONAnyPtr(`{"gasError":"Acme Gas Oracle RC=12345"}`), fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(txh.History[0].Actions))
	assert.Equal(t, (*txh.History[0].Actions[1].LastError).String(), `{"gasError":"Acme Gas Oracle RC=12345"}`)

	// Add the same action which should cause the previous one to inc its counter
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionRetrieveGasPrice, fftypes.JSONAnyPtr(`{"info":"helloworld"}`), fftypes.JSONAnyPtr(`{"error":"nogood"}`), fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(txh.History[0].Actions))
	assert.Equal(t, txh.History[0].Actions[1].Action, apitypes.TxActionRetrieveGasPrice)
	assert.Equal(t, 2, txh.History[0].Actions[1].OccurrenceCount)

	// Add the same action but with new error information should update the last error field
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionRetrieveGasPrice, nil, fftypes.JSONAnyPtr(`{"gasError":"Acme Gas Oracle RC=67890"}`), fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(txh.History[0].Actions))
	assert.NotNil(t, txh.History[0].Actions[1].LastErrorTime)
	assert.Equal(t, (*txh.History[0].Actions[1].LastError).String(), `{"gasError":"Acme Gas Oracle RC=67890"}`)

	// Add a new type of action
	reason := "known_transaction"
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, fftypes.JSONAnyPtr(`{"reason":"`+reason+`"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(txh.History[0].Actions))
	assert.Equal(t, txh.History[0].Actions[2].Action, apitypes.TxActionSubmitTransaction)
	assert.Equal(t, 1, txh.History[0].Actions[2].OccurrenceCount)
	assert.Nil(t, txh.History[0].Actions[2].LastErrorTime)

	// Add one more type of action

	receiptId := "123456"
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionReceiveReceipt, fftypes.JSONAnyPtr(`{"receiptId":"`+receiptId+`"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(txh.History[0].Actions))
	assert.Equal(t, txh.History[0].Actions[3].Action, apitypes.TxActionReceiveReceipt)
	assert.Equal(t, 1, txh.History[0].Actions[3].OccurrenceCount)
	assert.Nil(t, txh.History[0].Actions[3].LastErrorTime)

	// History is the complete list of unique sub-status types and actions
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(txh.DeprecatedHistorySummary))

	// Add some new sub-status and actions to check max lengths are correct
	// Seen one of these before - should increase summary length by 1
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusConfirmed, apitypes.TxActionReceiveReceipt, fftypes.JSONAnyPtr(`{"receiptId":"`+receiptId+`"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.Equal(t, 6, len(txh.DeprecatedHistorySummary))

	// Seen both of these before - no change expected
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(txh.DeprecatedHistorySummary))

	// Sanity check the history summary entries
	for _, historyEntry := range txh.DeprecatedHistorySummary {
		assert.NotNil(t, historyEntry.FirstOccurrence)
		assert.NotNil(t, historyEntry.LastOccurrence)
		assert.GreaterOrEqual(t, historyEntry.Count, 1)

		if historyEntry.Action == apitypes.TxActionRetrieveGasPrice {
			// The first and last occurrence timestamps shoudn't be the same
			assert.NotEqual(t, historyEntry.FirstOccurrence, historyEntry.LastOccurrence)

			// We should have a count of 3 for this action
			assert.Equal(t, historyEntry.Count, 3)
		}
	}
}

func TestSetReceipt(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)

	receipt := &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{Success: true},
	}

	err := p.SetTransactionReceipt(ctx, mtx.ID, receipt)
	assert.Regexp(t, "FF21067", err)

	err = p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	err = p.SetTransactionReceipt(ctx, mtx.ID, receipt)
	assert.NoError(t, err)

	receipt, err = p.GetTransactionReceipt(ctx, mtx.ID)
	assert.NoError(t, err)
	assert.True(t, receipt.Success)
}

func TestAddConfirmations(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)

	conf1 := &apitypes.Confirmation{
		BlockNumber: fftypes.FFuint64(11111),
	}
	conf2 := &apitypes.Confirmation{
		BlockNumber: fftypes.FFuint64(22222),
	}

	err := p.AddTransactionConfirmations(ctx, mtx.ID, false, conf1, conf2)
	assert.Regexp(t, "FF21067", err)

	err = p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	err = p.AddTransactionConfirmations(ctx, mtx.ID, false, conf1)
	assert.NoError(t, err)

	err = p.AddTransactionConfirmations(ctx, mtx.ID, false, conf2)
	assert.NoError(t, err)

	confirmations, err := p.GetTransactionConfirmations(ctx, mtx.ID)
	assert.NoError(t, err)
	assert.Equal(t, []*apitypes.Confirmation{conf1, conf2}, confirmations)

	err = p.AddTransactionConfirmations(ctx, mtx.ID, true, conf2)
	assert.NoError(t, err)

	confirmations, err = p.GetTransactionConfirmations(ctx, mtx.ID)
	assert.NoError(t, err)
	assert.Equal(t, []*apitypes.Confirmation{conf2}, confirmations)

}

func TestManagedTXSubStatusInvalidJSON(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)
	err := p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	reason := "\"cannot-marshall\""

	// Add a new type of action
	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, fftypes.JSONAnyPtr(`{"reason":"`+reason+`"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	val, err := json.Marshal(txh.History[0].Actions[0].LastInfo)

	// It should never be possible to cause the sub-status history to become un-marshallable
	assert.NoError(t, err)
	assert.Contains(t, string(val), "cannot-marshall")

}

func TestManagedTXSubStatusMaxEntries(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	mtx := newTestTX("0x12345", apitypes.TxStatusPending)
	err := p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)
	var nextSubStatus apitypes.TxSubStatus

	// Create 100 unique sub-status strings. We should only keep the
	// first 50
	for i := 0; i < 100; i++ {
		nextSubStatus = apitypes.TxSubStatus(fmt.Sprint(i))
		p.AddSubStatusAction(ctx, mtx.ID, nextSubStatus, apitypes.TxActionAssignNonce, nil, nil, fftypes.Now())
		assert.NoError(t, err)
	}

	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 50, len(txh.History))

}

func TestMaxHistoryCountSetToZero(t *testing.T) {
	tmconfig.Reset()
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	p.maxHistoryCount = 0
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)
	err := p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, nil, nil, fftypes.Now())
	assert.NoError(t, err)

	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(txh.History))
	assert.Equal(t, 0, len(txh.DeprecatedHistorySummary))

}

func TestAddReceivedStatusWhenNothingSet(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)

	defer done()
	mtx := newTestTX("0x12345", apitypes.TxStatusPending)
	err := p.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) { return 12345, nil })
	assert.NoError(t, err)

	txh, err := p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.NotNil(t, 0, txh.History)
	assert.Equal(t, 0, len(txh.History))

	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, false)
	assert.NoError(t, err)
	assert.Nil(t, txh.History)

	err = p.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, nil, nil, fftypes.Now())
	assert.NoError(t, err)

	txh, err = p.GetTransactionByIDWithStatus(ctx, mtx.ID, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(txh.History))
	assert.Equal(t, 1, len(txh.History[0].Actions))
	assert.Equal(t, apitypes.TxSubStatusReceived, txh.History[0].Status)
	assert.Equal(t, apitypes.TxActionSubmitTransaction, txh.History[0].Actions[0].Action)
}

func TestInsertTransactionsWithNextNonceLevelDB(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	// test a batch using the same signing address
	nonce := uint64(1000)
	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		nonce++
		return nonce, nil
	}

	txs := make([]*apitypes.ManagedTX, 5)
	for i := 0; i < 5; i++ {
		txs[i] = &apitypes.ManagedTX{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_0",
			},
			TransactionData: fmt.Sprintf("0x%04d", i),
		}
	}

	errs := p.InsertTransactionsWithNextNonce(ctx, txs, nextNonceCB)
	assert.Len(t, errs, 5)
	for i, err := range errs {
		assert.NoError(t, err, "Transaction %d should succeed", i)
		assert.NotEmpty(t, txs[i].SequenceID, "Transaction %d should have sequence ID", i)
		assert.NotNil(t, txs[i].Nonce, "Transaction %d should have nonce", i)
		// Verify nonces are sequential
		if i > 0 {
			assert.Equal(t, txs[i-1].Nonce.Int64()+1, txs[i].Nonce.Int64(), "Nonces should be sequential")
		}
	}

	// verify all transactions were persisted
	for i, tx := range txs {
		retrieved, err := p.GetTransactionByID(ctx, tx.ID)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved, "Transaction %d should be retrievable", i)
		assert.Equal(t, tx.ID, retrieved.ID)
		assert.Equal(t, tx.Nonce, retrieved.Nonce)
	}
}

func TestInsertTransactionsWithNextNonceMixedSignersLevelDB(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	// test a batch using different signing addresses
	nonce1 := uint64(2000)
	nonce2 := uint64(3000)
	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		if signer == "signer_1" {
			nonce1++
			return nonce1, nil
		}
		nonce2++
		return nonce2, nil
	}

	txs := []*apitypes.ManagedTX{
		{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_1",
			},
		},
		{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_2",
			},
		},
		{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_1",
			},
		},
	}

	errs := p.InsertTransactionsWithNextNonce(ctx, txs, nextNonceCB)
	assert.Len(t, errs, 3)
	for i, err := range errs {
		assert.NoError(t, err, "Transaction %d should succeed", i)
		assert.NotEmpty(t, txs[i].SequenceID, "Transaction %d should have sequence ID", i)
	}
}

func TestInsertTransactionsWithNextNonceInvalidLevelDB(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	// test a batch with an invalid transaction (missing From)
	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		return 1000, nil
	}

	txs := []*apitypes.ManagedTX{
		{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_0",
			},
		},
		{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "", // Invalid - missing From
			},
		},
		{
			ID:      fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status:  apitypes.TxStatusPending,
			Created: fftypes.Now(),
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_0",
			},
		},
	}

	errs := p.InsertTransactionsWithNextNonce(ctx, txs, nextNonceCB)
	assert.Len(t, errs, 3)
	assert.NoError(t, errs[0], "First transaction should succeed")
	assert.Error(t, errs[1], "Second transaction should fail (invalid)")
	assert.NoError(t, errs[2], "Third transaction should succeed")

	// verify valid transactions were persisted
	retrieved1, err := p.GetTransactionByID(ctx, txs[0].ID)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved1)

	retrieved3, err := p.GetTransactionByID(ctx, txs[2].ID)
	assert.NoError(t, err)
	assert.NotNil(t, retrieved3)
}

func TestInsertTransactionsWithNextNonceEmptyLevelDB(t *testing.T) {
	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		return 1000, nil
	}

	// test an empty batch
	errs := p.InsertTransactionsWithNextNonce(ctx, []*apitypes.ManagedTX{}, nextNonceCB)
	assert.Nil(t, errs)
}
