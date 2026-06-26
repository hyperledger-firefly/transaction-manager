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

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTXHistoryCompressionPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t, func(conf config.Section) {
		conf.Set(ConfigTXWriterHistoryCompactionInterval, "5m")
	})
	defer done()

	// Write an initial transaction
	txID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
	tx := &apitypes.ManagedTX{
		ID:     txID,
		Status: apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x111111",
			Nonce: fftypes.NewFFBigInt(333333),
		},
	}
	err := p.InsertTransactionWithNextNonce(ctx, tx, func(ctx context.Context, signer string) (uint64, error) {
		return 0, nil
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, tx.SequenceID)

	// Add a bunch of simulated status entries, which are good for compression
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce,
		fftypes.JSONAnyPtr(`{"nonce":"11111"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction,
		nil, fftypes.JSONAnyPtr(`"failed to submit 111"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction,
		nil, fftypes.JSONAnyPtr(`"failed to submit 222"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction,
		nil, fftypes.JSONAnyPtr(`"failed to submit 333"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionSubmitTransaction,
		fftypes.JSONAnyPtr(`{"transactionHash":"0x12345"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionReceiveReceipt,
		fftypes.JSONAnyPtr(`{"transactionHash":"0x111111","blockNumber":"11111"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionReceiveReceipt,
		fftypes.JSONAnyPtr(`{"transactionHash":"0x222222","blockNumber":"22222"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionReceiveReceipt,
		fftypes.JSONAnyPtr(`{"transactionHash":"0x333333","blockNumber":"33333"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionTimeout,
		nil, fftypes.JSONAnyPtr(`"some error 444"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionTimeout,
		nil, fftypes.JSONAnyPtr(`"some error 555"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusConfirmed, apitypes.TxActionConfirmTransaction,
		fftypes.JSONAnyPtr(`{"confirmed":true}`), nil, fftypes.Now())
	assert.NoError(t, err)

	// Flush with a TX update
	newStatus := apitypes.TxStatusSucceeded
	err = p.UpdateTransaction(ctx, txID, &apitypes.TXUpdates{
		Status: &newStatus,
	})
	assert.NoError(t, err)

	// Compress the state
	err = p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		return p.compressHistory(ctx, txID)
	})
	assert.NoError(t, err)

	// Get the history
	fb := p.NewTxHistoryFilter(ctx)
	history, _, err := p.ListTransactionHistory(ctx, txID, fb.And())
	assert.NoError(t, err)
	// Time strip the history for compare
	for _, h := range history {
		assert.NotNil(t, h.Time)
		if h.LastError != nil {
			assert.NotNil(t, h.LastErrorTime)
		}
		assert.NotZero(t, h.OccurrenceCount)
		if h.OccurrenceCount == 1 {
			assert.Equal(t, h.LastOccurrence.String(), h.Time.String())
		} else {
			assert.GreaterOrEqual(t, *h.LastOccurrence.Time(), *h.Time.Time())
		}
		h.ID = nil
		h.Time = nil
		h.LastOccurrence = nil
		h.LastErrorTime = nil
	}
	assert.Equal(t, []*apitypes.TXHistoryRecord{
		{
			TransactionID: txID,
			SubStatus:     apitypes.TxSubStatusConfirmed,
			TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
				Action:          apitypes.TxActionConfirmTransaction,
				OccurrenceCount: 1,
				LastInfo:        fftypes.JSONAnyPtr(`{"confirmed":true}`),
			},
		},
		{
			TransactionID: txID,
			SubStatus:     apitypes.TxSubStatusTracking,
			TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
				Action:          apitypes.TxActionTimeout,
				OccurrenceCount: 2,
				LastError:       fftypes.JSONAnyPtr(`"some error 555"`),
			},
		},
		{
			TransactionID: txID,
			SubStatus:     apitypes.TxSubStatusTracking,
			TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
				Action:          apitypes.TxActionReceiveReceipt,
				OccurrenceCount: 3,
				LastInfo:        fftypes.JSONAnyPtr(`{"transactionHash":"0x333333","blockNumber":"33333"}`),
			},
		},
		{
			TransactionID: txID,
			SubStatus:     apitypes.TxSubStatusTracking,
			TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
				Action:          apitypes.TxActionSubmitTransaction,
				OccurrenceCount: 1,
				LastInfo:        fftypes.JSONAnyPtr(`{"transactionHash":"0x12345"}`),
			},
		},
		{
			TransactionID: txID,
			SubStatus:     apitypes.TxSubStatusReceived,
			TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
				Action:          apitypes.TxActionSubmitTransaction,
				OccurrenceCount: 3,
				LastError:       fftypes.JSONAnyPtr(`"failed to submit 333"`),
			},
		},
		{
			TransactionID: txID,
			SubStatus:     apitypes.TxSubStatusReceived,
			TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
				Action:          apitypes.TxActionAssignNonce,
				OccurrenceCount: 1,
				LastInfo:        fftypes.JSONAnyPtr(`{"nonce":"11111"}`),
			},
		},
	}, history)

}

func TestCompactionFail(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterHistoryCompactionInterval, "5m")
	})
	defer done()

	longAgo := time.Now().Add(-1000 * time.Hour)
	p.writer.txMetaCache.Add("tx1", &txCacheEntry{
		lastCompacted: (*fftypes.FFTime)(&longAgo),
	})
	b := &transactionWriterBatch{
		compressionChecks: map[string]bool{
			"tx1": true,
		},
	}

	// Simulate two duplicate rows to compress
	mdb.ExpectBegin()
	mdb.ExpectQuery("SELECT.*txhistory").WillReturnRows(
		sqlmock.NewRows(append([]string{p.db.SequenceColumn()}, p.txHistory.Columns...)).
			AddRow(
				1111,
				fftypes.NewUUID().String(),
				"tx1",
				"status1",
				"action1",
				1,
				fftypes.Now().String(),
				fftypes.Now().String(),
				nil,
				nil,
				nil,
			).
			AddRow(
				2222,
				fftypes.NewUUID().String(),
				"tx1",
				"status1",
				"action1",
				1,
				fftypes.Now().String(),
				fftypes.Now().String(),
				nil,
				nil,
				nil,
			),
	)
	mdb.ExpectExec("UPDATE.*txhistory").WillReturnError(fmt.Errorf("pop"))
	mdb.ExpectRollback()

	err := p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		return p.writer.executeBatchOps(ctx, b)
	})
	assert.Regexp(t, "FF00178", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestCompactionOnCacheMiss(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterHistoryCompactionInterval, "5m")
	})
	defer done()

	b := &transactionWriterBatch{
		compressionChecks: map[string]bool{
			"tx1": true,
		},
	}

	// Simulate two duplicate rows to compress
	mdb.ExpectBegin()
	mdb.ExpectQuery("SELECT.*txhistory").WillReturnRows(
		sqlmock.NewRows(append([]string{p.db.SequenceColumn()}, p.txHistory.Columns...)),
	)
	mdb.ExpectCommit()

	err := p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		return p.writer.executeBatchOps(ctx, b)
	})
	assert.NoError(t, err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}
