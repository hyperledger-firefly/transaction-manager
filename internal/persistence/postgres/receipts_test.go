// Copyright © 2023 Kaleido, Inc.
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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestReceiptsInsertTwoInOneCyclePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterCount, 1)            // send them all to one worker
		dbconf.Set(ConfigTXWriterBatchTimeout, "10s") // ensure it triggers with the whole thing
		dbconf.Set(ConfigTXWriterBatchSize, 4)        // this must exactly match the number of ops
	})
	defer done()

	// Write initial transactions
	tx1ID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
	tx1 := &apitypes.ManagedTX{ID: tx1ID, Status: apitypes.TxStatusPending, TransactionHeaders: ffcapi.TransactionHeaders{From: "0x12345"}}
	// deliberately do not wait for the inserts
	insertOp := newTransactionOperation(tx1ID)
	insertOp.txInsert = tx1
	insertOp.nextNonceCB = func(ctx context.Context, signer string) (uint64, error) { return 0, nil }
	p.writer.queue(ctx, insertOp)

	// Insert receipt
	err := p.SetTransactionReceipt(ctx, tx1ID, &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			BlockNumber: fftypes.NewFFBigInt(12345),
		},
	})
	assert.NoError(t, err)

	// Immediately replace it in the same cycle
	err = p.SetTransactionReceipt(ctx, tx1ID, &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			BlockNumber: fftypes.NewFFBigInt(23456),
		},
	})
	assert.NoError(t, err)

	// Force a flush with a TX update
	newStatus := apitypes.TxStatusSucceeded
	err = p.UpdateTransaction(ctx, tx1ID, &apitypes.TXUpdates{
		Status: &newStatus,
	})
	assert.NoError(t, err)

	// Query the receipt to check the right one won
	r, err := p.GetTransactionReceipt(ctx, tx1ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(23456), r.BlockNumber.Int64())

}

func TestReceiptsReplaceInAnotherCyclePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterCount, 1)            // send them all to one worker
		dbconf.Set(ConfigTXWriterBatchTimeout, "10s") // ensure it triggers with the whole thing
		dbconf.Set(ConfigTXWriterBatchSize, 2)        // this must exactly match the number of ops
	})
	defer done()

	// Write initial transactions
	tx1ID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
	tx1 := &apitypes.ManagedTX{ID: tx1ID, Status: apitypes.TxStatusPending, TransactionHeaders: ffcapi.TransactionHeaders{From: "0x12345"}}
	// deliberately do not wait for the inserts
	insertOp := newTransactionOperation(tx1ID)
	insertOp.txInsert = tx1
	insertOp.nextNonceCB = func(ctx context.Context, signer string) (uint64, error) { return 0, nil }
	p.writer.queue(ctx, insertOp)

	// Insert receipt
	err := p.SetTransactionReceipt(ctx, tx1ID, &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			BlockNumber: fftypes.NewFFBigInt(12345),
		},
	})
	assert.NoError(t, err)

	// Replace it in the next cycle (note ConfigTXWriterBatchSize at head of test)
	err = p.SetTransactionReceipt(ctx, tx1ID, &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			BlockNumber: fftypes.NewFFBigInt(23456),
		},
	})
	assert.NoError(t, err)

	// Force a flush with a TX update
	newStatus := apitypes.TxStatusSucceeded
	err = p.UpdateTransaction(ctx, tx1ID, &apitypes.TXUpdates{
		Status: &newStatus,
	})
	assert.NoError(t, err)

	// Query the receipt to check the right one won
	r, err := p.GetTransactionReceipt(ctx, tx1ID)
	assert.NoError(t, err)
	assert.Equal(t, int64(23456), r.BlockNumber.Int64())

}

func TestGetTransactionReceiptNotFound(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*receipts").WillReturnRows(sqlmock.NewRows([]string{"seq"}))

	r, err := p.GetTransactionReceipt(ctx, "tx1")
	assert.NoError(t, err)
	assert.Nil(t, r)

	assert.NoError(t, mdb.ExpectationsWereMet())
}
