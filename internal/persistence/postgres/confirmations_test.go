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

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTransactionConfirmationsOrderPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterCount, 1)            // send them all to one worker
		dbconf.Set(ConfigTXWriterBatchTimeout, "10s") // ensure it triggers with the whole thing
		dbconf.Set(ConfigTXWriterBatchSize, 16)       // this must exactly match the number of ops
	})
	defer done()

	// Write initial transactions
	tx1ID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
	tx1 := &apitypes.ManagedTX{ID: tx1ID, Status: apitypes.TxStatusPending, TransactionHeaders: ffcapi.TransactionHeaders{From: "0x122345"}}
	tx2ID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
	tx2 := &apitypes.ManagedTX{ID: tx2ID, Status: apitypes.TxStatusPending, TransactionHeaders: ffcapi.TransactionHeaders{From: "0x122345"}}
	// deliberately do not wait for the inserts (there is only one worker, so they will go there too)
	insertOp := newTransactionOperation(tx1ID)
	insertOp.txInsert = tx1
	insertOp.nextNonceCB = func(ctx context.Context, signer string) (uint64, error) { return 0, nil }
	p.writer.queue(ctx, insertOp)
	insertOp = newTransactionOperation(tx2ID)
	insertOp.txInsert = tx2
	insertOp.nextNonceCB = func(ctx context.Context, signer string) (uint64, error) { return 0, nil }
	p.writer.queue(ctx, insertOp)

	// A few confirmations for tx1 - we'll zap these later with a fork
	var confirmations []*apitypes.Confirmation
	for i := 0; i < 5; i++ {
		confirmations = append(confirmations, &apitypes.Confirmation{
			BlockNumber: fftypes.FFuint64(i),
			BlockHash:   fmt.Sprintf("0x11%.3d", i),
			ParentHash:  fmt.Sprintf("0x22%.3d", i),
		})
	}
	err := p.AddTransactionConfirmations(ctx, tx1ID, true, confirmations...)
	assert.NoError(t, err)

	// A few confirmations for tx2 - these should stay
	confirmations = confirmations[:0]
	for i := 0; i < 2; i++ {
		confirmations = append(confirmations, &apitypes.Confirmation{
			BlockNumber: fftypes.FFuint64(i),
			BlockHash:   fmt.Sprintf("0x11%.3d", i),
			ParentHash:  fmt.Sprintf("0x22%.3d", i),
		})
	}
	err = p.AddTransactionConfirmations(ctx, tx2ID, true, confirmations...)
	assert.NoError(t, err)

	// Replace with a fork
	confirmations = confirmations[:0]
	for i := 0; i < 3; i++ {
		confirmations = append(confirmations, &apitypes.Confirmation{
			BlockNumber: fftypes.FFuint64(i),
			BlockHash:   fmt.Sprintf("0x33%.3d", i),
			ParentHash:  fmt.Sprintf("0x44%.3d", i),
		})
	}
	err = p.AddTransactionConfirmations(ctx, tx1ID, true, confirmations...)
	assert.NoError(t, err)

	// Add more to that fork
	confirmations = confirmations[:0]
	for i := 3; i < 6; i++ {
		confirmations = append(confirmations, &apitypes.Confirmation{
			BlockNumber: fftypes.FFuint64(i),
			BlockHash:   fmt.Sprintf("0x55%.3d", i),
			ParentHash:  fmt.Sprintf("0x66%.3d", i),
		})
	}
	err = p.AddTransactionConfirmations(ctx, tx1ID, false, confirmations...)
	assert.NoError(t, err)

	// Flush with an update
	newStatus := apitypes.TxStatusSucceeded
	err = p.UpdateTransaction(ctx, tx1ID, &apitypes.TXUpdates{Status: &newStatus})
	assert.NoError(t, err)

	// Check them for tx1 with the fork re-write
	confirmations, err = p.GetTransactionConfirmations(ctx, tx1ID)
	assert.NoError(t, err)
	assert.Equal(t, []*apitypes.Confirmation{
		{
			BlockNumber: 0,
			BlockHash:   "0x33000",
			ParentHash:  "0x44000",
		},
		{
			BlockNumber: 1,
			BlockHash:   "0x33001",
			ParentHash:  "0x44001",
		},
		{
			BlockNumber: 2,
			BlockHash:   "0x33002",
			ParentHash:  "0x44002",
		},
		{
			BlockNumber: 3,
			BlockHash:   "0x55003",
			ParentHash:  "0x66003",
		},
		{
			BlockNumber: 4,
			BlockHash:   "0x55004",
			ParentHash:  "0x66004",
		},
		{
			BlockNumber: 5,
			BlockHash:   "0x55005",
			ParentHash:  "0x66005",
		},
	}, confirmations)

	// Check them for tx2 that we didn't break those
	confirmations, err = p.GetTransactionConfirmations(ctx, tx2ID)
	assert.NoError(t, err)
	assert.Equal(t, []*apitypes.Confirmation{
		{
			BlockNumber: 0,
			BlockHash:   "0x11000",
			ParentHash:  "0x22000",
		},
		{
			BlockNumber: 1,
			BlockHash:   "0x11001",
			ParentHash:  "0x22001",
		},
	}, confirmations)

	// Filter just one
	fb := p.NewConfirmationFilter(ctx)
	crs, _, err := p.ListTransactionConfirmations(ctx, tx2ID, fb.And(fb.Eq("blocknumber", 1)))
	assert.Len(t, crs, 1)
	assert.Equal(t, crs[0].BlockNumber.Uint64(), uint64(1))

}

func TestGetTransactionConfirmationsFailQuery(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*confirmations").WillReturnError(fmt.Errorf("pop"))

	_, err := p.GetTransactionConfirmations(ctx, "tx1")
	assert.Regexp(t, "FF00176", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}
