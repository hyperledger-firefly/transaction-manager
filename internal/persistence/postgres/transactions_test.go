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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionBasicValidationPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// Write an initial transaction
	txID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
	tx := &apitypes.ManagedTX{
		ID:              txID,
		Status:          apitypes.TxStatusPending,
		DeleteRequested: nil,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x111111",
			To:    "0x222222",
			Gas:   fftypes.NewFFBigInt(555555),
			Value: fftypes.NewFFBigInt(666666),
		},
		GasPrice:        fftypes.JSONAnyPtr(`{"gas":"777777"}`),
		TransactionData: "0x999999",
		TransactionHash: "0xaaaaaa",
		PolicyInfo:      fftypes.JSONAnyPtr(`{"policy":"888888"}`),
		FirstSubmit:     fftypes.Now(),
		LastSubmit:      fftypes.Now(),
		ErrorMessage:    "error bbbbbb",
	}
	err := p.InsertTransactionWithNextNonce(ctx, tx, func(ctx context.Context, signer string) (uint64, error) {
		return 333333, nil
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, tx.SequenceID)
	origTXJson, err := json.Marshal(tx)
	assert.NoError(t, err)

	// Read it back immediately (testing the flush)
	tx1, err := p.GetTransactionByID(ctx, txID)
	assert.NoError(t, err)
	insertedTXJson, err := json.Marshal(tx1)
	assert.NoError(t, err)
	assert.JSONEq(t, string(origTXJson), string(insertedTXJson))

	// Queue up a bunch of items, which will flush with the transaction update at the end ...

	// A receipt
	receipt := &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			BlockNumber:      fftypes.NewFFBigInt(111111),
			TransactionIndex: fftypes.NewFFBigInt(222222),
			BlockHash:        "0x333333",
			Success:          true,
			ProtocolID:       "000/111/222",
			ExtraInfo:        fftypes.JSONAnyPtr(`{"extra":"444444"}`),
			ContractLocation: fftypes.JSONAnyPtr(`{"address":"0x555555"}`),
		},
	}
	err = p.SetTransactionReceipt(ctx, txID, receipt)
	assert.NoError(t, err)

	// A few confirmations
	confirmations := make([]*apitypes.Confirmation, 5)
	for i := 0; i < len(confirmations); i++ {
		confirmations[i] = &apitypes.Confirmation{
			BlockNumber: fftypes.FFuint64(i),
			BlockHash:   fmt.Sprintf("0x1%.3d", i),
			ParentHash:  fmt.Sprintf("0x2%.3d", i),
		}
	}
	err = p.AddTransactionConfirmations(ctx, txID, true, confirmations...)
	assert.NoError(t, err)

	// A couple of transaction history entries
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, fftypes.JSONAnyPtr(`{"nonce":"11111"}`), nil, fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, nil, fftypes.JSONAnyPtr(`"failed to submit 1"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusReceived, apitypes.TxActionSubmitTransaction, nil, fftypes.JSONAnyPtr(`"failed to submit 2"`), fftypes.Now())
	assert.NoError(t, err)
	err = p.AddSubStatusAction(ctx, txID, apitypes.TxSubStatusTracking, apitypes.TxActionSubmitTransaction, fftypes.JSONAnyPtr(`{"txhash":"0x12345"}`), nil, fftypes.Now())
	assert.NoError(t, err)

	// Finally the update - do a comprehensive one
	newStatus := apitypes.TxStatusFailed
	txUpdates := &apitypes.TXUpdates{
		Status:          &newStatus,
		DeleteRequested: fftypes.Now(),
		From:            strPtr("0xfff111111"),
		To:              strPtr("0xfff222222"),
		Nonce:           fftypes.NewFFBigInt(999333333),
		Gas:             fftypes.NewFFBigInt(999555555),
		Value:           fftypes.NewFFBigInt(999666666),
		GasPrice:        fftypes.JSONAnyPtr(`{"gas":"777777"}`),
		TransactionData: strPtr("0x999999"),
		TransactionHash: strPtr("0xaaaaaa"),
		PolicyInfo:      fftypes.JSONAnyPtr(`{"policy":"888888"}`),
		FirstSubmit:     fftypes.Now(),
		LastSubmit:      fftypes.Now(),
		ErrorMessage:    strPtr("error bbbbbb"),
	}
	err = p.UpdateTransaction(ctx, txID, txUpdates)
	assert.NoError(t, err)

	// Get back the merged object to check everything
	mtx, err := p.GetTransactionByIDWithStatus(ctx, txID, true)
	assert.NoError(t, err)
	assert.NotEqual(t, tx.Updated.String(), mtx.Updated.String())

	mtxExpected := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{
			ID:              txID,
			SequenceID:      tx.SequenceID, // will not have changed
			Created:         tx.Created,    // will not have changed
			Updated:         mtx.Updated,   // will have changed
			Status:          *txUpdates.Status,
			DeleteRequested: txUpdates.DeleteRequested,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  *txUpdates.From,
				To:    *txUpdates.To,
				Nonce: txUpdates.Nonce,
				Gas:   txUpdates.Gas,
				Value: txUpdates.Value,
			},
			GasPrice:        txUpdates.GasPrice,
			TransactionData: *txUpdates.TransactionData,
			TransactionHash: *txUpdates.TransactionHash,
			PolicyInfo:      txUpdates.PolicyInfo,
			FirstSubmit:     txUpdates.FirstSubmit,
			LastSubmit:      txUpdates.LastSubmit,
			ErrorMessage:    *txUpdates.ErrorMessage,
		},
		Receipt:       receipt,
		Confirmations: confirmations,
		History: []*apitypes.TxHistoryStateTransitionEntry{
			{
				Status: apitypes.TxSubStatusTracking,
				Time:   mtx.History[0].Time,
				Actions: []*apitypes.TxHistoryActionEntry{ // newest first
					{
						Action:          apitypes.TxActionSubmitTransaction,
						Time:            mtx.History[0].Actions[0].Time,
						LastOccurrence:  mtx.History[0].Actions[0].LastOccurrence,
						OccurrenceCount: 1,
						LastInfo:        fftypes.JSONAnyPtr(`{"txhash":"0x12345"}`),
						LastError:       nil,
						LastErrorTime:   nil,
					},
				},
			},
			{
				Status: apitypes.TxSubStatusReceived,
				Time:   mtx.History[1].Time,
				Actions: []*apitypes.TxHistoryActionEntry{ // newest first
					{
						Action:          apitypes.TxActionSubmitTransaction,
						Time:            mtx.History[1].Actions[0].Time,
						LastOccurrence:  mtx.History[1].Actions[0].LastOccurrence,
						OccurrenceCount: 2,
						LastInfo:        nil,
						LastError:       fftypes.JSONAnyPtr(`"failed to submit 2"`),
						LastErrorTime:   mtx.History[1].Actions[0].LastErrorTime,
					},
					{
						Action:          apitypes.TxActionAssignNonce,
						Time:            mtx.History[1].Actions[1].Time,
						LastOccurrence:  mtx.History[1].Actions[1].LastOccurrence,
						OccurrenceCount: 1,
						LastInfo:        fftypes.JSONAnyPtr(`{"nonce":"11111"}`),
						LastError:       nil,
						LastErrorTime:   nil,
					},
				},
			},
		},
	}
	expectedMTXJson, err := json.Marshal(mtxExpected)
	assert.NoError(t, err)
	actualMTXJson, err := json.Marshal(mtx)
	assert.NoError(t, err)
	assert.JSONEq(t, string(expectedMTXJson), string(actualMTXJson))

	// Check we can get the completion
	completions, err := p.ListTxCompletionsByCreateTime(ctx, nil, 25, txhandler.SortDirectionAscending)
	require.NoError(t, err)
	require.Len(t, completions, 1)
	require.Equal(t, txID, completions[0].ID)
	require.Equal(t, apitypes.TxStatusFailed, completions[0].Status)
	txCompleteSeq := *completions[0].Sequence

	// Check we can skip past it
	completions, err = p.ListTxCompletionsByCreateTime(ctx, &txCompleteSeq, 25, txhandler.SortDirectionAscending)
	require.NoError(t, err)
	require.Empty(t, completions)
	completions, err = p.ListTxCompletionsByCreateTime(ctx, &txCompleteSeq, 25, txhandler.SortDirectionDescending)
	require.NoError(t, err)
	require.Empty(t, completions)

	// Finally clean up
	err = p.DeleteTransaction(ctx, txID)
	assert.NoError(t, err)

	// Check all is gone
	count, err := p.receipts.Count(ctx, persistence.ReceiptFilters.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Zero(t, count)
	count, err = p.confirmations.Count(ctx, persistence.ConfirmationFilters.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Zero(t, count)
	count, err = p.txHistory.Count(ctx, persistence.TXHistoryFilters.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Zero(t, count)
	count, err = p.transactions.Count(ctx, persistence.TransactionFilters.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Zero(t, count)

}

func TestTransactionListByCreateTimePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// Write transactions with increasing nonces across two signers
	var txs []*apitypes.ManagedTX
	for i := int64(0); i < 10; i++ {
		signer := fmt.Sprintf("signer_%d", i%2) // alternate between two signers
		txID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
		tx := &apitypes.ManagedTX{
			ID:              txID,
			Status:          apitypes.TxStatusPending,
			DeleteRequested: nil,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  signer,
				Nonce: fftypes.NewFFBigInt(i / 2),
			},
		}
		err := p.InsertTransactionPreAssignedNonce(ctx, tx)
		assert.NoError(t, err)
		assert.NotEmpty(t, tx.SequenceID)
		txs = append(txs, tx)
	}

	fb := p.NewTransactionFilter(ctx)

	// List all the transactions - default is created time descending on the standard filter
	list1, _, err := p.ListTransactions(ctx, fb.And())
	assert.NoError(t, err)
	assert.Len(t, list1, 10)
	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("signer_%d", (9-i)%2), list1[i].From)
		assert.Equal(t, int64((9-i)/2), list1[i].Nonce.Int64())
	}

	// Use the function to limit this list to three ascending, after the 2nd
	list2, err := p.ListTransactionsByCreateTime(ctx, txs[1], 3, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list2, 3)
	for i := 0; i < 3; i++ {
		assert.Equal(t, fmt.Sprintf("signer_%d", (i+2)%2), list2[i].From)
		assert.Equal(t, int64((i+2)/2), list2[i].Nonce.Int64())
	}

	// bad sequence string
	_, err = p.ListTransactionsByCreateTime(ctx, &apitypes.ManagedTX{SequenceID: "!wrong"}, 0, txhandler.SortDirectionAscending)
	assert.Error(t, err)

}

func TestTransactionListByNoncePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// Write transactions with increasing nonces across two signers
	var txs []*apitypes.ManagedTX
	for i := int64(0); i < 50; i++ {
		signer := fmt.Sprintf("signer_%d", i%2) // alternate between two signers
		txID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
		tx := &apitypes.ManagedTX{
			ID:              txID,
			Status:          apitypes.TxStatusPending,
			DeleteRequested: nil,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  signer,
				Nonce: fftypes.NewFFBigInt(i / 2),
			},
		}
		err := p.InsertTransactionWithNextNonce(ctx, tx, func(ctx context.Context, signer string) (uint64, error) {
			return 0, nil
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, tx.SequenceID)
		txs = append(txs, tx)
	}

	// List all the transactions by nonce for the first signer ascending, these we promise order on
	list2, err := p.ListTransactionsByNonce(ctx, "signer_0", nil, 0, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list2, 25)
	for i := 0; i < 25; i++ {
		assert.Equal(t, "signer_0", list2[i].From)
		assert.Equal(t, int64(i), list2[i].Nonce.Int64())
	}

	// List all the transactions by nonce for the second signer descending, before 5
	list3, err := p.ListTransactionsByNonce(ctx, "signer_1", fftypes.NewFFBigInt(5), 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, list3, 5)
	for i := 0; i < 4; i++ {
		assert.Equal(t, "signer_1", list3[i].From)
		assert.Equal(t, int64(4-i), list3[i].Nonce.Int64())
	}

	// List just two nonces after 3 for signer_0
	list4, err := p.ListTransactionsByNonce(ctx, "signer_0", fftypes.NewFFBigInt(3), 2, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list4, 2)
	assert.Equal(t, "signer_0", list4[0].From)
	assert.Equal(t, int64(4), list4[0].Nonce.Int64())
	assert.Equal(t, "signer_0", list4[1].From)
	assert.Equal(t, int64(5), list4[1].Nonce.Int64())

	// Get individual by nonce, ok
	tx, err := p.GetTransactionByNonce(ctx, "signer_0", txs[2].Nonce)
	assert.NoError(t, err)
	assert.Equal(t, txs[2].Nonce, tx.Nonce)

	// Get individual by nonce, not found
	tx, err = p.GetTransactionByNonce(ctx, "signer_9999", txs[2].Nonce)
	assert.NoError(t, err)
	assert.Nil(t, tx)

}

func TestTransactionPendingPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// Write transactions with increasing nonces across two signers
	var txs []*apitypes.ManagedTX
	for i := int64(0); i < 10; i++ {
		signer := "signer_0" // all pending from one signer
		status := apitypes.TxStatusPending
		if i%2 == 0 {
			status = apitypes.TxStatusSucceeded
			signer = "signer_1"
		}
		txID := fmt.Sprintf("ns1:%s", fftypes.NewUUID())
		tx := &apitypes.ManagedTX{
			ID:              txID,
			Status:          status,
			DeleteRequested: nil,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  signer,
				Nonce: fftypes.NewFFBigInt(i / 2),
			},
		}
		err := p.InsertTransactionWithNextNonce(ctx, tx, func(ctx context.Context, signer string) (uint64, error) {
			return 0, nil
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, tx.SequenceID)
		txs = append(txs, tx)
	}

	// List all the pending TX
	list1, err := p.ListTransactionsPending(ctx, "", 0, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list1, 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, apitypes.TxStatusPending, list1[i].Status)
		assert.Equal(t, int64(i), list1[i].Nonce.Int64())
	}

	// List before given sequence with a limit
	list2, err := p.ListTransactionsPending(ctx, txs[7].SequenceID, 2, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, list2, 2)
	assert.Equal(t, apitypes.TxStatusPending, list2[0].Status)
	assert.Equal(t, int64(2), list2[0].Nonce.Int64())
	assert.Equal(t, apitypes.TxStatusPending, list2[1].Status)
	assert.Equal(t, int64(1), list2[1].Nonce.Int64())

	// bad sequence string
	_, err = p.ListTransactionsPending(ctx, "!wrong", 0, txhandler.SortDirectionAscending)
	assert.Error(t, err)

}

func TestTransactionMixConflictAndOkPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterCount, 1)            // send them all to one worker
		dbconf.Set(ConfigTXWriterBatchTimeout, "10s") // ensure it triggers with the whole thing
		dbconf.Set(ConfigTXWriterBatchSize, 2)        // this must exactly match the number of ops
	})
	defer done()

	nonce := uint64(1000)
	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		nonce++
		return nonce, nil
	}
	newTX := func(i int64) *apitypes.ManagedTX {
		return &apitypes.ManagedTX{
			ID:              fmt.Sprintf("ns1:%.9d", i),
			Status:          apitypes.TxStatusPending,
			DeleteRequested: nil,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  "signer_0",
				Nonce: fftypes.NewFFBigInt(i),
			},
		}
	}
	mtx1 := newTX(1)
	mtx2 := newTX(2)
	mtx3 := newTX(3)

	// Do a successful insert of TX1 and TX2 in the first batch
	op1 := newTransactionOperation(mtx1.ID)
	op1.txInsert = mtx1
	op1.nextNonceCB = nextNonceCB
	p.writer.queue(ctx, op1)
	op2 := newTransactionOperation(mtx2.ID)
	op2.txInsert = mtx2
	p.writer.queue(ctx, op2)
	assert.NoError(t, <-op1.done)
	assert.NoError(t, <-op2.done)

	// Now try and do mtx2 again - which should 409, and mtx3 - which should succeed
	op3 := newTransactionOperation(mtx2.ID)
	op3.txInsert = mtx2
	p.writer.queue(ctx, op3)
	op4 := newTransactionOperation(mtx3.ID)
	op4.txInsert = mtx3
	p.writer.queue(ctx, op4)
	assert.Regexp(t, "FF21065", <-op3.done)
	assert.NoError(t, <-op4.done)

}

func newTXRow(p *sqlPersistence) *sqlmock.Rows {
	return sqlmock.NewRows(append([]string{p.db.SequenceColumn()}, p.transactions.Columns...)).AddRow(
		12345,                  // seq
		"tx1",                  // dbsql.ColumnID,
		fftypes.Now().String(), // dbsql.ColumnCreated,
		fftypes.Now().String(), // dbsql.ColumnUpdated,
		"Pending",              // "status",
		fftypes.Now().String(), // "delete",
		"0x12345",              // "tx_from",
		"0x23456",              // "tx_to",
		"11111",                // "tx_nonce", (hex)
		"22222",                // "tx_gas", (hex)
		"33333",                // "tx_value", (hex)
		"44444",                // "tx_gasprice", (hex)
		"0xffffff",             // "tx_data",
		"0xeeeeee",             // "tx_hash",
		nil,                    // "policy_info",
		nil,                    // "first_submit",
		nil,                    // "last_submit",
		"",                     // "error_message",
	)
}

func TestGetTransactionByIDWithStatusHistorySummaryFail(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*transactions").WillReturnRows(newTXRow(p))
	mdb.ExpectQuery("SELECT.*receipts").WillReturnRows(
		sqlmock.NewRows(append([]string{p.db.SequenceColumn()}, p.receipts.Columns...)),
	)
	mdb.ExpectQuery("SELECT.*confirmations").WillReturnRows(
		sqlmock.NewRows(append([]string{p.db.SequenceColumn()}, p.confirmations.Columns...)),
	)
	mdb.ExpectQuery("SELECT.*txhistory").WillReturnError(fmt.Errorf("pop"))

	_, err := p.GetTransactionByIDWithStatus(ctx, "tx1", true)
	assert.Regexp(t, "FF00176", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestGetTransactionByIDWithStatusConfirmationsFail(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*transactions").WillReturnRows(newTXRow(p))
	mdb.ExpectQuery("SELECT.*receipts").WillReturnRows(
		sqlmock.NewRows(append([]string{p.db.SequenceColumn()}, p.receipts.Columns...)),
	)
	mdb.ExpectQuery("SELECT.*confirmations").WillReturnError(fmt.Errorf("pop"))

	_, err := p.GetTransactionByIDWithStatus(ctx, "tx1", true)
	assert.Regexp(t, "FF00176", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestGetTransactionByIDWithStatusReceiptFail(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*transactions").WillReturnRows(newTXRow(p))
	mdb.ExpectQuery("SELECT.*receipts").WillReturnError(fmt.Errorf("pop"))

	_, err := p.GetTransactionByIDWithStatus(ctx, "tx1", true)
	assert.Regexp(t, "FF00176", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestGetTransactionByIDTXFail(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*transactions").WillReturnError(fmt.Errorf("pop"))

	_, err := p.GetTransactionByIDWithStatus(ctx, "tx1", true)
	assert.Regexp(t, "FF00176", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestInsertTransactionsWithNextNoncePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t, func(dbconf config.Section) {
		dbconf.Set(ConfigTXWriterCount, 1)            // send them all to one worker
		dbconf.Set(ConfigTXWriterBatchTimeout, "10s") // ensure it triggers with the whole thing
		dbconf.Set(ConfigTXWriterBatchSize, 10)       // large enough to batch all
	})
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
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
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

	for i, tx := range txs {
		retrieved, err := p.GetTransactionByID(ctx, tx.ID)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved, "Transaction %d should be retrievable", i)
		assert.Equal(t, tx.ID, retrieved.ID)
		assert.Equal(t, tx.Nonce, retrieved.Nonce)
	}
}

func TestInsertTransactionsWithNextNonceMixedSignersPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
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
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_1",
			},
		},
		{
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_2",
			},
		},
		{
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
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

func TestInsertTransactionsWithNextNonceInvalidPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// test a batch with an invalid transaction (missing From)
	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		return 1000, nil
	}

	txs := []*apitypes.ManagedTX{
		{
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "signer_0",
			},
		},
		{
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "", // Invalid - missing From
			},
		},
		{
			ID:     fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
			Status: apitypes.TxStatusPending,
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

func TestInsertTransactionsWithNextNonceEmptyPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	nextNonceCB := func(ctx context.Context, signer string) (uint64, error) {
		return 1000, nil
	}

	// test an empty batch
	errs := p.InsertTransactionsWithNextNonce(ctx, []*apitypes.ManagedTX{}, nextNonceCB)
	assert.Nil(t, errs)
}
