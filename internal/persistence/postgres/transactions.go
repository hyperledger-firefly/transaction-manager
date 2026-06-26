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

package postgres

import (
	"context"
	"strconv"

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

func (p *sqlPersistence) newTransactionCollection(forMigration bool) *dbsql.CrudBase[*apitypes.ManagedTX] {
	collection := &dbsql.CrudBase[*apitypes.ManagedTX]{
		DB:    p.db,
		Table: "transactions",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"status",
			"delete",
			"tx_from",
			"tx_to",
			"tx_nonce",
			"tx_gas",
			"tx_value",
			"tx_gasprice",
			"tx_data",
			"tx_hash",
			"policy_info",
			"first_submit",
			"last_submit",
			"error_message",
		},
		FilterFieldMap: map[string]string{
			"sequence":        p.db.SequenceColumn(),
			"transactiondata": "tx_data",
			"transactionhash": "tx_hash",
			"deleterequested": "delete",
			"from":            "tx_from",
			"to":              "tx_to",
			"nonce":           "tx_nonce",
			"gas":             "tx_gas",
			"value":           "tx_value",
			"gasprice":        "tx_gasprice",
			"policyinfo":      "policy_info",
			"firstsubmit":     "first_submit",
			"lastsubmit":      "last_submit",
			"errormessage":    "error_message",
		},
		PatchDisabled: true,
		TimesDisabled: forMigration,
		NilValue:      func() *apitypes.ManagedTX { return nil },
		NewInstance:   func() *apitypes.ManagedTX { return &apitypes.ManagedTX{} },
		GetFieldPtr: func(inst *apitypes.ManagedTX, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "status":
				return &inst.Status
			case "delete":
				return &inst.DeleteRequested
			case "tx_from":
				return &inst.From
			case "tx_to":
				return &inst.To
			case "tx_nonce":
				return &inst.Nonce
			case "tx_gas":
				return &inst.Gas
			case "tx_value":
				return &inst.Value
			case "tx_gasprice":
				return &inst.GasPrice
			case "tx_data":
				return &inst.TransactionData
			case "tx_hash":
				return &inst.TransactionHash
			case "policy_info":
				return &inst.PolicyInfo
			case "first_submit":
				return &inst.FirstSubmit
			case "last_submit":
				return &inst.LastSubmit
			case "error_message":
				return &inst.ErrorMessage
			}
			return nil
		},
	}
	collection.Validate()
	return collection
}

func (p *sqlPersistence) NewTransactionFilter(ctx context.Context) ffapi.FilterBuilder {
	return persistence.TransactionFilters.NewFilter(ctx)
}

func (p *sqlPersistence) ListTransactions(ctx context.Context, filter ffapi.AndFilter) ([]*apitypes.ManagedTX, *ffapi.FilterResult, error) {
	return p.transactions.GetMany(ctx, filter)
}

func (p *sqlPersistence) ListTransactionsByCreateTime(ctx context.Context, after *apitypes.ManagedTX, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {
	var afterSeq *int64
	if after != nil {
		seq, err := strconv.ParseInt(after.SequenceID, 10, 64)
		if err != nil {
			// This shouldn't happen - we always calculate and store this from the persisted DB sequence
			return nil, err
		}
		afterSeq = &seq
	}
	filter := p.seqAfterFilter(ctx, persistence.TransactionFilters, afterSeq, limit, dir)
	transactions, _, err := p.transactions.GetMany(ctx, filter)
	return transactions, err

}

func (p *sqlPersistence) ListTransactionsByNonce(ctx context.Context, signer string, after *fftypes.FFBigInt, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {
	//nolint:gosec // Safe conversion as limit is always positive
	fb := persistence.TransactionFilters.NewFilterLimit(ctx, uint64(limit))
	conditions := []ffapi.Filter{
		fb.Eq("from", signer),
	}
	if after != nil {
		if dir == txhandler.SortDirectionDescending {
			conditions = append(conditions, fb.Lt("nonce", after))
		} else {
			conditions = append(conditions, fb.Gt("nonce", after))
		}
	}
	var filter ffapi.Filter = fb.And(conditions...)
	if dir == txhandler.SortDirectionDescending {
		filter = filter.Sort("-nonce")
	} else {
		filter = filter.Sort("nonce")
	}
	transactions, _, err := p.transactions.GetMany(ctx, filter)
	return transactions, err
}

func (p *sqlPersistence) ListTransactionsPending(ctx context.Context, afterSequenceID string, limit int, dir txhandler.SortDirection) ([]*apitypes.ManagedTX, error) {
	var afterSeq *int64
	if afterSequenceID != "" {
		seq, err := strconv.ParseInt(afterSequenceID, 10, 64)
		if err != nil {
			// This shouldn't happen - we always calculate and store this from the persisted DB sequence
			return nil, err
		}
		afterSeq = &seq
	}
	filter := p.seqAfterFilter(ctx, persistence.TransactionFilters, afterSeq, limit, dir,
		persistence.TransactionFilters.NewFilter(ctx).Eq("status", apitypes.TxStatusPending))
	transactions, _, err := p.transactions.GetMany(ctx, filter)
	return transactions, err
}

func (p *sqlPersistence) GetTransactionByID(ctx context.Context, txID string) (*apitypes.ManagedTX, error) {
	return p.transactions.GetByID(ctx, txID)
}

func (p *sqlPersistence) GetTransactionByIDWithStatus(ctx context.Context, txID string, withHistory bool) (*apitypes.TXWithStatus, error) {
	tx, err := p.transactions.GetByID(ctx, txID)
	if tx == nil || err != nil {
		return nil, err
	}
	receiptRecord, err := p.receipts.GetByID(ctx, txID)
	if err != nil {
		return nil, err
	}
	var receipt *ffcapi.TransactionReceiptResponse
	if receiptRecord != nil {
		receipt = receiptRecord.TransactionReceiptResponse
	}
	confirmations, err := p.GetTransactionConfirmations(ctx, txID)
	if err != nil {
		return nil, err
	}
	txh := &apitypes.TXWithStatus{
		ManagedTX:     tx,
		Receipt:       receipt,
		Confirmations: confirmations,
	}
	if withHistory {
		history, err := p.buildHistorySummary(ctx, txID, true, p.historySummaryLimit, nil)
		if err != nil {
			return nil, err
		}
		txh.History = history.entries
	}
	return txh, nil
}

func (p *sqlPersistence) GetTransactionByNonce(ctx context.Context, signer string, nonce *fftypes.FFBigInt) (*apitypes.ManagedTX, error) {
	fb := persistence.TransactionFilters.NewFilterLimit(ctx, 1)
	filter := fb.And(
		fb.Eq("from", signer),
		fb.Eq("nonce", nonce),
	)
	transactions, _, err := p.transactions.GetMany(ctx, filter)
	if len(transactions) == 0 || err != nil {
		return nil, err
	}
	return transactions[0], err
}

func (p *sqlPersistence) InsertTransactionPreAssignedNonce(ctx context.Context, tx *apitypes.ManagedTX) error {
	// Dispatch to TX writer
	op := newTransactionOperation(tx.ID)
	op.txInsert = tx
	op.noncePreAssigned = true
	p.writer.queue(ctx, op)
	return op.flush(ctx) // wait for completion
}

func (p *sqlPersistence) InsertTransactionWithNextNonce(ctx context.Context, tx *apitypes.ManagedTX, nextNonceCB txhandler.NextNonceCallback) error {
	// Dispatch to TX writer
	op := newTransactionOperation(tx.ID)
	op.txInsert = tx
	op.nextNonceCB = nextNonceCB
	p.writer.queue(ctx, op)
	return op.flush(ctx) // wait for completion
}

func (p *sqlPersistence) InsertTransactionsWithNextNonce(ctx context.Context, txs []*apitypes.ManagedTX, nextNonceCB txhandler.NextNonceCallback) []error {
	if len(txs) == 0 {
		return nil
	}
	errs := make([]error, len(txs))

	// Create operations for all transactions, marking invalid ones immediately
	ops := make([]*transactionOperation, len(txs))
	for i, tx := range txs {
		if tx.From == "" {
			errs[i] = i18n.NewError(ctx, tmmsgs.MsgTransactionOpInvalid)
			ops[i] = nil // Mark as invalid
			continue
		}
		op := newTransactionOperation(tx.ID)
		op.txInsert = tx
		op.nextNonceCB = nextNonceCB
		ops[i] = op
	}

	// Queue all valid operations - they will be processed in batches by the writer
	// Note: Transactions with different signers will go to different workers,
	// so they won't be in the same DB transaction. However, transactions with
	// the same signer will be batched together efficiently.
	for _, op := range ops {
		if op != nil {
			p.writer.queue(ctx, op)
		}
	}

	// Wait for all valid operations to complete and collect individual errors
	for i, op := range ops {
		if op != nil {
			errs[i] = op.flush(ctx) // Wait for completion, capture individual errors
		}
		// Invalid operations already have their errors set above
	}
	return errs
}

func (p *sqlPersistence) UpdateTransaction(ctx context.Context, txID string, updates *apitypes.TXUpdates) error {
	// Dispatch to TX writer
	op := newTransactionOperation(txID)
	op.txUpdate = updates
	p.writer.queue(ctx, op)
	return op.flush(ctx) // wait for completion
}

func (p *sqlPersistence) DeleteTransaction(ctx context.Context, txID string) error {
	// Dispatch to TX writer, as we need to sequence it against updates and delete all associated records
	op := newTransactionOperation(txID)
	op.txDelete = &txID
	p.writer.queue(ctx, op)
	return op.flush(ctx) // wait for completion
}

func (p *sqlPersistence) updateTransaction(ctx context.Context, txID string, updates *apitypes.TXUpdates) error {
	sqlUpdate := persistence.TransactionFilters.NewUpdate(ctx).S()
	if updates.Status != nil {
		sqlUpdate = sqlUpdate.Set("status", *updates.Status)
	}
	if updates.DeleteRequested != nil {
		sqlUpdate = sqlUpdate.Set("deleterequested", updates.DeleteRequested)
	}
	if updates.From != nil {
		sqlUpdate = sqlUpdate.Set("from", *updates.From)
	}
	if updates.To != nil {
		sqlUpdate = sqlUpdate.Set("to", *updates.To)
	}
	if updates.Nonce != nil {
		sqlUpdate = sqlUpdate.Set("nonce", updates.Nonce)
	}
	if updates.Gas != nil {
		sqlUpdate = sqlUpdate.Set("gas", updates.Gas)
	}
	if updates.Value != nil {
		sqlUpdate = sqlUpdate.Set("value", updates.Value)
	}
	if updates.GasPrice != nil {
		sqlUpdate = sqlUpdate.Set("gasprice", updates.GasPrice)
	}
	if updates.TransactionData != nil {
		sqlUpdate = sqlUpdate.Set("transactiondata", *updates.TransactionData)
	}
	if updates.TransactionHash != nil {
		sqlUpdate = sqlUpdate.Set("transactionhash", *updates.TransactionHash)
	}
	if updates.PolicyInfo != nil {
		sqlUpdate = sqlUpdate.Set("policyinfo", updates.PolicyInfo)
	}
	if updates.FirstSubmit != nil {
		sqlUpdate = sqlUpdate.Set("firstsubmit", updates.FirstSubmit)
	}
	if updates.LastSubmit != nil {
		sqlUpdate = sqlUpdate.Set("lastsubmit", updates.LastSubmit)
	}
	if updates.ErrorMessage != nil {
		sqlUpdate = sqlUpdate.Set("errormessage", *updates.ErrorMessage)
	}
	return p.transactions.Update(ctx, txID, sqlUpdate)
}
