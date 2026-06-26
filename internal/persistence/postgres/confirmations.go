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

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func (p *sqlPersistence) newConfirmationsCollection() *dbsql.CrudBase[*apitypes.ConfirmationRecord] {
	collection := &dbsql.CrudBase[*apitypes.ConfirmationRecord]{
		DB:    p.db,
		Table: "confirmations",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"tx_id",
			"block_number",
			"block_hash",
			"parent_hash",
		},
		FilterFieldMap: map[string]string{
			"sequence":    p.db.SequenceColumn(),
			"transaction": "tx_id",
			"blocknumber": "block_number",
			"blockhash":   "block_hash",
			"parenthash":  "parent_hash",
		},
		PatchDisabled: true,
		NilValue:      func() *apitypes.ConfirmationRecord { return nil },
		NewInstance: func() *apitypes.ConfirmationRecord {
			return &apitypes.ConfirmationRecord{
				Confirmation: &apitypes.Confirmation{},
			}
		},
		GetFieldPtr: func(inst *apitypes.ConfirmationRecord, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "tx_id":
				return &inst.TransactionID
			case "block_number":
				return &inst.BlockNumber
			case "block_hash":
				return &inst.BlockHash
			case "parent_hash":
				return &inst.ParentHash
			}
			return nil
		},
	}
	collection.Validate()
	return collection
}

func (p *sqlPersistence) NewConfirmationFilter(ctx context.Context) ffapi.FilterBuilder {
	return persistence.ConfirmationFilters.NewFilter(ctx)
}

func (p *sqlPersistence) ListTransactionConfirmations(ctx context.Context, txID string, filter ffapi.AndFilter) ([]*apitypes.ConfirmationRecord, *ffapi.FilterResult, error) {
	return p.confirmations.GetMany(ctx, filter.Condition(filter.Builder().Eq("transaction", txID)))
}

func (p *sqlPersistence) GetTransactionConfirmations(ctx context.Context, txID string) ([]*apitypes.Confirmation, error) {
	// We query in increasing insertion order
	filter := persistence.ConfirmationFilters.NewFilter(ctx).Eq("transaction", txID).Sort("sequence").Ascending()
	records, _, err := p.confirmations.GetMany(ctx, filter)
	if err != nil {
		return nil, err
	}
	confirmations := make([]*apitypes.Confirmation, len(records))
	for i, r := range records {
		confirmations[i] = r.Confirmation
	}
	return confirmations, nil
}

func (p *sqlPersistence) AddTransactionConfirmations(ctx context.Context, txID string, clearExisting bool, confirmations ...*apitypes.Confirmation) error {
	// Dispatch to TX writer
	for i, c := range confirmations {
		op := newTransactionOperation(txID)
		if i == 0 {
			// We dispatch a clear with the first record, if it was requested due to a fork
			op.clearConfirmations = clearExisting
		}
		op.confirmation = &apitypes.ConfirmationRecord{
			ResourceBase: dbsql.ResourceBase{
				ID: fftypes.NewUUID(),
			},
			TransactionID: txID,
			Confirmation:  c,
		}
		p.writer.queue(ctx, op)
	}
	return nil // we do this async for performance
}
