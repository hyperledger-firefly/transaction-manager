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
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (p *sqlPersistence) newReceiptsCollection() *dbsql.CrudBase[*apitypes.ReceiptRecord] {
	collection := &dbsql.CrudBase[*apitypes.ReceiptRecord]{
		DB:    p.db,
		Table: "receipts",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"block_number",
			"tx_index",
			"block_hash",
			"success",
			"protocol_id",
			"extra_info",
			"contract_loc",
		},
		FilterFieldMap: map[string]string{
			"sequence":         p.db.SequenceColumn(),
			"transaction":      dbsql.ColumnID,
			"blocknumber":      "block_number",
			"transactionindex": "tx_index",
			"blockhash":        "block_hash",
			"protocolid":       "protocol_id",
			"extrainfo":        "extra_info",
			"contractlocation": "contract_loc",
		},
		PatchDisabled: true,
		NilValue:      func() *apitypes.ReceiptRecord { return nil },
		NewInstance: func() *apitypes.ReceiptRecord {
			return &apitypes.ReceiptRecord{
				TransactionReceiptResponse: &ffcapi.TransactionReceiptResponse{},
			}
		},
		GetFieldPtr: func(inst *apitypes.ReceiptRecord, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.TransactionID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "block_number":
				return &inst.BlockNumber
			case "tx_index":
				return &inst.TransactionIndex
			case "block_hash":
				return &inst.BlockHash
			case "success":
				return &inst.Success
			case "protocol_id":
				return &inst.ProtocolID
			case "extra_info":
				return &inst.ExtraInfo
			case "contract_loc":
				return &inst.ContractLocation
			}
			return nil
		},
	}
	collection.Validate()
	return collection
}

func (p *sqlPersistence) GetTransactionReceipt(ctx context.Context, txID string) (receipt *ffcapi.TransactionReceiptResponse, err error) {
	r, err := p.receipts.GetByID(ctx, txID)
	if r == nil || err != nil {
		return nil, err
	}
	return r.TransactionReceiptResponse, err
}

func (p *sqlPersistence) SetTransactionReceipt(ctx context.Context, txID string, receipt *ffcapi.TransactionReceiptResponse) error {
	op := newTransactionOperation(txID)
	op.receipt = &apitypes.ReceiptRecord{
		TransactionID:              txID,
		TransactionReceiptResponse: receipt,
	}
	p.writer.queue(ctx, op)
	return nil // we do this async for performance
}
