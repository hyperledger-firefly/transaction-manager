// Copyright © 2022 - 2025 Kaleido, Inc.
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

package apitypes

import (
	"context"
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
)

func TestManagedTxNamespace(t *testing.T) {
	ctx := context.Background()
	mtx := &ManagedTX{
		ID: "not a valid ID",
	}

	// returns empty string when the ID is invalid
	ns := mtx.Namespace(ctx)
	assert.Equal(t, "", ns)

	// returns the namespace correctly
	mtx.ID = fftypes.NewNamespacedUUIDString(ctx, "ns1", fftypes.NewUUID())
	ns = mtx.Namespace(ctx)
	assert.Equal(t, "ns1", ns)
}

func TestTxHistoryRecord(t *testing.T) {
	r := &TXHistoryRecord{
		ID: fftypes.NewUUID(),
	}
	assert.Equal(t, r.ID.String(), r.GetID())
	t1 := fftypes.Now()
	r.SetCreated(t1)
	assert.Equal(t, t1, r.LastOccurrence)
	r.SetUpdated(fftypes.Now()) //no-op
}

func TestManagedTX(t *testing.T) {
	u1 := fftypes.NewUUID()
	mtx := &ManagedTX{
		ID: fmt.Sprintf("ns1:%s", u1),
	}
	ns, id, err := fftypes.ParseNamespacedUUID(context.Background(), mtx.ID)
	assert.NoError(t, err)
	assert.Equal(t, "ns1", ns)
	assert.Equal(t, u1, id)
	assert.Equal(t, mtx.ID, mtx.GetID())
	t1 := fftypes.Now()
	mtx.SetCreated(t1)
	assert.Equal(t, t1, mtx.Created)
	t2 := fftypes.Now()
	mtx.SetUpdated(t2)
	assert.Equal(t, t2, mtx.Updated)
	mtx.SetSequence(12345)
	assert.Equal(t, "000000012345", mtx.SequenceID)
}

func TestReceiptRecord(t *testing.T) {
	u1 := fftypes.NewUUID()
	r := &ReceiptRecord{
		TransactionID: fmt.Sprintf("ns1:%s", u1),
	}
	assert.Equal(t, r.TransactionID, r.GetID())
	t1 := fftypes.Now()
	r.SetCreated(t1)
	assert.Equal(t, t1, r.Created)
	t2 := fftypes.Now()
	r.SetUpdated(t2)
	assert.Equal(t, t2, r.Updated)
}

func TestTXUpdatesMerge(t *testing.T) {
	txu := &TXUpdates{}
	txu2 := &TXUpdates{
		Status:          ptrTo(TxStatusPending),
		DeleteRequested: fftypes.Now(),
		From:            ptrTo("1111"),
		To:              ptrTo("2222"),
		Nonce:           fftypes.NewFFBigInt(3333),
		Gas:             fftypes.NewFFBigInt(4444),
		Value:           fftypes.NewFFBigInt(5555),
		GasPrice:        fftypes.JSONAnyPtr(`{"some": "stuff"}`),
		TransactionData: ptrTo("xxxx"),
		TransactionHash: ptrTo("yyyy"),
		PolicyInfo:      fftypes.JSONAnyPtr(`{"more": "stuff"}`),
		FirstSubmit:     fftypes.Now(),
		LastSubmit:      fftypes.Now(),
		ErrorMessage:    ptrTo("pop"),
	}
	txu.Merge(txu2)
	assert.Equal(t, *txu2, *txu)
	txu.Merge(&TXUpdates{})
	assert.Equal(t, *txu2, *txu)
}

func TestApplyExternalTxUpdates(t *testing.T) {
	tests := []struct {
		name              string
		initialTx         *ManagedTX
		updates           TXUpdatesExternal
		expectedTx        *ManagedTX
		expectedResult    TXUpdates
		expectedUpdate    bool
		expectedChangeMap TxUpdateRecord
	}{
		{
			name: "no updates - nothing changes",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x123",
					Nonce: fftypes.NewFFBigInt(1),
					Gas:   fftypes.NewFFBigInt(21000),
					Value: fftypes.NewFFBigInt(1000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"20000000000"`),
				TransactionData: "0x",
			},
			updates: TXUpdatesExternal{},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x123",
					Nonce: fftypes.NewFFBigInt(1),
					Gas:   fftypes.NewFFBigInt(21000),
					Value: fftypes.NewFFBigInt(1000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"20000000000"`),
				TransactionData: "0x",
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update To field",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To: "0x123",
				},
			},
			updates: TXUpdatesExternal{
				To: ptrTo("0x456"),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To: "0x456",
				},
			},
			expectedResult: TXUpdates{
				To: ptrTo("0x456"),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"to": {
					OldValue: ptrTo("0x123"),
					NewValue: "0x456",
				},
			},
		},
		{
			name: "update To field - same value",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To: "0x123",
				},
			},
			updates: TXUpdatesExternal{
				To: ptrTo("0x123"),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To: "0x123",
				},
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update Nonce field",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Nonce: fftypes.NewFFBigInt(1),
				},
			},
			updates: TXUpdatesExternal{
				Nonce: fftypes.NewFFBigInt(2),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Nonce: fftypes.NewFFBigInt(2),
				},
			},
			expectedResult: TXUpdates{
				Nonce: fftypes.NewFFBigInt(2),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"nonce": {
					OldValue: ptrTo("1"),
					NewValue: "2",
				},
			},
		},
		{
			name: "update Nonce field - same value",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Nonce: fftypes.NewFFBigInt(1),
				},
			},
			updates: TXUpdatesExternal{
				Nonce: fftypes.NewFFBigInt(1),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Nonce: fftypes.NewFFBigInt(1),
				},
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update Gas field",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Gas: fftypes.NewFFBigInt(21000),
				},
			},
			updates: TXUpdatesExternal{
				Gas: fftypes.NewFFBigInt(50000),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Gas: fftypes.NewFFBigInt(50000),
				},
			},
			expectedResult: TXUpdates{
				Gas: fftypes.NewFFBigInt(50000),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"gas": {
					OldValue: ptrTo("21000"),
					NewValue: "50000",
				},
			},
		},
		{
			name: "update Gas field - same value",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Gas: fftypes.NewFFBigInt(21000),
				},
			},
			updates: TXUpdatesExternal{
				Gas: fftypes.NewFFBigInt(21000),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Gas: fftypes.NewFFBigInt(21000),
				},
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update Value field",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Value: fftypes.NewFFBigInt(1000),
				},
			},
			updates: TXUpdatesExternal{
				Value: fftypes.NewFFBigInt(2000),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Value: fftypes.NewFFBigInt(2000),
				},
			},
			expectedResult: TXUpdates{
				Value: fftypes.NewFFBigInt(2000),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"value": {
					OldValue: ptrTo("1000"),
					NewValue: "2000",
				},
			},
		},
		{
			name: "update Value field - same value",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Value: fftypes.NewFFBigInt(1000),
				},
			},
			updates: TXUpdatesExternal{
				Value: fftypes.NewFFBigInt(1000),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					Value: fftypes.NewFFBigInt(1000),
				},
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update GasPrice field",
			initialTx: &ManagedTX{
				GasPrice: fftypes.JSONAnyPtr(`"20000000000"`),
			},
			updates: TXUpdatesExternal{
				GasPrice: fftypes.JSONAnyPtr(`"30000000000"`),
			},
			expectedTx: &ManagedTX{
				GasPrice: fftypes.JSONAnyPtr(`"30000000000"`),
			},
			expectedResult: TXUpdates{
				GasPrice: fftypes.JSONAnyPtr(`"30000000000"`),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"gasPrice": {
					OldValue: ptrTo("\"20000000000\""),
					NewValue: "\"30000000000\"",
				},
			},
		},
		{
			name: "update GasPrice field - same value",
			initialTx: &ManagedTX{
				GasPrice: fftypes.JSONAnyPtr(`"20000000000"`),
			},
			updates: TXUpdatesExternal{
				GasPrice: fftypes.JSONAnyPtr(`"20000000000"`),
			},
			expectedTx: &ManagedTX{
				GasPrice: fftypes.JSONAnyPtr(`"20000000000"`),
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update TransactionData field",
			initialTx: &ManagedTX{
				TransactionData: "0x",
			},
			updates: TXUpdatesExternal{
				TransactionData: ptrTo("0x123456"),
			},
			expectedTx: &ManagedTX{
				TransactionData: "0x123456",
			},
			expectedResult: TXUpdates{
				TransactionData: ptrTo("0x123456"),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"transactionData": {
					OldValue: ptrTo("0x"),
					NewValue: "0x123456",
				},
			},
		},
		{
			name: "update TransactionData field - same value",
			initialTx: &ManagedTX{
				TransactionData: "0x123456",
			},
			updates: TXUpdatesExternal{
				TransactionData: ptrTo("0x123456"),
			},
			expectedTx: &ManagedTX{
				TransactionData: "0x123456",
			},
			expectedResult:    TXUpdates{},
			expectedUpdate:    false,
			expectedChangeMap: TxUpdateRecord{},
		},
		{
			name: "update multiple fields",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x123",
					Nonce: fftypes.NewFFBigInt(1),
					Gas:   fftypes.NewFFBigInt(21000),
					Value: fftypes.NewFFBigInt(1000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"20000000000"`),
				TransactionData: "0x",
			},
			updates: TXUpdatesExternal{
				To:              ptrTo("0x456"),
				Nonce:           fftypes.NewFFBigInt(2),
				Gas:             fftypes.NewFFBigInt(50000),
				Value:           fftypes.NewFFBigInt(2000),
				GasPrice:        fftypes.JSONAnyPtr(`"30000000000"`),
				TransactionData: ptrTo("0x123456"),
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x456",
					Nonce: fftypes.NewFFBigInt(2),
					Gas:   fftypes.NewFFBigInt(50000),
					Value: fftypes.NewFFBigInt(2000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"30000000000"`),
				TransactionData: "0x123456",
			},
			expectedResult: TXUpdates{
				To:              ptrTo("0x456"),
				Nonce:           fftypes.NewFFBigInt(2),
				Gas:             fftypes.NewFFBigInt(50000),
				Value:           fftypes.NewFFBigInt(2000),
				GasPrice:        fftypes.JSONAnyPtr(`"30000000000"`),
				TransactionData: ptrTo("0x123456"),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"gasPrice": {
					OldValue: ptrTo("\"20000000000\""),
					NewValue: "\"30000000000\"",
				},
				"transactionData": {
					OldValue: ptrTo("0x"),
					NewValue: "0x123456",
				},
				"to": {
					OldValue: ptrTo("0x123"),
					NewValue: "0x456",
				},
				"nonce": {
					OldValue: ptrTo("1"),
					NewValue: "2",
				},
				"gas": {
					OldValue: ptrTo("21000"),
					NewValue: "50000",
				},
				"value": {
					OldValue: ptrTo("1000"),
					NewValue: "2000",
				},
			},
		},
		{
			name: "update some fields, keep others unchanged",
			initialTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x123",
					Nonce: fftypes.NewFFBigInt(1),
					Gas:   fftypes.NewFFBigInt(21000),
					Value: fftypes.NewFFBigInt(1000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"20000000000"`),
				TransactionData: "0x",
			},
			updates: TXUpdatesExternal{
				To:    ptrTo("0x456"),
				Nonce: fftypes.NewFFBigInt(2),
				// Gas, Value, GasPrice, TransactionData not specified
			},
			expectedTx: &ManagedTX{
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x456",
					Nonce: fftypes.NewFFBigInt(2),
					Gas:   fftypes.NewFFBigInt(21000),
					Value: fftypes.NewFFBigInt(1000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"20000000000"`),
				TransactionData: "0x",
			},
			expectedResult: TXUpdates{
				To:    ptrTo("0x456"),
				Nonce: fftypes.NewFFBigInt(2),
			},
			expectedUpdate: true,
			expectedChangeMap: TxUpdateRecord{
				"nonce": {
					OldValue: ptrTo("1"),
					NewValue: "2",
				},
				"to": {
					OldValue: ptrTo("0x123"),
					NewValue: "0x456",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of the initial transaction to avoid modifying the test data
			mtx := &ManagedTX{}
			if tt.initialTx.To != "" {
				mtx.To = tt.initialTx.To
			}
			if tt.initialTx.Nonce != nil {
				mtx.Nonce = tt.initialTx.Nonce
			}
			if tt.initialTx.Gas != nil {
				mtx.Gas = tt.initialTx.Gas
			}
			if tt.initialTx.Value != nil {
				mtx.Value = tt.initialTx.Value
			}
			if tt.initialTx.GasPrice != nil {
				mtx.GasPrice = tt.initialTx.GasPrice
			}
			if tt.initialTx.TransactionData != "" {
				mtx.TransactionData = tt.initialTx.TransactionData
			}

			result, updated, changeMap := mtx.ApplyExternalTxUpdates(tt.updates)
			assert.Equal(t, tt.expectedResult, result)
			assert.Equal(t, tt.expectedUpdate, updated)
			assert.Equal(t, tt.expectedChangeMap, changeMap)

			// Verify the transaction was updated correctly
			assert.Equal(t, tt.expectedTx.To, mtx.To)
			if tt.expectedTx.Nonce != nil {
				assert.Equal(t, tt.expectedTx.Nonce.Int().String(), mtx.Nonce.Int().String())
			} else {
				assert.Nil(t, mtx.Nonce)
			}
			if tt.expectedTx.Gas != nil {
				assert.Equal(t, tt.expectedTx.Gas.Int().String(), mtx.Gas.Int().String())
			} else {
				assert.Nil(t, mtx.Gas)
			}
			if tt.expectedTx.Value != nil {
				assert.Equal(t, tt.expectedTx.Value.Int().String(), mtx.Value.Int().String())
			} else {
				assert.Nil(t, mtx.Value)
			}
			assert.Equal(t, tt.expectedTx.GasPrice, mtx.GasPrice)
			assert.Equal(t, tt.expectedTx.TransactionData, mtx.TransactionData)

			// Verify the result contains the expected updates
			assert.Equal(t, tt.expectedResult.To, result.To)
			if tt.expectedResult.Nonce != nil {
				assert.Equal(t, tt.expectedResult.Nonce.Int().String(), result.Nonce.Int().String())
			} else {
				assert.Nil(t, result.Nonce)
			}
			if tt.expectedResult.Gas != nil {
				assert.Equal(t, tt.expectedResult.Gas.Int().String(), result.Gas.Int().String())
			} else {
				assert.Nil(t, result.Gas)
			}
			if tt.expectedResult.Value != nil {
				assert.Equal(t, tt.expectedResult.Value.Int().String(), result.Value.Int().String())
			} else {
				assert.Nil(t, result.Value)
			}
			assert.Equal(t, tt.expectedResult.GasPrice, result.GasPrice)
			assert.Equal(t, tt.expectedResult.TransactionData, result.TransactionData)

			// Verify the updated flag
			assert.Equal(t, tt.expectedUpdate, updated)
		})
	}
}
