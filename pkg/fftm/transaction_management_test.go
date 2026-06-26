// Copyright © 2022 Kaleido, Inc.
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

package fftm

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/mocks/persistencemocks"
	"github.com/hyperledger-firefly/transaction-manager/mocks/txhandlermocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestReconcileConfirmationsForTransaction(t *testing.T) {
	_, m, done := newTestManager(t)
	defer done()
	mFFC := m.connector.(*ffcapimocks.API)

	mFFC.On("ReconcileConfirmationsForTransaction", m.ctx, "0x1234567890", []*ffcapi.MinimalBlockInfo{}, uint64(1)).Return(&ffcapi.ConfirmationUpdateResult{
		Confirmations: []*ffcapi.MinimalBlockInfo{},
	}, nil)

	_, err := m.ReconcileConfirmationsForTransaction(m.ctx, "0x1234567890", []*ffcapi.MinimalBlockInfo{}, uint64(1))
	assert.NoError(t, err)
}

func TestGetTransactionErrors(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetTransactionByIDWithStatus", m.ctx, mock.Anything, true).Return(nil, fmt.Errorf("pop")).Once()
	mp.On("GetTransactionByIDWithStatus", m.ctx, mock.Anything, false).Return(nil, nil).Once()
	mp.On("Close", mock.Anything).Return(nil).Maybe()

	_, err := m.GetTransactionByIDWithStatus(m.ctx, "id", true)
	assert.Regexp(t, "pop", err)

	_, err = m.GetTransactionByIDWithStatus(m.ctx, "id", false)
	assert.Regexp(t, "FF21067", err)

	mp.AssertExpectations(t)

}

func TestGetTransactionsErrors(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetTransactionByID", m.ctx, mock.Anything).Return(nil, fmt.Errorf("pop")).Once()
	mp.On("GetTransactionByID", m.ctx, mock.Anything).Return(nil, nil).Once()
	mp.On("Close", mock.Anything).Return(nil).Maybe()

	_, err := m.getTransactions(m.ctx, "", "bad limit", "", false, "")
	assert.Regexp(t, "FF21044", err)

	_, err = m.getTransactions(m.ctx, "", "", "", false, "wrong")
	assert.Regexp(t, "FF21064", err)

	_, err = m.getTransactions(m.ctx, "", "", "cannot be specified with pending", true, "")
	assert.Regexp(t, "FF21063", err)

	_, err = m.getTransactions(m.ctx, "after-causes-failure", "", "", false, "")
	assert.Regexp(t, "pop", err)

	_, err = m.getTransactions(m.ctx, "after-not-found", "", "", false, "")
	assert.Regexp(t, "FF21062", err)

	mp.AssertExpectations(t)

}

func TestDeleteTransactionError(t *testing.T) {
	_, m, done := newTestManager(t)
	defer done()
	mth := &txhandlermocks.TransactionHandler{}
	transientChannel := make(chan struct{})
	defer close(transientChannel)
	mth.On("HandleCancelTransaction", m.ctx, mock.Anything).Return(nil, fmt.Errorf("pop")).Once()
	m.txHandler = mth

	status, _, err := m.requestTransactionDeletion(m.ctx, "")
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Regexp(t, "pop", err)

	mth.AssertExpectations(t)

}

func TestUpdateTransaction(t *testing.T) {
	tests := []struct {
		name           string
		txID           string
		txUpdate       *apitypes.TXUpdatesExternal
		mockReturn     *apitypes.ManagedTX
		mockError      error
		expectedResult *apitypes.ManagedTX
		expectedError  string
	}{
		{
			name: "successful update",
			txID: "tx123",
			txUpdate: &apitypes.TXUpdatesExternal{
				To:              ptrTo("0x456"),
				Nonce:           fftypes.NewFFBigInt(2),
				Gas:             fftypes.NewFFBigInt(50000),
				Value:           fftypes.NewFFBigInt(2000),
				GasPrice:        fftypes.JSONAnyPtr(`"30000000000"`),
				TransactionData: ptrTo("0x123456"),
			},
			mockReturn: &apitypes.ManagedTX{
				ID: "tx123",
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x456",
					Nonce: fftypes.NewFFBigInt(2),
					Gas:   fftypes.NewFFBigInt(50000),
					Value: fftypes.NewFFBigInt(2000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"30000000000"`),
				TransactionData: "0x123456",
			},
			mockError: nil,
			expectedResult: &apitypes.ManagedTX{
				ID: "tx123",
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x456",
					Nonce: fftypes.NewFFBigInt(2),
					Gas:   fftypes.NewFFBigInt(50000),
					Value: fftypes.NewFFBigInt(2000),
				},
				GasPrice:        fftypes.JSONAnyPtr(`"30000000000"`),
				TransactionData: "0x123456",
			},
			expectedError: "",
		},
		{
			name:           "nil update - should return error",
			txID:           "tx123",
			txUpdate:       nil,
			mockReturn:     nil,
			mockError:      nil,
			expectedResult: nil,
			expectedError:  "FF21064", // MsgInvalidSortDirection error code
		},
		{
			name: "handler returns error",
			txID: "tx123",
			txUpdate: &apitypes.TXUpdatesExternal{
				Gas: fftypes.NewFFBigInt(50000),
			},
			mockReturn:     nil,
			mockError:      fmt.Errorf("handler error"),
			expectedResult: nil,
			expectedError:  "handler error",
		},
		{
			name: "partial update",
			txID: "tx123",
			txUpdate: &apitypes.TXUpdatesExternal{
				To:    ptrTo("0x456"),
				Nonce: fftypes.NewFFBigInt(2),
				// Other fields not specified
			},
			mockReturn: &apitypes.ManagedTX{
				ID: "tx123",
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x456",
					Nonce: fftypes.NewFFBigInt(2),
				},
			},
			mockError: nil,
			expectedResult: &apitypes.ManagedTX{
				ID: "tx123",
				TransactionHeaders: ffcapi.TransactionHeaders{
					To:    "0x456",
					Nonce: fftypes.NewFFBigInt(2),
				},
			},
			expectedError: "",
		},
		{
			name:     "empty update",
			txID:     "tx123",
			txUpdate: &apitypes.TXUpdatesExternal{
				// No fields specified
			},
			mockReturn: &apitypes.ManagedTX{
				ID: "tx123",
			},
			mockError: nil,
			expectedResult: &apitypes.ManagedTX{
				ID: "tx123",
			},
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, m, done := newTestManager(t)
			defer done()

			// Set up mock transaction handler
			mth := &txhandlermocks.TransactionHandler{}
			if tt.txUpdate != nil {
				mth.On("HandleTransactionUpdate", m.ctx, tt.txID, *tt.txUpdate).Return(tt.mockReturn, tt.mockError).Once()
			}
			m.txHandler = mth

			// Call the function
			result, err := m.updateTransaction(m.ctx, tt.txID, tt.txUpdate)

			// Verify results
			if tt.expectedError != "" {
				assert.Error(t, err)
				if tt.expectedError != "handler error" {
					assert.Regexp(t, tt.expectedError, err.Error())
				} else {
					assert.Equal(t, tt.expectedError, err.Error())
				}
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}

			// Verify mock expectations
			mth.AssertExpectations(t)
		})
	}
}

func ptrTo[T any](v T) *T {
	return &v
}
