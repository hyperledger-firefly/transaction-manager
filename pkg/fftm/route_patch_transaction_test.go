// Copyright © 2025 Kaleido, Inc.
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
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/mocks/txhandlermocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPatchTransaction(t *testing.T) {

	url, m, done := newTestManager(t)
	defer done()
	tx := newTestTxn(t, m, "0x0aaaaa", 10001, apitypes.TxStatusSucceeded)
	txID := tx.ID

	err := m.Start()
	assert.NoError(t, err)
	mth := txhandlermocks.TransactionHandler{}
	mockUpdate := &apitypes.TXUpdatesExternal{
		Gas: fftypes.NewFFBigInt(10003),
	}
	mth.On("HandleTransactionUpdate", mock.Anything, tx.ID, *mockUpdate).Return(tx, nil).Once()
	m.txHandler = &mth

	var txOut *apitypes.ManagedTX
	res, err := resty.New().R().
		SetResult(&txOut).
		SetBody(mockUpdate).
		Patch(fmt.Sprintf("%s/transactions/%s", url, txID))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, txID, txOut.ID)
}

func TestPatchTransactionFailed(t *testing.T) {
	url, m, done := newTestManager(t)
	defer done()

	err := m.Start()
	assert.NoError(t, err)
	mth := txhandlermocks.TransactionHandler{}
	mockUpdate := &apitypes.TXUpdatesExternal{
		Gas: fftypes.NewFFBigInt(10003),
	}
	mth.On("HandleTransactionUpdate", mock.Anything, "1234", *mockUpdate).Return(nil, fmt.Errorf("error")).Once()
	m.txHandler = &mth

	var txOut *apitypes.ManagedTX
	res, err := resty.New().R().
		SetResult(&txOut).
		SetBody(mockUpdate).
		Patch(fmt.Sprintf("%s/transactions/%s", url, "1234"))
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())
}
