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
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/mocks/persistencemocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetTransactionReceiptOK(t *testing.T) {

	url, m, done := newTestManagerMockNoRichDB(t)
	defer done()

	mpm := m.persistence.(*persistencemocks.Persistence)
	mpm.On("GetTransactionReceipt", mock.Anything, "tx1").Return(
		&ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
				BlockHash: "0x123456",
			},
		}, nil,
	)

	var out apitypes.ReceiptRecord
	res, err := resty.New().R().
		SetResult(&out).
		Get(fmt.Sprintf("%s/transactions/%s/receipt", url, "tx1"))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, "0x123456", out.BlockHash)
}

func TestGetTransactionReceiptNotFound(t *testing.T) {

	url, m, done := newTestManagerMockNoRichDB(t)
	defer done()

	mpm := m.persistence.(*persistencemocks.Persistence)
	mpm.On("GetTransactionReceipt", mock.Anything, "tx1").Return(nil, nil)

	var errRes fftypes.RESTError
	res, err := resty.New().R().
		SetError(&errRes).
		Get(fmt.Sprintf("%s/transactions/%s/receipt", url, "tx1"))
	assert.NoError(t, err)
	assert.Equal(t, 404, res.StatusCode())
	assert.Regexp(t, "FF00164", errRes.Error)
}
