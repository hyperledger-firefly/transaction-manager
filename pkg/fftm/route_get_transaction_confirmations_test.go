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
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetTransactionConfirmationsOK(t *testing.T) {

	url, _, mrc, done := newTestManagerMockRichDB(t)
	defer done()

	id1 := fftypes.NewUUID()
	mrc.On("ListTransactionConfirmations", mock.Anything, "tx1", mock.Anything).Return(
		[]*apitypes.ConfirmationRecord{{TransactionID: "tx1", ResourceBase: dbsql.ResourceBase{ID: id1}}}, nil, nil,
	)

	var out []*apitypes.ConfirmationRecord
	res, err := resty.New().R().
		SetResult(&out).
		Get(fmt.Sprintf("%s/transactions/%s/confirmations", url, "tx1"))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, id1, out[0].ID)
}

func TestGetTransactionConfirmationsNoRichQuery(t *testing.T) {

	url, _, done := newTestManagerMockNoRichDB(t)
	defer done()

	var txOut apitypes.ManagedTX
	var errorOut fftypes.RESTError
	res, err := resty.New().R().
		SetResult(&txOut).
		SetError(&errorOut).
		Get(fmt.Sprintf("%s/transactions/%s/confirmations", url, "tx1"))
	assert.NoError(t, err)
	assert.Equal(t, 501, res.StatusCode())
	assert.Regexp(t, "FF21085", errorOut.Error)
}
