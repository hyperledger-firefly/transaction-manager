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
	"context"
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBalanceOK(t *testing.T) {
	_, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	mca := m.connector.(*ffcapimocks.API)
	mca.On("AddressBalance", mock.Anything, mock.Anything).Return(&ffcapi.AddressBalanceResponse{Balance: fftypes.NewFFBigInt(999)}, ffcapi.ErrorReason(""), nil)

	res, err := m.getLiveBalance(context.Background(), "0x4a8c8f1717570f9774652075e249ded38124d708", "latest")

	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, int64(999), res.AddressBalanceResponse.Balance.Int64())

	res, err = m.getLiveBalance(context.Background(), "0x4a8c8f1717570f9774652075e249ded38124d708", "")

	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, int64(999), res.AddressBalanceResponse.Balance.Int64())

	mca.AssertExpectations(t)
}

func TestBalanceFail(t *testing.T) {

	_, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	mca := m.connector.(*ffcapimocks.API)
	mca.On("AddressBalance", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	res, err := m.getLiveBalance(context.Background(), "0x4a8c8f1717570f9774652075e249ded38124d708", "")

	assert.Nil(t, res)
	assert.Regexp(t, "pop", err)

	mca.AssertExpectations(t)
}
