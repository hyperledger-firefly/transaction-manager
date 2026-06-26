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
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestGetLiveStatusError(t *testing.T) {
	_, m, close := newTestManager(t)
	defer close()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("IsLive", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("failed to get live status"))

	_, err := m.getLiveStatus(m.ctx)
	assert.Error(t, err)

	mfc.AssertExpectations(t)
}

func TestGetReadyStatusError(t *testing.T) {
	_, m, close := newTestManager(t)
	defer close()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("IsReady", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("failed to get ready status"))

	_, err := m.getReadyStatus(m.ctx)
	assert.Error(t, err)

	mfc.AssertExpectations(t)
}
