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
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPutLogging(t *testing.T) {
	_, m, done := newTestManagerWithMetrics(t, false)
	defer done()

	require.True(t, m.monitoringEnabled)
	url := fmt.Sprintf("http://%s", m.monitoringServer.Addr())

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("IsLive", mock.Anything).Return(&ffcapi.LiveResponse{Up: true}, ffcapi.ErrorReason(""), nil).Maybe()

	err := m.Start()
	assert.NoError(t, err)

	defer log.SetLevel("info")

	// Valid level change
	res, err := resty.New().R().Put(url + "/logging?level=debug")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, logrus.DebugLevel, logrus.GetLevel())

	// No level supplied is a no-op
	res, err = resty.New().R().Put(url + "/logging")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())

	// Invalid level
	res, err = resty.New().R().Put(url + "/logging?level=wrong")
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())

	// Wrong method
	res, err = resty.New().R().Get(url + "/logging")
	assert.NoError(t, err)
	assert.Equal(t, 405, res.StatusCode())
}
