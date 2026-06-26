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
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDeleteEventStream(t *testing.T) {

	url, m, done := newTestManager(t)
	defer done()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventStreamStopped", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStoppedResponse{}, ffcapi.ErrorReason(""), nil).Maybe()

	err := m.Start()
	assert.NoError(t, err)

	// Create stream
	var es apitypes.EventStream
	res, err := resty.New().R().
		SetBody(&apitypes.EventStream{
			Name: strPtr("my event stream"),
		}).
		SetResult(&es).
		Post(url + "/eventstreams")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())

	// Then delete it
	res, err = resty.New().R().
		SetResult(&es).
		Delete(url + "/eventstreams/" + es.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, 204, res.StatusCode())

	assert.Nil(t, m.eventStreams[(*es.ID)])

}
