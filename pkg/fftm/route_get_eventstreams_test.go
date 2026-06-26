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
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetEventStreams(t *testing.T) {

	url, m, done := newTestManager(t)
	defer done()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventStreamStopped", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStoppedResponse{}, ffcapi.ErrorReason(""), nil).Maybe()

	err := m.Start()
	assert.NoError(t, err)

	// Create 3 streams
	var es1, es2, es3 apitypes.EventStream
	res, err := resty.New().R().SetBody(&apitypes.EventStream{Name: strPtr("stream1")}).SetResult(&es1).Post(url + "/eventstreams")
	assert.NoError(t, err)
	res, err = resty.New().R().SetBody(&apitypes.EventStream{Name: strPtr("stream2")}).SetResult(&es2).Post(url + "/eventstreams")
	assert.NoError(t, err)
	res, err = resty.New().R().SetBody(&apitypes.EventStream{Name: strPtr("stream3")}).SetResult(&es3).Post(url + "/eventstreams")
	assert.NoError(t, err)

	// Then get it
	var ess []*apitypes.EventStream
	res, err = resty.New().R().
		SetResult(&ess).
		Get(url + "/eventstreams?limit=1&after=" + es2.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())

	assert.Len(t, ess, 1)
	assert.Equal(t, es1.ID, ess[0].ID)

}

func TestGetEventStreamsRich(t *testing.T) {

	url, _, mrq, done := newTestManagerMockRichDB(t)
	defer done()
	e1 := fftypes.NewUUID()
	mrq.On("ListStreams", mock.Anything, mock.Anything, mock.Anything).Return(
		[]*apitypes.EventStream{{ID: e1}}, nil, nil,
	)

	var streams []*apitypes.EventStream
	res, err := resty.New().R().
		SetResult(&streams).
		Get(fmt.Sprintf("%s/eventstreams", url))
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Equal(t, e1, streams[0].ID)

}
