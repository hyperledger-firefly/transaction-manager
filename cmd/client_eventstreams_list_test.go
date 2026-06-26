// Copyright © 2023 Kaleido, Inc.
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

package cmd

import (
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/internal/apiclient"
	"github.com/hyperledger-firefly/transaction-manager/mocks/apiclientmocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEventStreamsList(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "list"})
	mc.On("GetEventStreams", mock.Anything).Return([]apitypes.EventStream{}, nil)
	err := cmd.Execute()
	assert.NoError(t, err)
	mc.AssertExpectations(t)
}

func TestEventStreamsListError(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "list"})
	mc.On("GetEventStreams", mock.Anything).Return(nil, fmt.Errorf("pop"))
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}

func TestEventStreamsListBadClientConfig(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, fmt.Errorf("pop") })
	cmd.SetArgs([]string{"eventstreams", "list"})
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}
