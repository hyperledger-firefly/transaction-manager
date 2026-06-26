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

func TestListenersList(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"listeners", "list", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7"})
	mc.On("GetListeners", mock.Anything, "f9506df2-5473-4fd4-9cfb-f835656eaaa7").Return([]apitypes.Listener{}, nil)
	err := cmd.Execute()
	assert.NoError(t, err)
	mc.AssertExpectations(t)
}

func TestListenersListNoEventStream(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"listeners", "list"})
	err := cmd.Execute()
	assert.Regexp(t, "eventstream flag not set", err)
}

func TestListenersListError(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"listeners", "list", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7"})
	mc.On("GetListeners", mock.Anything, "f9506df2-5473-4fd4-9cfb-f835656eaaa7").Return(nil, fmt.Errorf("pop"))
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}

func TestListenersListBadClientConf(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, fmt.Errorf("pop") })
	cmd.SetArgs([]string{"listeners", "list", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7"})
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}
