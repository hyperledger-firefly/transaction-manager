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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEventStreamsDeleteByID(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "delete", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7"})
	mc.On("DeleteEventStream", mock.Anything, "f9506df2-5473-4fd4-9cfb-f835656eaaa7").Return(nil)
	err := cmd.Execute()
	assert.NoError(t, err)
	mc.AssertExpectations(t)
}

func TestEventStreamsDeleteByIDBadClientConfig(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, fmt.Errorf("pop") })
	cmd.SetArgs([]string{"eventstreams", "delete", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7"})
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}

func TestEventStreamsDeleteByName(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "delete", "--name", "foo"})
	mc.On("DeleteEventStreamsByName", mock.Anything, "foo").Return(nil)
	err := cmd.Execute()
	assert.NoError(t, err)
	mc.AssertExpectations(t)
}

func TestEventStreamsDeleteByNameBadClientConfig(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, fmt.Errorf("pop") })
	cmd.SetArgs([]string{"eventstreams", "delete", "--name", "foo"})
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}

func TestEventStreamsDeleteNoID(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "delete"})
	err := cmd.Execute()
	assert.Regexp(t, "eventstream or name flag must be set", err)
}

func TestEventStreamsDeleteIDandName(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "delete", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7", "--name", "foo"})
	err := cmd.Execute()
	assert.Regexp(t, "eventstream and name flags cannot be combined", err)
}

func TestEventStreamsDeleteByNameError(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "delete", "--name", "foo"})
	mc.On("DeleteEventStreamsByName", mock.Anything, "foo").Return(fmt.Errorf("pop"))
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}

func TestEventStreamsDeleteByIDError(t *testing.T) {
	mc := apiclientmocks.NewFFTMClient(t)
	cmd := buildClientCommand(func() (apiclient.FFTMClient, error) { return mc, nil })
	cmd.SetArgs([]string{"eventstreams", "delete", "--eventstream", "f9506df2-5473-4fd4-9cfb-f835656eaaa7"})
	mc.On("DeleteEventStream", mock.Anything, "f9506df2-5473-4fd4-9cfb-f835656eaaa7").Return(fmt.Errorf("pop"))
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
	mc.AssertExpectations(t)
}
