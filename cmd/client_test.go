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
	"testing"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/transaction-manager/internal/apiclient"
	"github.com/stretchr/testify/assert"
)

func TestClientCommand(t *testing.T) {
	cmd := ClientCommand()
	err := cmd.Execute()
	assert.NoError(t, err)
}

func TestCreateDefaultClient(t *testing.T) {
	client, err := createClient()
	assert.NotNil(t, client)
	assert.NoError(t, err)
}

func TestCreateClientTLSFail(t *testing.T) {
	cfg := config.RootSection("fftm_client")
	apiclient.InitConfig(cfg)
	tlsConf := cfg.SubSection("tls")
	tlsConf.Set(fftls.HTTPConfTLSEnabled, true)
	tlsConf.Set(fftls.HTTPConfTLSCAFile, "!!!badness")
	_, err := createClient()
	assert.Regexp(t, "FF00153", err)
}
