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

package apiclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/stretchr/testify/assert"
)

func newTestClientServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (FFTMClient, *httptest.Server) {
	server := httptest.NewServer(http.HandlerFunc(handler))
	config := config.RootSection("fftm_client")
	InitConfig(config)
	config.Set("url", server.URL)
	client, err := NewFFTMClient(context.Background(), config)
	assert.NoError(t, err)
	return client, server
}

func TestBadTLSCreds(t *testing.T) {
	config := config.RootSection("fftm_client")
	InitConfig(config)
	tlsConf := config.SubSection("tls")
	tlsConf.Set(fftls.HTTPConfTLSEnabled, true)
	tlsConf.Set(fftls.HTTPConfTLSCAFile, "!!!badness")
	_, err := NewFFTMClient(context.Background(), config)
	assert.Regexp(t, "FF00153", err)
}
