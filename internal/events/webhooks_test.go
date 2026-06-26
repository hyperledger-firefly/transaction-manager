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

package events

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

func newTestWebhooks(t *testing.T, url string) *webhookAction {
	tmconfig.Reset()
	truthy := true
	oneSec := 1 * time.Second
	wa, err := newWebhookAction(context.Background(), &apitypes.WebhookConfig{
		TLSkipHostVerify: &truthy,
		URL:              &url,
		RequestTimeout:   (*fftypes.FFDuration)(&oneSec),
	})
	assert.NoError(t, err)
	return wa
}

func TestWebhooksBadTLS(t *testing.T) {
	tmconfig.Reset()
	tlsConf := tmconfig.WebhookPrefix.SubSection("tls")
	tlsConf.Set(fftls.HTTPConfTLSEnabled, true)
	tlsConf.Set(fftls.HTTPConfTLSCAFile, "!!!!badness")
	_, err := newWebhookAction(context.Background(), &apitypes.WebhookConfig{})
	assert.Regexp(t, "FF00153", err)
}

func TestWebhooksBadHost(t *testing.T) {
	tmconfig.Reset()
	ws := newTestWebhooks(t, "http://www.sample.invalid/guaranteed-to-fail")

	err := ws.attemptBatch(context.Background(), 0, 0, []*apitypes.EventWithContext{})
	assert.Regexp(t, "FF21041", err)
}

func TestWebhooksPrivateBlocked(t *testing.T) {
	tmconfig.Reset()
	ws := newTestWebhooks(t, "http://10.0.0.1/one-of-the-private-ranges")
	falsy := false
	ws.allowPrivateIPs = falsy

	err := ws.attemptBatch(context.Background(), 0, 0, []*apitypes.EventWithContext{})
	assert.Regexp(t, "FF21033", err)
}

func TestWebhooksCustomHeaders403(t *testing.T) {

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/test/path", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "test-value", r.Header.Get("test-header"))
		var events []*apitypes.EventWithContext
		err := json.NewDecoder(r.Body).Decode(&events)
		assert.NoError(t, err)
		w.WriteHeader(403)
	}))
	defer s.Close()

	tmconfig.Reset()
	ws := newTestWebhooks(t, fmt.Sprintf("http://%s/test/path", s.Listener.Addr()))
	ws.spec.Headers = map[string]string{
		"test-header": "test-value",
	}

	done := make(chan struct{})
	go func() {
		err := ws.attemptBatch(context.Background(), 0, 0, []*apitypes.EventWithContext{})
		assert.Regexp(t, "FF21035.*403", err)
		close(done)
	}()
	<-done
}

func TestWebhooksCustomHeadersConnectFail(t *testing.T) {

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Close()

	tmconfig.Reset()
	ws := newTestWebhooks(t, fmt.Sprintf("http://%s/test/path", s.Listener.Addr()))

	done := make(chan struct{})
	go func() {
		err := ws.attemptBatch(context.Background(), 0, 0, []*apitypes.EventWithContext{})
		assert.Regexp(t, "FF21042", err)
		close(done)
	}()
	<-done
}
