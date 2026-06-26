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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

func TestGetEventSteams(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eventstreams" {
			eventStreams := []apitypes.EventStream{
				{
					ID: fftypes.NewUUID(),
				},
			}
			responseJSON, _ := json.Marshal(eventStreams)
			w.Header().Add("Content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseJSON)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	eventStreams, err := client.GetEventStreams(context.Background())
	assert.NoError(t, err)
	assert.NotEmpty(t, eventStreams)
}

func TestGetEventStreamsError(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eventstreams" {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	_, err := client.GetEventStreams(context.Background())
	assert.Error(t, err)
}

func TestDeleteListenersByName(t *testing.T) {
	esID := fftypes.NewUUID()
	listener1ID := fftypes.NewUUID()
	listener1Name := "fft:samesamebutdifferent:deleteme:ns1"
	listener2ID := fftypes.NewUUID()
	listener2Name := "fft:samesamebutdifferent:deletemetoo:ns1"
	listener3ID := fftypes.NewUUID()
	listener3Name := "fft:samesamebutdifferent:stuffandthings:default"
	listener4ID := fftypes.NewUUID()
	listener4Name := "fft:samesamebutdifferent:stuffandthings:alsodelete:ns1"
	deletedIDs := []string{}

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/eventstreams/%s/listeners", esID) {
			listeners := []apitypes.Listener{
				{
					ID:   listener1ID,
					Name: &listener1Name,
				},
				{
					ID:   listener2ID,
					Name: &listener2Name,
				},
				{
					ID:   listener3ID,
					Name: &listener3Name,
				},
				{
					ID:   listener4ID,
					Name: &listener4Name,
				},
			}
			responseJSON, _ := json.Marshal(listeners)
			w.Header().Add("Content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseJSON)
		}
		if strings.HasPrefix(r.URL.Path, fmt.Sprintf("/eventstreams/%s/listeners/", esID)) {
			listenerID := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("/eventstreams/%s/listeners/", esID))
			deletedIDs = append(deletedIDs, listenerID)
			w.WriteHeader(http.StatusNoContent)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteListenersByName(context.Background(), esID.String(), "^fft:.*:ns1$")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{listener1ID.String(), listener2ID.String(), listener4ID.String()}, deletedIDs)
}

func TestDeleteListenersByNameBadRegexp(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {}
	client, _ := newTestClientServer(t, handler)
	err := client.DeleteListenersByName(context.Background(), "esID", "*!BADREGEX")
	assert.Error(t, err)
	assert.Regexp(t, "error parsing regexp", err)
}

func TestDeleteListenersByNameListInvalidResponse(t *testing.T) {
	esID := fftypes.NewUUID()

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/eventstreams/%s/listeners", esID) {
			w.Header().Set("Content-Length", "1")
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteListenersByName(context.Background(), esID.String(), "^fft:.*:ns1$")
	assert.Error(t, err)
}

func TestDeleteListenersByNameListError(t *testing.T) {
	esID := fftypes.NewUUID()

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/eventstreams/%s/listeners", esID) {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteListenersByName(context.Background(), esID.String(), "^fft:.*:ns1$")
	assert.Error(t, err)
}

func TestDeleteListenersByNameDeleteError(t *testing.T) {
	esID := fftypes.NewUUID()
	listener1ID := fftypes.NewUUID()
	listener1Name := "fft:samesamebutdifferent:deleteme:ns1"
	listener2ID := fftypes.NewUUID()
	listener2Name := "fft:samesamebutdifferent:deletemetoo:ns1"
	listener3ID := fftypes.NewUUID()
	listener3Name := "fft:samesamebutdifferent:stuffandthings:default"
	listener4ID := fftypes.NewUUID()
	listener4Name := "fft:samesamebutdifferent:stuffandthings:alsodelete:ns1"

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/eventstreams/%s/listeners", esID) {
			listeners := []apitypes.Listener{
				{
					ID:   listener1ID,
					Name: &listener1Name,
				},
				{
					ID:   listener2ID,
					Name: &listener2Name,
				},
				{
					ID:   listener3ID,
					Name: &listener3Name,
				},
				{
					ID:   listener4ID,
					Name: &listener4Name,
				},
			}
			responseJSON, _ := json.Marshal(listeners)
			w.Header().Add("Content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseJSON)
		}
		if strings.HasPrefix(r.URL.Path, fmt.Sprintf("/eventstreams/%s/listeners/", esID)) {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteListenersByName(context.Background(), esID.String(), "^fft:.*:ns1$")
	assert.Error(t, err)
}

func TestDeleteListenersByNameDeleteInvalidResponse(t *testing.T) {
	esID := fftypes.NewUUID()
	listener1ID := fftypes.NewUUID()
	listener1Name := "fft:samesamebutdifferent:deleteme:ns1"
	listener2ID := fftypes.NewUUID()
	listener2Name := "fft:samesamebutdifferent:deletemetoo:ns1"
	listener3ID := fftypes.NewUUID()
	listener3Name := "fft:samesamebutdifferent:stuffandthings:default"
	listener4ID := fftypes.NewUUID()
	listener4Name := "fft:samesamebutdifferent:stuffandthings:alsodelete:ns1"

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/eventstreams/%s/listeners", esID) {
			listeners := []apitypes.Listener{
				{
					ID:   listener1ID,
					Name: &listener1Name,
				},
				{
					ID:   listener2ID,
					Name: &listener2Name,
				},
				{
					ID:   listener3ID,
					Name: &listener3Name,
				},
				{
					ID:   listener4ID,
					Name: &listener4Name,
				},
			}
			responseJSON, _ := json.Marshal(listeners)
			w.Header().Add("Content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseJSON)
		}
		if strings.HasPrefix(r.URL.Path, fmt.Sprintf("/eventstreams/%s/listeners/", esID)) {
			w.Header().Set("Content-Length", "1")
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteListenersByName(context.Background(), esID.String(), "^fft:.*:ns1$")
	assert.Error(t, err)
}

func TestDeleteEventStream(t *testing.T) {
	esID := fftypes.NewUUID()

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == fmt.Sprintf("/eventstreams/%s", esID) {
			w.WriteHeader(http.StatusOK)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteEventStream(context.Background(), esID.String())
	assert.NoError(t, err)
}

func TestDeleteEventStreamError(t *testing.T) {
	esID := fftypes.NewUUID()

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == fmt.Sprintf("/eventstreams/%s", esID) {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteEventStream(context.Background(), esID.String())
	assert.Error(t, err)
}

func TestDeleteEventStreamInvalidResponse(t *testing.T) {
	esID := fftypes.NewUUID()

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == fmt.Sprintf("/eventstreams/%s", esID) {
			w.Header().Set("Content-Length", "1")
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteEventStream(context.Background(), esID.String())
	assert.Error(t, err)
}

func TestDeleteEventStreamsByName(t *testing.T) {
	es1ID := fftypes.NewUUID()
	es1Name := "fft:samesamebutdifferent:deleteme:ns1"
	es2ID := fftypes.NewUUID()
	es2Name := "fft:samesamebutdifferent:deletemetoo:ns1"
	deletedIDs := []string{}

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eventstreams" {
			eventStreams := []apitypes.EventStream{
				{
					ID:   es1ID,
					Name: &es1Name,
				},
				{
					ID:   es2ID,
					Name: &es2Name,
				},
			}
			responseJSON, _ := json.Marshal(eventStreams)
			w.Header().Add("Content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseJSON)
		}
		if strings.HasPrefix(r.URL.Path, "/eventstreams/") {
			esID := strings.TrimPrefix(r.URL.Path, "/eventstreams/")
			deletedIDs = append(deletedIDs, esID)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteEventStreamsByName(context.Background(), "^fft:.*:ns1$")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{es1ID.String(), es2ID.String()}, deletedIDs)
}

func TestDeleteEventStreamsByNameBadRegex(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {}
	client, _ := newTestClientServer(t, handler)
	err := client.DeleteEventStreamsByName(context.Background(), "*!BADREGEX")
	assert.Error(t, err)
	assert.Regexp(t, "error parsing regexp", err)
}

func TestDeleteEventStreamsByNameError(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eventstreams" {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteEventStreamsByName(context.Background(), "^fft:.*:ns1$")
	assert.Error(t, err)
}

func TestDeleteEventStreamsByNameErrorDelete(t *testing.T) {
	es1ID := fftypes.NewUUID()
	es1Name := "fft:samesamebutdifferent:deleteme:ns1"
	es2ID := fftypes.NewUUID()
	es2Name := "fft:samesamebutdifferent:deletemetoo:ns1"

	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eventstreams" {
			eventStreams := []apitypes.EventStream{
				{
					ID:   es1ID,
					Name: &es1Name,
				},
				{
					ID:   es2ID,
					Name: &es2Name,
				},
			}
			responseJSON, _ := json.Marshal(eventStreams)
			w.Header().Add("Content-type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(responseJSON)
		}
		if strings.HasPrefix(r.URL.Path, "/eventstreams/") {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	client, server := newTestClientServer(t, handler)
	defer server.Close()

	err := client.DeleteEventStreamsByName(context.Background(), "^fft:.*:ns1$")
	assert.Error(t, err)
}
