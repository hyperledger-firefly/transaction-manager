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

package ffcapi

import (
	"github.com/hyperledger-firefly/common/pkg/fftypes"
)

type EventListenerHWMRequest struct {
	StreamID   *fftypes.UUID `json:"streamId"`
	ListenerID *fftypes.UUID `json:"listenerId"`
}

// EventListenerHWM return value.
//
// Important:
//
// The connector SHOULD implement LastDetected as well as Checkpoint on the response returned.
//
// Checkpoint is how far the connector has scanned.
// LastDetected is the checkpoint of the last event it pushed to FFTM.
//
// These differ, because the connector can scan past events that are still sat in the
// channel unread - FFTM cannot see those for itself, so it must be told.
//
// FFTM applies Checkpoint only once everything up to LastDetected has been delivered and acked.
//
// The connector needs to hold in memory a LastDetected value (to return when asked) that
// is updated BEFORE updating the value it returns for the scan position.
// When building the response for EventListenerHWM() read the scan position first, then
// the last detected (or guard them both under a single mutex).
//
// Following these simple rules ensures FFTM never moves the checkpoint past an event
// that is still in flight.
//
// Setting nil LastDetected is correct when nothing has been pushed for this listener.
//
// A connector that never implements LastDetected (always nil) is functional, but accepts
// a risk of a checkpoint being persisted past events FFTM has not yet acknowledged.
type EventListenerHWMResponse struct {
	Checkpoint   EventListenerCheckpoint `json:"checkpoint"`             // how far the connector has scanned
	Catchup      bool                    `json:"catchup,omitempty"`      // informational only - informs an operator that the stream is catching up
	LastDetected EventListenerCheckpoint `json:"lastDetected,omitempty"` // the checkpoint of the last event it pushed to FFTM
}
