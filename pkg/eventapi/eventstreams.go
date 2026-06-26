// Copyright © 2025 Kaleido, Inc.
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

package eventapi

import (
	"context"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

type EventStream interface {
	AddOrUpdateListener(ctx context.Context, id *fftypes.UUID,
		updates *apitypes.Listener, reset bool) (*apitypes.Listener, error) // Add or update a listener
	RemoveListener(ctx context.Context, id *fftypes.UUID) error          // Stop and remove a listener
	UpdateSpec(ctx context.Context, updates *apitypes.EventStream) error // Apply definition updates (if there are changes)
	Spec() *apitypes.EventStream                                         // Retrieve the merged definition to persist
	Status() apitypes.EventStreamStatus                                  // Get the current status
	Start(ctx context.Context) error                                     // Start delivery
	Stop(ctx context.Context) error                                      // Stop delivery (does not remove checkpoints)
	Delete(ctx context.Context) error                                    // Stop delivery, and clean up any checkpoint

	// For externally managed persistence with API to drive the internal event listener
	PollAPIMangedStream(ctx context.Context, checkpointIn *apitypes.EventStreamCheckpoint, timeout time.Duration) (events []*apitypes.EventWithContext, checkpointOut *apitypes.EventStreamCheckpoint, err error)
}
