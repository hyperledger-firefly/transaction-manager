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

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

type listener struct {
	es             *eventStream
	spec           *apitypes.Listener
	lastCheckpoint *fftypes.FFTime
	checkpoint     ffcapi.EventListenerCheckpoint
}

type blockListenerAddRequest struct {
	ListenerID *fftypes.UUID
	StreamID   *fftypes.UUID
	Name       string
	FromBlock  string
	Checkpoint *ffcapi.BlockListenerCheckpoint
}

func listenerSpecToOptions(spec *apitypes.Listener) ffcapi.EventListenerOptions {
	return ffcapi.EventListenerOptions{
		FromBlock: *spec.FromBlock,
		Filters:   spec.Filters,
		Options:   spec.Options,
	}
}

func (l *listener) stop(startedState *startedStreamState) (err error) {
	if l.spec.Type != nil && *l.spec.Type == apitypes.ListenerTypeBlocks {
		err = l.es.confirmations.StopConfirmedBlockListener(startedState.ctx, l.spec.ID)
	} else {
		_, _, err = l.es.connector.EventListenerRemove(startedState.ctx, &ffcapi.EventListenerRemoveRequest{
			StreamID:   l.spec.StreamID,
			ListenerID: l.spec.ID,
		})

	}
	return
}

func (l *listener) buildAddRequest(ctx context.Context, cp *apitypes.EventStreamCheckpoint) *ffcapi.EventListenerAddRequest {
	req := &ffcapi.EventListenerAddRequest{
		EventListenerOptions: listenerSpecToOptions(l.spec),
		Name:                 *l.spec.Name,
		ListenerID:           l.spec.ID,
		StreamID:             l.spec.StreamID,
	}
	if cp != nil {
		jsonCP := cp.Listeners[*l.spec.ID]
		if jsonCP != nil {
			listenerCheckpoint := l.es.connector.EventStreamNewCheckpointStruct()
			err := json.Unmarshal(jsonCP, &listenerCheckpoint)
			if err != nil {
				log.L(ctx).Errorf("Failed to restore checkpoint for listener '%s': %s", l.spec.ID, err)
			} else {
				req.Checkpoint = listenerCheckpoint
			}
		}
	}
	return req
}

func (l *listener) buildBlockAddRequest(ctx context.Context, cp *apitypes.EventStreamCheckpoint) *blockListenerAddRequest {
	req := &blockListenerAddRequest{
		Name:       *l.spec.Name,
		ListenerID: l.spec.ID,
		StreamID:   l.spec.StreamID,
		FromBlock:  *l.spec.FromBlock,
	}
	if cp != nil {
		jsonCP := cp.Listeners[*l.spec.ID]
		if jsonCP != nil {
			var listenerCheckpoint ffcapi.BlockListenerCheckpoint
			err := json.Unmarshal(jsonCP, &listenerCheckpoint)
			if err != nil {
				log.L(ctx).Errorf("Failed to restore checkpoint for block listener '%s': %s", l.spec.ID, err)
			} else {
				req.Checkpoint = &listenerCheckpoint
			}
		}
	}
	return req
}

func (l *listener) start(startedState *startedStreamState, cp *apitypes.EventStreamCheckpoint) error {
	_, _, err := l.es.connector.EventListenerAdd(startedState.ctx, l.buildAddRequest(startedState.ctx, cp))
	return err
}
