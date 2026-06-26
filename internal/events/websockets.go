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

package events

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/internal/ws"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func mergeValidateWsConfig(ctx context.Context, changed bool, base *apitypes.WebSocketConfig, updates *apitypes.WebSocketConfig) (*apitypes.WebSocketConfig, bool, error) {

	if base == nil {
		base = &apitypes.WebSocketConfig{}
	}
	if updates == nil {
		updates = &apitypes.WebSocketConfig{}
	}
	merged := &apitypes.WebSocketConfig{}

	// Distribution mode
	changed = apitypes.CheckUpdateEnum(changed, &merged.DistributionMode, base.DistributionMode, updates.DistributionMode, esDefaults.websocketDistributionMode)
	switch *merged.DistributionMode {
	case apitypes.DistributionModeLoadBalance, apitypes.DistributionMode("workloaddistribution"):
		// Migrate old "workloadDistribution" enum value to more consistent with other FF enums "load_balance"
		*merged.DistributionMode = apitypes.DistributionModeLoadBalance
	case apitypes.DistributionModeBroadcast:
	default:
		return nil, false, i18n.NewError(ctx, tmmsgs.MsgInvalidDistributionMode, *merged.DistributionMode)
	}

	return merged, changed, nil
}

type webSocketAction struct {
	topic      string
	spec       *apitypes.WebSocketConfig
	wsChannels ws.WebSocketChannels
}

func newWebSocketAction(wsChannels ws.WebSocketChannels, spec *apitypes.WebSocketConfig, topic string) *webSocketAction {
	return &webSocketAction{
		spec:       spec,
		wsChannels: wsChannels,
		topic:      topic,
	}
}

// attemptBatch attempts to deliver a batch over socket IO
func (w *webSocketAction) attemptBatch(ctx context.Context, batchNumber int64, attempt int, events []*apitypes.EventWithContext) error {
	var err error

	// Get a blocking channel to send and receive on our chosen namespace
	sender, broadcaster, receiver := w.wsChannels.GetChannels(w.topic)

	var channel chan<- interface{}
	switch *w.spec.DistributionMode {
	case apitypes.DistributionModeBroadcast:
		channel = broadcaster
	case apitypes.DistributionModeLoadBalance:
		channel = sender
	default:
		return i18n.NewError(ctx, tmmsgs.MsgInvalidDistributionMode, *w.spec.DistributionMode)
	}

	// Send the batch of events
	select {
	case channel <- &apitypes.EventBatch{
		BatchNumber: batchNumber,
		Events:      events,
	}:
		break
	case <-ctx.Done():
		err = i18n.NewError(ctx, tmmsgs.MsgWebSocketInterruptedSend)
	}

	// If we ever add more distribution modes, we may want to change this logic from a simple if statement
	if err == nil && *w.spec.DistributionMode != apitypes.DistributionModeBroadcast {
		log.L(ctx).Infof("Batch %d dispatched (len=%d,attempt=%d)", batchNumber, len(events), attempt)
		err = w.waitForAck(ctx, receiver, batchNumber)
	}

	// Pass back any exception due
	if err != nil {
		log.L(ctx).Infof("WebSocket event batch %d delivery failed (len=%d,attempt=%d): %s", batchNumber, len(events), attempt, err)
		return err
	}
	log.L(ctx).Infof("WebSocket event batch %d complete (len=%d,attempt=%d)", batchNumber, len(events), attempt)
	return nil
}

func (w *webSocketAction) waitForAck(ctx context.Context, receiver <-chan *ws.WebSocketCommandMessageOrError, batchNumber int64) error {
	// Wait for the next ack or exception
	for {
		select {
		case msgOrErr := <-receiver:
			if msgOrErr.Err != nil {
				// If we get an error, we have to assume the other side did not receive this batch, and send it again
				return msgOrErr.Err
			}
			if msgOrErr.Msg.BatchNumber != batchNumber {
				log.L(ctx).Infof("Discarding ack for batch %d (awaiting %d)", msgOrErr.Msg.BatchNumber, batchNumber)
				continue
			}
			log.L(ctx).Infof("Batch %d acknowledged", batchNumber)
			return nil
		case <-ctx.Done():
			return i18n.NewError(ctx, tmmsgs.MsgWebSocketInterruptedReceive)
		}
	}
}
