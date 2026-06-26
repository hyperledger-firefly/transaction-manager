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
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/internal/ws"
	"github.com/hyperledger-firefly/transaction-manager/mocks/wsmocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

func TestWSAttemptBatchBadDistMode(t *testing.T) {

	mws := &wsmocks.WebSocketChannels{}
	mockWSChannels(mws)

	dmw := apitypes.DistributionMode("wrong")
	wsa := newWebSocketAction(mws, &apitypes.WebSocketConfig{
		DistributionMode: &dmw,
	}, "ut_stream")

	err := wsa.attemptBatch(context.Background(), 0, 0, []*apitypes.EventWithContext{})
	assert.Regexp(t, "FF21034", err)

}

func TestWSAttemptIgnoreWrongAcks(t *testing.T) {

	mws := &wsmocks.WebSocketChannels{}
	_, _, rc := mockWSChannels(mws)

	go func() {
		rc <- &ws.WebSocketCommandMessageOrError{Msg: &ws.WebSocketCommandMessage{
			BatchNumber: 12345,
		}}
		rc <- &ws.WebSocketCommandMessageOrError{Msg: &ws.WebSocketCommandMessage{
			BatchNumber: 23456,
		}}
	}()

	dmw := apitypes.DistributionModeBroadcast
	wsa := newWebSocketAction(mws, &apitypes.WebSocketConfig{
		DistributionMode: &dmw,
	}, "ut_stream")

	err := wsa.attemptBatch(context.Background(), 0, 0, []*apitypes.EventWithContext{})
	assert.NoError(t, err)

	err = wsa.waitForAck(context.Background(), rc, 23456)
	assert.NoError(t, err)
}

func TestWSAttemptBatchExitPushingEvent(t *testing.T) {

	mws := &wsmocks.WebSocketChannels{}
	_, bc, _ := mockWSChannels(mws)
	bc <- []*apitypes.EventWithContext{} // block the broadcast channel

	dmw := apitypes.DistributionModeBroadcast
	wsa := newWebSocketAction(mws, &apitypes.WebSocketConfig{
		DistributionMode: &dmw,
	}, "ut_stream")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wsa.attemptBatch(ctx, 0, 0, []*apitypes.EventWithContext{})
	assert.Regexp(t, "FF21038", err)

}

func TestWSAttemptBatchExitReceivingReply(t *testing.T) {

	mws := &wsmocks.WebSocketChannels{}
	_, _, rc := mockWSChannels(mws)

	dmw := apitypes.DistributionModeBroadcast
	wsa := newWebSocketAction(mws, &apitypes.WebSocketConfig{
		DistributionMode: &dmw,
	}, "ut_stream")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := wsa.waitForAck(ctx, rc, -1)
	assert.Regexp(t, "FF21039", err)

}

func TestWSAttemptBatchNackFromClient(t *testing.T) {

	mws := &wsmocks.WebSocketChannels{}
	_, _, rc := mockWSChannels(mws)
	rc <- &ws.WebSocketCommandMessageOrError{
		Err: fmt.Errorf("pop"),
	}

	dmw := apitypes.DistributionModeBroadcast
	wsa := newWebSocketAction(mws, &apitypes.WebSocketConfig{
		DistributionMode: &dmw,
	}, "ut_stream")

	err := wsa.waitForAck(context.Background(), rc, -1)
	assert.Regexp(t, "pop", err)

}
