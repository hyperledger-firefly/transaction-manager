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

package blocklistener

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
)

type NewBlockHashConsumer interface {
	GetReceiveChannel() chan<- *ffcapi.BlockHashEvent
}

// BufferChannel ensures it always pulls blocks from the channel passed to the connector
// for new block events, regardless of whether the downstream confirmations update queue
// is full blocked (likely because the event stream is blocked).
// This is critical to avoid the situation where one blocked stream, stops another stream
// from receiving block events.
// We use the same "GapPotential" flag that the connector can mark on a reconnect, to mark
// when we've had to discard events for a blocked event listener (event listeners could stay
// blocked indefinitely, so we can't leak memory by storing up an indefinite number of new
// block events).
func BufferChannel(ctx context.Context, target NewBlockHashConsumer) (buffered chan *ffcapi.BlockHashEvent, done chan struct{}) {
	buffered = make(chan *ffcapi.BlockHashEvent)
	done = make(chan struct{})
	go func() {
		defer close(done)
		var blockedUpdate *ffcapi.BlockHashEvent
		for {
			if blockedUpdate != nil {
				select {
				case blockUpdate := <-buffered:
					// Have to discard this
					blockedUpdate.GapPotential = true // there is a gap for sure at this point
					log.L(ctx).Debugf("Blocked event stream missed new block event: %v", blockUpdate.BlockHashes)
				case target.GetReceiveChannel() <- blockedUpdate:
					// We're not blocked any more
					log.L(ctx).Infof("Event stream block-listener unblocked")
					blockedUpdate = nil
				case <-ctx.Done():
					log.L(ctx).Debugf("Block listener exiting (previously blocked)")
					return
				}
			} else {
				select {
				case update := <-buffered:
					log.L(ctx).Debugf("Received block event: %v", update.BlockHashes)
					// Nothing to do unless we have confirmations turned on
					if target != nil {
						select {
						case target.GetReceiveChannel() <- update:
							// all good, we passed it on
						default:
							// we can't deliver it immediately, we switch to blocked mode
							log.L(ctx).Infof("Event stream block-listener became blocked")
							// Take a copy of the block update, so we can modify (to mark a gap) without affecting other streams
							var bu = *update
							blockedUpdate = &bu
						}
					}
				case <-ctx.Done():
					log.L(ctx).Debugf("Block listener exiting")
					return
				}
			}
		}
	}()
	return buffered, done
}
