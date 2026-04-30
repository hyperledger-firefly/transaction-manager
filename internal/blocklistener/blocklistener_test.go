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
	"testing"

	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
)

type testBlockConsumer struct {
	c chan *ffcapi.BlockHashEvent
}

func (tbc *testBlockConsumer) GetReceiveChannel() chan<- *ffcapi.BlockHashEvent {
	return tbc.c
}

func TestBlockListenerDoesNotBlock(t *testing.T) {

	unBuffered := make(chan *ffcapi.BlockHashEvent, 1)
	ctx, cancelCtx := context.WithCancel(context.Background())

	buffered, blockListenerDone := BufferChannel(ctx, &testBlockConsumer{c: unBuffered})

	for i := 0; i < 100; i++ {
		buffered <- &ffcapi.BlockHashEvent{}
	}

	// Get the one that was stuck in the pipe
	bhe := <-unBuffered
	assert.False(t, bhe.GapPotential)

	// We should get the unblocking one too, with GapPotential set
	bhe = <-unBuffered
	assert.True(t, bhe.GapPotential)

	// Block it again
	for i := 0; i < 100; i++ {
		buffered <- &ffcapi.BlockHashEvent{}
	}

	// And check we can exit while blocked
	cancelCtx()
	<-blockListenerDone

}

func TestExitOnContextCancel(t *testing.T) {

	unBuffered := make(chan *ffcapi.BlockHashEvent)
	ctx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()

	_, blockListenerDone := BufferChannel(ctx, &testBlockConsumer{c: unBuffered})
	<-blockListenerDone

}
