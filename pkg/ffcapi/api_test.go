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
	"crypto/rand"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func TestSortEvents(t *testing.T) {

	events := make(Events, 10000)
	listenerUpdates := make(ListenerEvents, len(events))
	for i := 0; i < 10000; i++ {
		b, _ := rand.Int(rand.Reader, big.NewInt(1000))
		t, _ := rand.Int(rand.Reader, big.NewInt(10))
		l, _ := rand.Int(rand.Reader, big.NewInt(10))
		events[i] = &Event{
			ID: EventID{
				BlockNumber:      fftypes.FFuint64(b.Uint64()),
				TransactionIndex: fftypes.FFuint64(t.Uint64()),
				LogIndex:         fftypes.FFuint64(l.Uint64()),
			},
		}
		listenerUpdates[i] = &ListenerEvent{
			Event: events[i],
		}
	}
	sort.Sort(events)
	sort.Sort(listenerUpdates)

	for i := 1; i < len(events); i++ {
		assert.LessOrEqual(t, strings.Compare(events[i-1].ID.ProtocolID(), events[i].ID.ProtocolID()), 0)
		assert.LessOrEqual(t, strings.Compare(events[i-1].String(), events[i].String()), 0)
		assert.LessOrEqual(t, strings.Compare(listenerUpdates[i-1].Event.ID.ProtocolID(), listenerUpdates[i].Event.ID.ProtocolID()), 0)
	}
}

func TestSortBlockEventsString(t *testing.T) {

	assert.Equal(t, "block[12345/0x9614ad189f45ecff5f4949b22891c6bca7d83b40b50d8104bed101bc94395257]", (&BlockEvent{BlockInfo: BlockInfo{
		BlockNumber: fftypes.NewFFBigInt(12345),
		BlockHash:   "0x9614ad189f45ecff5f4949b22891c6bca7d83b40b50d8104bed101bc94395257",
	}}).String())
}

func TestBlockListenerCheckpoint(t *testing.T) {

	b10 := &BlockListenerCheckpoint{Block: 10}
	b20 := &BlockListenerCheckpoint{Block: 20}
	b30 := &BlockListenerCheckpoint{Block: 30}
	assert.True(t, b10.LessThan(b20))
	assert.False(t, b30.LessThan(b20))
	assert.False(t, b20.LessThan(b20))
}
