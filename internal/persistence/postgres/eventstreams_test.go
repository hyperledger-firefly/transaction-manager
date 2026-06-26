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

package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestEventStreamBasicPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// Write a stream
	es := &apitypes.EventStream{
		ID:                fftypes.NewUUID(),
		Name:              strPtr("es1"),
		Type:              &apitypes.EventStreamTypeWebhook,
		ErrorHandling:     &apitypes.ErrorHandlingTypeBlock,
		BatchSize:         u64Ptr(11111),
		BatchTimeout:      ffDurationPtr(22222 * time.Second),
		RetryTimeout:      ffDurationPtr(33333 * time.Second),
		BlockedRetryDelay: ffDurationPtr(44444 * time.Second),
		Webhook: &apitypes.WebhookConfig{
			URL: strPtr("http://example.com"),
		},
	}
	err := p.WriteStream(ctx, es)
	assert.NoError(t, err)
	es.Created = nil
	es.Updated = nil

	// Get it back
	es1, err := p.GetStream(ctx, es.ID)
	assert.NoError(t, err)
	assert.NotNil(t, es1.Created)
	assert.NotNil(t, es1.Updated)
	es1.Created = nil
	es1.Updated = nil
	assert.Equal(t, es, es1)

	// Update it
	esUpdated := &apitypes.EventStream{
		ID:                es.ID,
		Name:              strPtr("es2"),
		Type:              &apitypes.EventStreamTypeWebSocket,
		ErrorHandling:     &apitypes.ErrorHandlingTypeSkip,
		BatchSize:         u64Ptr(99911111),
		BatchTimeout:      ffDurationPtr(99922222 * time.Second),
		RetryTimeout:      ffDurationPtr(99933333 * time.Second),
		BlockedRetryDelay: ffDurationPtr(99944444 * time.Second),
		WebSocket: &apitypes.WebSocketConfig{
			DistributionMode: &apitypes.DistributionModeBroadcast,
		},
	}
	err = p.WriteStream(ctx, esUpdated)
	assert.NoError(t, err)
	esUpdated.Created = nil
	esUpdated.Updated = nil

	// Get it back
	es2, err := p.GetStream(ctx, es.ID)
	assert.NoError(t, err)
	assert.NotNil(t, es2.Created)
	assert.NotNil(t, es2.Updated)
	es2.Created = nil
	es2.Updated = nil
	assert.Equal(t, esUpdated, es2)

	// Delete it
	err = p.DeleteStream(ctx, es.ID)
	assert.NoError(t, err)
	es3, err := p.GetStream(ctx, es.ID)
	assert.NoError(t, err)
	assert.Nil(t, es3)

}

func TestEventStreamAfterPaginatePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	var eventStreams []*apitypes.EventStream
	for i := 0; i < 20; i++ {
		es := &apitypes.EventStream{
			ID:                fftypes.NewUUID(),
			Name:              strPtr(fmt.Sprintf("es_%.3d", i)),
			BatchTimeout:      ffDurationPtr(22222 * time.Second),
			RetryTimeout:      ffDurationPtr(33333 * time.Second),
			BlockedRetryDelay: ffDurationPtr(44444 * time.Second),
		}
		err := p.WriteStream(ctx, es)
		assert.NoError(t, err)
		eventStreams = append(eventStreams, es)
	}

	// List them backwards, with no limit
	list1, err := p.ListStreamsByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, list1, len(eventStreams))
	for i := 0; i < len(eventStreams); i++ {
		assert.Equal(t, eventStreams[len(eventStreams)-i-1].Name, list1[i].Name)
	}

	// List them forwards, with no limit
	list1, err = p.ListStreamsByCreateTime(ctx, nil, 0, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list1, len(eventStreams))
	for i := 0; i < len(eventStreams); i++ {
		assert.Equal(t, eventStreams[i].Name, list1[i].Name)
	}

	// List them backwards with pagination
	list2, err := p.ListStreamsByCreateTime(ctx, eventStreams[10].ID, 5, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, list2, 5)
	assert.Equal(t, *eventStreams[9].Name, *list2[0].Name)
	assert.Equal(t, *eventStreams[8].Name, *list2[1].Name)
	assert.Equal(t, *eventStreams[7].Name, *list2[2].Name)
	assert.Equal(t, *eventStreams[6].Name, *list2[3].Name)
	assert.Equal(t, *eventStreams[5].Name, *list2[4].Name)

	// List them forwards with pagination
	list3, err := p.ListStreamsByCreateTime(ctx, eventStreams[10].ID, 5, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list3, 5)
	assert.Equal(t, *eventStreams[11].Name, *list3[0].Name)
	assert.Equal(t, *eventStreams[12].Name, *list3[1].Name)
	assert.Equal(t, *eventStreams[13].Name, *list3[2].Name)
	assert.Equal(t, *eventStreams[14].Name, *list3[3].Name)
	assert.Equal(t, *eventStreams[15].Name, *list3[4].Name)

	// Fails with after check if not found
	_, err = p.ListStreamsByCreateTime(ctx, fftypes.NewUUID(), 5, txhandler.SortDirectionAscending)
	assert.Regexp(t, "FF00164", err)

	// Find just one
	fb := p.NewStreamFilter(ctx)
	list4, _, err := p.ListStreams(ctx, fb.And(fb.Eq("name", *eventStreams[15].Name)))
	assert.NoError(t, err)
	assert.Len(t, list4, 1)
	assert.Equal(t, *eventStreams[15].Name, *list4[0].Name)

}
