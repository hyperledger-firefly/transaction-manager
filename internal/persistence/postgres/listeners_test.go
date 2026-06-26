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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestListenerBasicPSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	// Write a listener
	stream1 := fftypes.NewUUID()
	l := &apitypes.Listener{
		ID:       fftypes.NewUUID(),
		Name:     strPtr("l1"),
		StreamID: stream1,
		Type:     &apitypes.ListenerTypeEvents,
		Filters: apitypes.ListenerFilters{
			*fftypes.JSONAnyPtr(`{"filter":"one"}`),
			*fftypes.JSONAnyPtr(`{"filter":"two"}`),
		},
		Options:   fftypes.JSONAnyPtr(`{"some":"options"}`),
		Signature: strPtr("event(stuff)"),
		FromBlock: strPtr("0"),
	}
	err := p.WriteListener(ctx, l)
	assert.NoError(t, err)
	l.Created = nil
	l.Updated = nil

	// Get it back
	l1, err := p.GetListener(ctx, l.ID)
	assert.NoError(t, err)
	assert.NotNil(t, l1.Created)
	assert.NotNil(t, l1.Updated)
	l1.Created = nil
	l1.Updated = nil
	assert.Equal(t, l, l1)

	// Update it
	lUpdated := &apitypes.Listener{
		ID:       l.ID,
		Name:     strPtr("l2"),
		Type:     &apitypes.ListenerTypeEvents,
		StreamID: stream1,
		Filters: apitypes.ListenerFilters{
			*fftypes.JSONAnyPtr(`{"filter":"three"}`),
		},
		Options:   fftypes.JSONAnyPtr(`{"new":"options"}`),
		Signature: strPtr("event(things)"),
		FromBlock: strPtr("12345"),
	}
	err = p.WriteListener(ctx, lUpdated)
	assert.NoError(t, err)
	lUpdated.Created = nil
	lUpdated.Updated = nil

	// Get it back
	l2, err := p.GetListener(ctx, l.ID)
	assert.NoError(t, err)
	assert.NotNil(t, l2.Created)
	assert.NotNil(t, l2.Updated)
	l2.Created = nil
	l2.Updated = nil
	assert.Equal(t, lUpdated, l2)

	// Delete it
	err = p.DeleteListener(ctx, l.ID)
	assert.NoError(t, err)
	es3, err := p.GetListener(ctx, l.ID)
	assert.NoError(t, err)
	assert.Nil(t, es3)

}

func TestListenerAfterPaginatePSQL(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	// Do a set of transaction operations through the writers, and confirm the results are correct
	ctx, p, _, done := initTestPSQL(t)
	defer done()

	var listeners []*apitypes.Listener
	stream1 := fftypes.NewUUID()
	stream2 := fftypes.NewUUID()
	for i := 0; i < 20; i++ {
		l := &apitypes.Listener{
			ID:       fftypes.NewUUID(),
			Name:     strPtr(fmt.Sprintf("l_%.3d", i)),
			Type:     &apitypes.ListenerTypeEvents,
			StreamID: stream1,
		}
		if i >= 10 {
			l.StreamID = stream2
		}
		err := p.WriteListener(ctx, l)
		assert.NoError(t, err)
		listeners = append(listeners, l)
	}

	// List them backwards, with no limit
	list1, err := p.ListListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionDescending)
	assert.NoError(t, err)
	assert.Len(t, list1, len(listeners))
	for i := 0; i < len(listeners); i++ {
		assert.Equal(t, listeners[len(listeners)-i-1].Name, list1[i].Name)
	}

	// List them forwards, with no limit, within just one listener
	list1, err = p.ListStreamListenersByCreateTime(ctx, nil, 0, txhandler.SortDirectionAscending, stream1)
	assert.NoError(t, err)
	assert.Len(t, list1, 10)
	for i := 0; i < 10; i++ {
		assert.Equal(t, listeners[i].Name, list1[i].Name)
	}

	// List them backwards with pagination - limited by stream
	list2, err := p.ListStreamListenersByCreateTime(ctx, listeners[12].ID, 5, txhandler.SortDirectionDescending, stream2)
	assert.NoError(t, err)
	assert.Len(t, list2, 2)
	assert.Equal(t, *listeners[11].Name, *list2[0].Name)
	assert.Equal(t, *listeners[10].Name, *list2[1].Name) // first one in stream2

	// List them forwards with pagination
	list3, err := p.ListListenersByCreateTime(ctx, listeners[10].ID, 5, txhandler.SortDirectionAscending)
	assert.NoError(t, err)
	assert.Len(t, list3, 5)
	assert.Equal(t, *listeners[11].Name, *list3[0].Name)
	assert.Equal(t, *listeners[12].Name, *list3[1].Name)
	assert.Equal(t, *listeners[13].Name, *list3[2].Name)
	assert.Equal(t, *listeners[14].Name, *list3[3].Name)
	assert.Equal(t, *listeners[15].Name, *list3[4].Name)

	// Fails with after check if not found
	_, err = p.ListStreamsByCreateTime(ctx, fftypes.NewUUID(), 5, txhandler.SortDirectionAscending)
	assert.Regexp(t, "FF00164", err)

	// Find just one
	fb := p.NewListenerFilter(ctx)
	list4, _, err := p.ListListeners(ctx, fb.And(fb.Eq("name", *listeners[15].Name)))
	assert.NoError(t, err)
	assert.Len(t, list4, 1)
	assert.Equal(t, *listeners[15].Name, *list4[0].Name)

	// Check search is scoped
	fb = p.NewListenerFilter(ctx)
	list5, _, err := p.ListStreamListeners(ctx, stream1, fb.And(fb.Eq("name", *listeners[15].Name)))
	assert.NoError(t, err)
	assert.Empty(t, list5)

}

func TestListListenersAfterNotFound(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*listeners").WillReturnRows(sqlmock.NewRows([]string{"seq"}))

	_, err := p.ListListenersByCreateTime(ctx, fftypes.NewUUID(), 0, txhandler.SortDirectionAscending)
	assert.Regexp(t, "FF00164", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestListStreamListenersAfterNotFound(t *testing.T) {
	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*listeners").WillReturnRows(sqlmock.NewRows([]string{"seq"}))

	_, err := p.ListStreamListenersByCreateTime(ctx, fftypes.NewUUID(), 0, txhandler.SortDirectionAscending, fftypes.NewUUID())
	assert.Regexp(t, "FF00164", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}
