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

package fftm

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/mocks/persistencemocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRestoreStreamsAndListenersOK(t *testing.T) {

	_, m, done := newTestManager(t)
	defer done()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerRemove", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerRemoveResponse{}, ffcapi.ErrorReason(""), nil).Maybe()
	mfc.On("EventStreamStopped", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStoppedResponse{}, ffcapi.ErrorReason(""), nil).Maybe()

	falsy := false

	es1 := &apitypes.EventStream{ID: apitypes.NewULID(), Name: strPtr("stream1"), Suspended: &falsy}
	err := m.persistence.WriteStream(m.ctx, es1)
	assert.NoError(t, err)

	e1l1 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("listener1"), StreamID: es1.ID}
	err = m.persistence.WriteListener(m.ctx, e1l1)
	assert.NoError(t, err)

	e1l2 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("listener2"), StreamID: es1.ID}
	err = m.persistence.WriteListener(m.ctx, e1l2)
	assert.NoError(t, err)

	es2 := &apitypes.EventStream{ID: apitypes.NewULID(), Name: strPtr("stream2"), Suspended: &falsy}
	err = m.persistence.WriteStream(m.ctx, es2)
	assert.NoError(t, err)

	e2l1 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("listener3"), StreamID: es2.ID}
	err = m.persistence.WriteListener(m.ctx, e2l1)
	assert.NoError(t, err)

	err = m.Start()
	assert.NoError(t, err)

	assert.Equal(t, es1.ID, m.streamsByName["stream1"])
	assert.Equal(t, es2.ID, m.streamsByName["stream2"])

	// The listener names are indexed too, so a later create cannot re-use them
	assert.Equal(t, e1l1.ID, m.listenersByName["listener1"])
	assert.Equal(t, e1l2.ID, m.listenersByName["listener2"])
	assert.Equal(t, e2l1.ID, m.listenersByName["listener3"])

	mfc.AssertExpectations(t)

}

func TestRestoreStreamsReadFailed(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamsByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending).Return(nil, fmt.Errorf("pop"))

	err := m.restoreStreams()
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

func TestRestoreListenersReadFailed(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamsByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{
		{ID: fftypes.NewUUID()},
	}, nil)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), 0, txhandler.SortDirectionAscending, mock.Anything).Return(nil, fmt.Errorf("pop"))

	err := m.restoreStreams()
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

func TestRestoreStreamsValidateFail(t *testing.T) {

	_, m, done := newTestManager(t)
	defer done()

	falsy := false
	es1 := &apitypes.EventStream{ID: apitypes.NewULID(), Name: strPtr(""), Suspended: &falsy}
	err := m.persistence.WriteStream(m.ctx, es1)
	assert.NoError(t, err)

	err = m.restoreStreams()
	assert.Regexp(t, "FF21028", err)

}

func TestRestoreListenersStartFail(t *testing.T) {

	_, m, done := newTestManager(t)
	defer done()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	falsy := false
	es1 := &apitypes.EventStream{ID: apitypes.NewULID(), Name: strPtr("stream1"), Suspended: &falsy}
	err := m.persistence.WriteStream(m.ctx, es1)
	assert.NoError(t, err)

	e1l1 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("listener1"), StreamID: es1.ID}
	err = m.persistence.WriteListener(m.ctx, e1l1)
	assert.NoError(t, err)

	err = m.restoreStreams()
	assert.Regexp(t, "pop", err)

	mfc.AssertExpectations(t)

}

func TestDeleteStartedListener(t *testing.T) {

	_, m, done := newTestManager(t)
	defer done()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventStreamStopped", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStoppedResponse{}, ffcapi.ErrorReason(""), nil).Maybe()

	falsy := false
	es1 := &apitypes.EventStream{ID: apitypes.NewULID(), Name: strPtr("stream1"), Suspended: &falsy}
	err := m.persistence.WriteStream(m.ctx, es1)
	assert.NoError(t, err)

	e1l1 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("listener1"), StreamID: es1.ID}
	err = m.persistence.WriteListener(m.ctx, e1l1)
	assert.NoError(t, err)

	err = m.Start()
	assert.NoError(t, err)

	err = m.DeleteStream(m.ctx, es1.ID.String())
	assert.NoError(t, err)

	mfc.AssertExpectations(t)

}

func TestDeleteStartedListenerFail(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	esID := apitypes.NewULID()
	lID := apitypes.NewULID()
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return([]*apitypes.Listener{
		{ID: lID, StreamID: esID},
	}, nil)
	mp.On("DeleteListener", m.ctx, lID).Return(fmt.Errorf("pop"))

	err := m.deleteAllStreamListeners(m.ctx, esID)
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

func TestDeleteStartedListenerWithPagination(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	esID := apitypes.NewULID()
	lID := apitypes.NewULID()
	secondID := apitypes.NewULID()
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return(
		[]*apitypes.Listener{
			{ID: lID, StreamID: esID},
			{ID: secondID, StreamID: esID},
		}, nil).Once()
	thirdID := apitypes.NewULID()
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return(
		[]*apitypes.Listener{
			{ID: thirdID, StreamID: esID},
		}, nil).Once()
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return(
		[]*apitypes.Listener{}, nil)
	mp.On("DeleteListener", m.ctx, lID).Return(nil)
	mp.On("DeleteListener", m.ctx, secondID).Return(nil)
	mp.On("DeleteListener", m.ctx, thirdID).Return(nil)

	err := m.deleteAllStreamListeners(m.ctx, esID)
	assert.NoError(t, err)

	mp.AssertExpectations(t)
}

func TestDeleteStreamBadID(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	err := m.DeleteStream(m.ctx, "Bad ID")
	assert.Regexp(t, "FF00138", err)

}

func TestDeleteStreamListenerPersistenceFail(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	esID := apitypes.NewULID()
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return(nil, fmt.Errorf("pop"))

	err := m.DeleteStream(m.ctx, esID.String())
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

func TestDeleteStreamPersistenceFail(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	esID := apitypes.NewULID()
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return([]*apitypes.Listener{}, nil)
	mp.On("DeleteStream", m.ctx, esID).Return(fmt.Errorf("pop"))

	err := m.DeleteStream(m.ctx, esID.String())
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

func TestDeleteStreamNotInitialized(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	esID := apitypes.NewULID()
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending, esID).Return([]*apitypes.Listener{}, nil)
	mp.On("DeleteStream", m.ctx, esID).Return(nil)

	err := m.DeleteStream(m.ctx, esID.String())
	assert.NoError(t, err)

	mp.AssertExpectations(t)
}

func TestCreateRenameStreamNameReservation(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(fmt.Errorf("temporary")).Once()
	mp.On("DeleteCheckpoint", m.ctx, mock.Anything).Return(fmt.Errorf("temporary")).Once()
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	// Reject missing name
	_, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{})
	assert.Regexp(t, "FF21028", err)

	// Attempt to start and encounter a temporary error
	_, err = m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("Name1")})
	assert.Regexp(t, "temporary", err)

	// Ensure we still allow use of the name after the glitch is fixed
	es1, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("Name1")})
	assert.NoError(t, err)

	// Ensure we can't create another stream of same name
	_, err = m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("Name1")})
	assert.Regexp(t, "FF21047", err)

	// Create a second stream to test clash on rename
	es2, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("Name2")})
	assert.NoError(t, err)

	// Check for clash
	_, err = m.UpdateStream(m.ctx, es1.ID.String(), &apitypes.EventStream{Name: strPtr("Name2")})
	assert.Regexp(t, "FF21047", err)

	// Check for no-op rename to self
	_, err = m.UpdateStream(m.ctx, es2.ID.String(), &apitypes.EventStream{Name: strPtr("Name2")})
	assert.NoError(t, err)

	mp.AssertExpectations(t)
}

func TestCreateStreamValidateFail(t *testing.T) {

	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	wrongType := apitypes.DistributionMode("wrong")
	_, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1"), Type: &wrongType})
	assert.Regexp(t, "FF21029", err)

}

func TestCreateAndStoreNewStreamListenerBadID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.CreateAndStoreNewStreamListener(m.ctx, "bad", nil)
	assert.Regexp(t, "FF00138", err)
}

func TestUpdateExistingListenerNotFound(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetListener", m.ctx, mock.Anything).Return(nil, nil)

	_, err := m.UpdateExistingListener(m.ctx, apitypes.NewULID().String(), apitypes.NewULID().String(), &apitypes.Listener{}, false)
	assert.Regexp(t, "FF21046", err)

	mp.AssertExpectations(t)
}

func TestCreateOrUpdateListenerNotFound(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.createOrUpdateListener(m.ctx, apitypes.NewULID(), nil, &apitypes.Listener{StreamID: apitypes.NewULID()}, false)
	assert.Regexp(t, "FF21045", err)

}

func TestCreateOrUpdateListenerFail(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)
	mp.On("WriteListener", m.ctx, mock.Anything).Return(nil)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	_, err = m.createOrUpdateListener(m.ctx, apitypes.NewULID(), nil, &apitypes.Listener{StreamID: es.ID}, false)
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

func TestCreateOrUpdateListenerFailMergeEthCompatMethods(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))
	mfc.On("EventListenerRemove", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerRemoveResponse{}, ffcapi.ErrorReason(""), nil)

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	l := &apitypes.Listener{
		StreamID:         es.ID,
		EthCompatMethods: fftypes.JSONAnyPtr(`{}`),
	}

	_, err = m.createOrUpdateListener(m.ctx, apitypes.NewULID(), nil, l, false)
	assert.Error(t, err)

	mp.AssertExpectations(t)
}

func TestCreateOrUpdateListenerWriteFail(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("WriteListener", m.ctx, mock.Anything).Return(fmt.Errorf("pop"))
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerRemove", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerRemoveResponse{}, ffcapi.ErrorReason(""), nil)

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	_, err = m.createOrUpdateListener(m.ctx, apitypes.NewULID(), nil, &apitypes.Listener{StreamID: es.ID}, false)
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
}

type testListenerCounts struct {
	listenerAdd    atomic.Int32
	listenerRemove atomic.Int32
	writeListener  atomic.Int32
}

func testStreamWithListenerMocks(t *testing.T, m *manager) (*apitypes.EventStream, *testListenerCounts) {
	counts := &testListenerCounts{}

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)
	mp.On("WriteListener", m.ctx, mock.Anything).Run(func(args mock.Arguments) {
		counts.writeListener.Add(1)
	}).Return(nil).Maybe()

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventStreamStopped", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStoppedResponse{}, ffcapi.ErrorReason(""), nil).Maybe()
	// A fixed resolved signature, so that renames pass the "filters cannot change" check
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{
		ResolvedSignature: "sig1",
	}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		counts.listenerAdd.Add(1)
	}).Return(nil, ffcapi.ErrorReason(""), nil).Maybe()
	mfc.On("EventListenerRemove", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		counts.listenerRemove.Add(1)
	}).Return(&ffcapi.EventListenerRemoveResponse{}, ffcapi.ErrorReason(""), nil).Maybe()

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})
	require.NoError(t, err)
	return es, counts
}

func TestConcurrentCreateListenerSameName(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)

	const racers = 8
	start := make(chan struct{})
	errors := make([]error, racers)
	wg := sync.WaitGroup{}
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			_, errors[i] = m.createAndStoreNewListener(m.ctx, &apitypes.Listener{
				StreamID: es.ID,
				Name:     strPtr("sameName"),
			})
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for _, err := range errors {
		if err == nil {
			winners++
		} else {
			assert.Regexp(t, "FF21098", err)
		}
	}
	assert.Equal(t, 1, winners)

	// The losers never got as far as the connector, or the database
	assert.Equal(t, int32(1), counts.listenerAdd.Load())
	assert.Equal(t, int32(0), counts.listenerRemove.Load())
	assert.Equal(t, int32(1), counts.writeListener.Load())

	assert.NotNil(t, m.listenersByName["sameName"])
}

func TestCreateRenameListenerNameReservation(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)
	mp := m.persistence.(*persistencemocks.Persistence)

	// Create L1
	l1, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	require.NoError(t, err)
	assert.Equal(t, l1.ID, m.listenersByName["L1"])

	// A second listener cannot take the same name
	_, err = m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	assert.Regexp(t, "FF21098", err)

	// Create L2
	l2, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L2")})
	require.NoError(t, err)

	// Renaming L2 onto L1's name is a clash
	_, err = m.createOrUpdateListener(m.ctx, l2.ID, strPtr("L2"), &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")}, false)
	assert.Regexp(t, "FF21098", err)
	assert.Equal(t, l2.ID, m.listenersByName["L2"])

	// A no-op rename to its own name is fine, and must not release the name
	_, err = m.createOrUpdateListener(m.ctx, l2.ID, strPtr("L2"), &apitypes.Listener{StreamID: es.ID, Name: strPtr("L2")}, false)
	require.NoError(t, err)
	assert.Equal(t, l2.ID, m.listenersByName["L2"])

	// A real rename releases the old name for re-use
	l2Renamed, err := m.createOrUpdateListener(m.ctx, l2.ID, strPtr("L2"), &apitypes.Listener{StreamID: es.ID, Name: strPtr("L3")}, false)
	require.NoError(t, err)
	assert.Nil(t, m.listenersByName["L2"])
	assert.Equal(t, l2.ID, m.listenersByName["L3"])

	l4, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L2")})
	require.NoError(t, err)
	assert.Equal(t, l4.ID, m.listenersByName["L2"])

	// Deleting releases the name. DeleteListener reads the spec back from persistence, so it sees
	// the renamed listener - not the spec the original create returned.
	mp.On("GetListener", m.ctx, l2.ID).Return(l2Renamed, nil)
	mp.On("DeleteListener", m.ctx, l2.ID).Return(nil)
	err = m.DeleteListener(m.ctx, es.ID.String(), l2.ID.String())
	require.NoError(t, err)
	assert.Nil(t, m.listenersByName["L3"])

	l5, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L3")})
	require.NoError(t, err)
	assert.Equal(t, l5.ID, m.listenersByName["L3"])

	// Only the one listener we deleted was ever deregistered
	assert.Equal(t, int32(1), counts.listenerRemove.Load())
}

func TestCreateListenerWriteFailReleasesName(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)
	mp := m.persistence.(*persistencemocks.Persistence)
	// Take precedence over the .Maybe() success case set up above
	mp.ExpectedCalls = prependExpectation(mp.ExpectedCalls,
		mp.On("WriteListener", m.ctx, mock.Anything).Return(fmt.Errorf("pop")).Once())

	_, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	assert.Regexp(t, "pop", err)

	// The listener was never applied to the stream, so there is nothing to deregister and the name
	// is free again
	assert.Equal(t, int32(0), counts.listenerAdd.Load())
	assert.Equal(t, int32(0), counts.listenerRemove.Load())
	assert.Nil(t, m.listenersByName["L1"])

	// ... and the stream has no knowledge of it, so restarting cannot resurrect a listener that
	// was never written
	assert.Empty(t, restartStream(t, m, es).InitialListeners)

	l1, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	require.NoError(t, err)
	assert.Equal(t, l1.ID, m.listenersByName["L1"])
	assert.Len(t, restartStream(t, m, es).InitialListeners, 1)
}

func TestCreateListenerPrepareFailReleasesName(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)

	// A reset of a listener the stream does not know about is rejected by the prepare, after the
	// name has already been reserved
	_, err := m.createOrUpdateListener(m.ctx, apitypes.NewULID(), nil, &apitypes.Listener{
		StreamID: es.ID,
		Name:     strPtr("L1"),
	}, true)
	assert.Regexp(t, "FF21052", err)

	assert.Equal(t, int32(0), counts.writeListener.Load())
	assert.Nil(t, m.listenersByName["L1"])
}

func TestUpdateListenerWriteFailLeavesListenerAlone(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)
	mp := m.persistence.(*persistencemocks.Persistence)

	l1, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	require.NoError(t, err)

	mp.ExpectedCalls = prependExpectation(mp.ExpectedCalls,
		mp.On("WriteListener", m.ctx, mock.Anything).Return(fmt.Errorf("pop")).Once())

	_, err = m.createOrUpdateListener(m.ctx, l1.ID, strPtr("L1"), &apitypes.Listener{StreamID: es.ID, Name: strPtr("L2")}, false)
	assert.Regexp(t, "pop", err)

	// The pre-existing listener must NOT have been torn down, and must keep its original name
	assert.Equal(t, int32(0), counts.listenerRemove.Load())
	assert.Equal(t, l1.ID, m.listenersByName["L1"])
	assert.Nil(t, m.listenersByName["L2"])

	// It is still live - so updating it again does not re-register it with the connector
	_, err = m.createOrUpdateListener(m.ctx, l1.ID, strPtr("L1"), &apitypes.Listener{StreamID: es.ID, Name: strPtr("L2")}, false)
	require.NoError(t, err)
	assert.Equal(t, int32(1), counts.listenerAdd.Load())
	assert.Equal(t, int32(0), counts.listenerRemove.Load())
}

func TestCreateListenerStartFailSelfHeals(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.ExpectedCalls = prependExpectation(mfc.ExpectedCalls,
		mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			counts.listenerAdd.Add(1)
		}).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop")).Once())

	l1, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	assert.Regexp(t, "pop", err)
	assert.Nil(t, l1)

	// It was durably written before we went to the connector, so the name stays reserved against
	// it - re-using the name would collide with the row that is now on disk
	assert.Equal(t, int32(1), counts.writeListener.Load())
	assert.NotNil(t, m.listenersByName["L1"])
	assert.Equal(t, int32(0), counts.listenerRemove.Load())

	// Restarting the stream registers it, with no further API call needed
	req := restartStream(t, m, es)
	require.Len(t, req.InitialListeners, 1)
	assert.Equal(t, "L1", req.InitialListeners[0].Name)
}

func TestRestoreStreamsDuplicateListenerNames(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	initialListeners := atomic.Int32{}

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req := args[1].(*ffcapi.EventStreamStartRequest)
		initialListeners.Add(int32(len(req.InitialListeners)))
	}).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{
		ResolvedSignature: "sig1",
	}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), nil).Maybe()

	falsy := false
	es1 := &apitypes.EventStream{ID: apitypes.NewULID(), Name: strPtr("stream1"), Suspended: &falsy}
	l1 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("dup"), StreamID: es1.ID}
	l2 := &apitypes.Listener{ID: apitypes.NewULID(), Name: strPtr("dup"), StreamID: es1.ID}
	// An unnamed listener (the name column is nullable) is simply skipped by the index
	l3 := &apitypes.Listener{ID: apitypes.NewULID(), StreamID: es1.ID}

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("ListStreamsByCreateTime", m.ctx, (*fftypes.UUID)(nil), startupPaginationLimit, txhandler.SortDirectionAscending).
		Return([]*apitypes.EventStream{es1}, nil).Once()
	mp.On("ListStreamsByCreateTime", m.ctx, es1.ID, startupPaginationLimit, txhandler.SortDirectionAscending).
		Return([]*apitypes.EventStream{}, nil)
	mp.On("ListStreamListenersByCreateTime", m.ctx, (*fftypes.UUID)(nil), 0, txhandler.SortDirectionAscending, es1.ID).
		Return([]*apitypes.Listener{l1, l2, l3}, nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	// Startup must not fail
	err := m.restoreStreams()
	require.NoError(t, err)

	// All three listeners were registered with the connector when the stream started
	assert.Equal(t, int32(3), initialListeners.Load())

	// The first one holds the name, and a new create cannot take it
	assert.Equal(t, l1.ID, m.listenersByName["dup"])
	_, err = m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es1.ID, Name: strPtr("dup")})
	assert.Regexp(t, "FF21098", err)

	mp.AssertExpectations(t)
}

func TestCreateListenerVerifyOptionsFail(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, counts := testStreamWithListenerMocks(t, m)

	badType := apitypes.ListenerType("wrong")
	_, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{
		StreamID: es.ID,
		Name:     strPtr("L1"),
		Type:     &badType,
	})
	assert.Regexp(t, "FF21089", err)

	// We never got as far as taking the name, or touching the connector
	assert.Nil(t, m.listenersByName["L1"])
	assert.Equal(t, int32(0), counts.listenerAdd.Load())
	assert.Equal(t, int32(0), counts.writeListener.Load())
}

func TestDeleteListenerPersistenceFailKeepsName(t *testing.T) {
	_, m, done := newTestManagerMockPersistence(t)
	defer done()

	es, _ := testStreamWithListenerMocks(t, m)
	mp := m.persistence.(*persistencemocks.Persistence)

	l1, err := m.createAndStoreNewListener(m.ctx, &apitypes.Listener{StreamID: es.ID, Name: strPtr("L1")})
	require.NoError(t, err)

	mp.On("GetListener", m.ctx, l1.ID).Return(l1, nil)
	mp.On("DeleteListener", m.ctx, l1.ID).Return(fmt.Errorf("pop"))

	err = m.DeleteListener(m.ctx, es.ID.String(), l1.ID.String())
	assert.Regexp(t, "pop", err)

	// The row is still there, so the name must stay reserved against it
	assert.Equal(t, l1.ID, m.listenersByName["L1"])
}

func restartStream(t *testing.T, m *manager, es *apitypes.EventStream) *ffcapi.EventStreamStartRequest {
	mfc := m.connector.(*ffcapimocks.API)
	started := make(chan *ffcapi.EventStreamStartRequest, 1)
	mfc.ExpectedCalls = prependExpectation(mfc.ExpectedCalls,
		mfc.On("EventStreamStart", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			started <- args[1].(*ffcapi.EventStreamStartRequest)
		}).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil).Once())

	m.mux.Lock()
	s := m.eventStreams[*es.ID]
	m.mux.Unlock()
	require.NoError(t, s.Stop(m.ctx))
	require.NoError(t, s.Start(m.ctx))
	return <-started
}

func prependExpectation(calls []*mock.Call, added *mock.Call) []*mock.Call {
	reordered := make([]*mock.Call, 0, len(calls))
	reordered = append(reordered, added)
	for _, c := range calls {
		if c != added {
			reordered = append(reordered, c)
		}
	}
	return reordered
}

func TestDeleteListenerBadID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	err := m.DeleteListener(m.ctx, "bad ID", "bad ID")
	assert.Regexp(t, "FF00138", err)

}

func TestDeleteListenerStreamNotFound(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	l1 := &apitypes.Listener{ID: apitypes.NewULID(), StreamID: apitypes.NewULID()}
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetListener", m.ctx, mock.Anything).Return(l1, nil)

	err := m.DeleteListener(m.ctx, l1.StreamID.String(), l1.ID.String())
	assert.Regexp(t, "FF21045", err)

	mp.AssertExpectations(t)

}

func TestDeleteListenerFail(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("WriteListener", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerRemove", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	l1, err := m.createOrUpdateListener(m.ctx, apitypes.NewULID(), nil, &apitypes.Listener{StreamID: es.ID}, false)
	assert.NoError(t, err)

	mp.On("GetListener", m.ctx, mock.Anything).Return(l1, nil)

	err = m.DeleteListener(m.ctx, l1.StreamID.String(), l1.ID.String())
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)

}

func TestUpdateStreamBadID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.UpdateStream(m.ctx, "bad ID", &apitypes.EventStream{})
	assert.Regexp(t, "FF00138", err)

}

func TestUpdateStreamNotFound(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.UpdateStream(m.ctx, apitypes.NewULID().String(), &apitypes.EventStream{})
	assert.Regexp(t, "FF21045", err)

}

func TestUpdateStreamBadChanges(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()
	mfc := m.connector.(*ffcapimocks.API)
	mp := m.persistence.(*persistencemocks.Persistence)

	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)

	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	wrongType := apitypes.DistributionMode("wrong")
	_, err = m.UpdateStream(m.ctx, es.ID.String(), &apitypes.EventStream{Type: &wrongType})
	assert.Regexp(t, "FF21029", err)

}

func TestUpdateStreamWriteFail(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()
	mfc := m.connector.(*ffcapimocks.API)
	mp := m.persistence.(*persistencemocks.Persistence)

	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil).Once()
	mp.On("WriteStream", m.ctx, mock.Anything).Return(fmt.Errorf("pop"))
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	_, err = m.UpdateStream(m.ctx, es.ID.String(), &apitypes.EventStream{})
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)

}

func TestGetStreamBadID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.GetStream(m.ctx, "bad ID")
	assert.Regexp(t, "FF00138", err)

}

func TestGetStreamNotFound(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.GetStream(m.ctx, apitypes.NewULID().String())
	assert.Regexp(t, "FF21045", err)

}

func TestGetStreamsBadLimit(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.GetStreamsByCreateTime(m.ctx, "", "wrong")
	assert.Regexp(t, "FF21044", err)

}

func TestGetListenerBadAfter(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.getListeners(m.ctx, "!bad UUID", "")
	assert.Regexp(t, "FF00138", err)

}

func TestGetListenerBadStreamID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.GetListener(m.ctx, "bad ID", apitypes.NewULID().String())
	assert.Regexp(t, "FF00138", err)

}

func TestGetListenerBadListenerID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.GetListener(m.ctx, apitypes.NewULID().String(), "bad ID")
	assert.Regexp(t, "FF00138", err)

}

func TestGetListenerLookupErr(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetListener", m.ctx, mock.Anything).Return(nil, fmt.Errorf("pop"))

	_, err := m.GetListener(m.ctx, apitypes.NewULID().String(), apitypes.NewULID().String())
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)

}

func TestGetListenerNotFound(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetListener", m.ctx, mock.Anything).Return(nil, nil)

	_, err := m.GetListener(m.ctx, apitypes.NewULID().String(), apitypes.NewULID().String())
	assert.Regexp(t, "FF21046", err)

	mp.AssertExpectations(t)

}

func TestGetStreamListenersByCreateTimeBadLimit(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.getStreamListenersByCreateTime(m.ctx, "", "!bad limit", apitypes.NewULID().String())
	assert.Regexp(t, "FF21044", err)

}

func TestGetStreamListenersByCreateTimeBadStreamID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, err := m.getStreamListenersByCreateTime(m.ctx, "", "", "bad ID")
	assert.Regexp(t, "FF00138", err)

}

func TestGetStreamListenersBadStreamID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, _, err := m.getStreamListenersRich(m.ctx, "", nil)
	assert.Regexp(t, "FF00138", err)

}

func TestMergeEthCompatMethods(t *testing.T) {
	l := &apitypes.Listener{
		EthCompatMethods: fftypes.JSONAnyPtr(`[{"method1": "awesomeMethod"}]`),
		Options:          fftypes.JSONAnyPtr(`{"otherOption": "otherValue"}`),
	}
	err := mergeEthCompatMethods(context.Background(), l)
	assert.NoError(t, err)
	b, err := json.Marshal(l.Options)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"methods": [{"method1":"awesomeMethod"}], "signer":true, "otherOption":"otherValue"}`, string(b))
	assert.Nil(t, l.EthCompatMethods)

	l = &apitypes.Listener{
		EthCompatMethods: fftypes.JSONAnyPtr(`[{"method1": "awesomeMethod"}]`),
		Options:          nil,
	}
	err = mergeEthCompatMethods(context.Background(), l)
	assert.NoError(t, err)
	b, err = json.Marshal(l.Options)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"methods": [{"method1":"awesomeMethod"}],"signer":true}`, string(b))
	assert.Nil(t, l.EthCompatMethods)
}

func TestMergeEthCompatMethodsFail(t *testing.T) {
	l := &apitypes.Listener{
		EthCompatMethods: fftypes.JSONAnyPtr(`[{"method1": "awesomeMethod"}`),
		Options:          fftypes.JSONAnyPtr(`{"otherOption": "otherValue"}`),
	}
	err := mergeEthCompatMethods(context.Background(), l)
	assert.Error(t, err)

	l = &apitypes.Listener{
		EthCompatMethods: fftypes.JSONAnyPtr(`[{"method1": "awesomeMethod"}]`),
		Options:          fftypes.JSONAnyPtr(`{"otherOption": "otherValue"`),
	}
	err = mergeEthCompatMethods(context.Background(), l)
	assert.Error(t, err)
}

func TestGetListenerStatusFailStillReturn(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	l1 := &apitypes.Listener{ID: apitypes.NewULID(), StreamID: apitypes.NewULID()}
	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("GetListener", m.ctx, mock.Anything).Return(l1, nil)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventListenerHWM", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop")).Maybe()

	l, err := m.GetListener(m.ctx, l1.StreamID.String(), l1.ID.String())
	assert.NoError(t, err)
	assert.Nil(t, l.Checkpoint)
	assert.False(t, l.Catchup)

	mp.AssertExpectations(t)

}

func TestListStreamsRichNonRichQuery(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, _, err := m.ListStreamsRich(m.ctx, persistence.EventStreamFilters.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF21081", err)

}

func TestListStreamsOK(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	m.richQueryEnabled = true
	mpm := m.persistence.(*persistencemocks.Persistence)
	mrq := &persistencemocks.RichQuery{}
	mpm.On("RichQuery").Return(mrq)
	mrq.On("ListStreams", mock.Anything, mock.Anything, mock.Anything).
		Return([]*apitypes.EventStream{}, (*ffapi.FilterResult)(nil), nil)

	_, _, err := m.ListStreamsRich(m.ctx, persistence.EventStreamFilters.NewFilter(context.Background()).And())
	assert.NoError(t, err)

}

func TestListStreamListenersRichNonRichQuery(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	_, _, err := m.ListStreamListenersRich(m.ctx, apitypes.NewULID().String(), persistence.ListenerFilters.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF21081", err)

}

func TestListStreamListenersBadUUID(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	m.richQueryEnabled = true

	_, _, err := m.ListStreamListenersRich(m.ctx, "!!!", persistence.ListenerFilters.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00138", err)

}

func TestListStreamListenersOK(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	m.richQueryEnabled = true
	mpm := m.persistence.(*persistencemocks.Persistence)
	mrq := &persistencemocks.RichQuery{}
	mpm.On("RichQuery").Return(mrq)
	mrq.On("ListStreamListeners", mock.Anything, mock.Anything, mock.Anything).
		Return([]*apitypes.Listener{}, (*ffapi.FilterResult)(nil), nil)

	_, _, err := m.ListStreamListenersRich(m.ctx, apitypes.NewULID().String(), persistence.ListenerFilters.NewFilter(context.Background()).And())
	assert.NoError(t, err)

}

func TestGetAPIManagedEventStreamBadSpec(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	randName := apitypes.NewULID().String()
	_, _, err := m.GetAPIManagedEventStream(&apitypes.EventStream{
		ID:   apitypes.NewULID(),
		Name: &randName,
	}, []*apitypes.Listener{})
	assert.Regexp(t, "FF21092", err)

}

func TestGetAPIManagedEventStreamRetained(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	randName := apitypes.NewULID().String()
	spec := &apitypes.EventStream{Name: &randName}

	isNew, es1, err := m.GetAPIManagedEventStream(spec, []*apitypes.Listener{})
	assert.NoError(t, err)
	assert.True(t, isNew, err)

	isNew, es2, err := m.GetAPIManagedEventStream(spec, []*apitypes.Listener{})
	assert.NoError(t, err)
	assert.False(t, isNew, err)
	assert.Same(t, es1, es2)

	err = m.CleanupAPIManagedEventStream(*spec.Name)
	require.NoError(t, err)

}
