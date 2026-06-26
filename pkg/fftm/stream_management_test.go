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

	_, err := m.createOrUpdateListener(m.ctx, apitypes.NewULID(), &apitypes.Listener{StreamID: apitypes.NewULID()}, false)
	assert.Regexp(t, "FF21045", err)

}

func TestCreateOrUpdateListenerFail(t *testing.T) {
	_, m, close := newTestManagerMockPersistence(t)
	defer close()

	mp := m.persistence.(*persistencemocks.Persistence)
	mp.On("WriteStream", m.ctx, mock.Anything).Return(nil)
	mp.On("GetCheckpoint", m.ctx, mock.Anything).Return(nil, nil)

	mfc := m.connector.(*ffcapimocks.API)
	mfc.On("EventStreamStart", mock.Anything, mock.Anything).Return(&ffcapi.EventStreamStartResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerVerifyOptions", mock.Anything, mock.Anything).Return(&ffcapi.EventListenerVerifyOptionsResponse{}, ffcapi.ErrorReason(""), nil)
	mfc.On("EventListenerAdd", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	_, err = m.createOrUpdateListener(m.ctx, apitypes.NewULID(), &apitypes.Listener{StreamID: es.ID}, false)
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

	es, err := m.CreateAndStoreNewStream(m.ctx, &apitypes.EventStream{Name: strPtr("stream1")})

	l := &apitypes.Listener{
		StreamID:         es.ID,
		EthCompatMethods: fftypes.JSONAnyPtr(`{}`),
	}

	_, err = m.createOrUpdateListener(m.ctx, apitypes.NewULID(), l, false)
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

	_, err = m.createOrUpdateListener(m.ctx, apitypes.NewULID(), &apitypes.Listener{StreamID: es.ID}, false)
	assert.Regexp(t, "pop", err)

	mp.AssertExpectations(t)
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

	l1, err := m.createOrUpdateListener(m.ctx, apitypes.NewULID(), &apitypes.Listener{StreamID: es.ID}, false)
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
