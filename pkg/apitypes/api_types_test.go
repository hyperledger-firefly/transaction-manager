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

package apitypes

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
)

func TestCheckUpdateString(t *testing.T) {
	var val1 = "val1"
	var val2 = "val2"
	var pVal3 *string

	changed := CheckUpdateString(false, &pVal3, nil, nil, "defVal")
	assert.Equal(t, "defVal", *pVal3) // the default won
	assert.True(t, changed)

	changed = CheckUpdateString(false, &pVal3, &val1, &val2, "differentDefault")
	assert.Equal(t, "val2", *pVal3) // val2 won
	assert.True(t, changed)

	changed = CheckUpdateString(true, &pVal3, &val2, &val2, "differentDefault")
	assert.Equal(t, "val2", *pVal3)
	assert.True(t, changed) // because it was already changed

	changed = CheckUpdateString(false, &pVal3, &val2, &val2, "differentDefault")
	assert.Equal(t, "val2", *pVal3)
	assert.False(t, changed) // the value hasn't changed

	changed = CheckUpdateString(false, &pVal3, &val1, nil, "differentDefault")
	assert.Equal(t, "val1", *pVal3) // val1 won
	assert.False(t, changed)        // which was the current value
}

func TestCheckUpdateBool(t *testing.T) {
	var val1 = true
	var val2 = false
	var pVal3 *bool

	changed := CheckUpdateBool(false, &pVal3, nil, nil, true)
	assert.Equal(t, true, *pVal3) // the default won
	assert.True(t, changed)

	changed = CheckUpdateBool(false, &pVal3, &val1, &val2, false)
	assert.Equal(t, false, *pVal3) // val2 won
	assert.True(t, changed)

	changed = CheckUpdateBool(true, &pVal3, &val2, &val2, false)
	assert.Equal(t, false, *pVal3)
	assert.True(t, changed) // because it was already changed

	changed = CheckUpdateBool(false, &pVal3, &val2, &val2, false)
	assert.Equal(t, false, *pVal3)
	assert.False(t, changed) // the value hasn't changed

	changed = CheckUpdateBool(false, &pVal3, &val1, nil, false)
	assert.Equal(t, true, *pVal3) // val1 won
	assert.False(t, changed)      // which was the current value
}

func TestCheckUpdateInt64(t *testing.T) {
	var val1 uint64 = 1111
	var val2 uint64 = 2222
	var pVal3 *uint64

	changed := CheckUpdateUint64(false, &pVal3, nil, nil, 3333)
	assert.Equal(t, uint64(3333), *pVal3) // the default won
	assert.True(t, changed)

	changed = CheckUpdateUint64(false, &pVal3, &val1, &val2, 4444)
	assert.Equal(t, uint64(2222), *pVal3) // val2 won
	assert.True(t, changed)

	changed = CheckUpdateUint64(true, &pVal3, &val2, &val2, 4444)
	assert.Equal(t, uint64(2222), *pVal3)
	assert.True(t, changed) // because it was already changed

	changed = CheckUpdateUint64(false, &pVal3, &val2, &val2, 4444)
	assert.Equal(t, uint64(2222), *pVal3)
	assert.False(t, changed) // the value hasn't changed

	changed = CheckUpdateUint64(false, &pVal3, &val1, nil, 4444)
	assert.Equal(t, uint64(1111), *pVal3) // val1 won
	assert.False(t, changed)              // which was the current value
}

func TestCheckUpdateDuration(t *testing.T) {
	var val1 fftypes.FFDuration = fftypes.FFDuration(1111 * time.Second)
	var val2 fftypes.FFDuration = fftypes.FFDuration(2222 * time.Second)
	var pVal3 *fftypes.FFDuration

	changed := CheckUpdateDuration(false, &pVal3, nil, nil, fftypes.FFDuration(3333*time.Second))
	assert.Equal(t, fftypes.FFDuration(3333*time.Second), *pVal3) // the default won
	assert.True(t, changed)

	changed = CheckUpdateDuration(false, &pVal3, &val1, &val2, fftypes.FFDuration(4444*time.Second))
	assert.Equal(t, fftypes.FFDuration(2222*time.Second), *pVal3) // val2 won
	assert.True(t, changed)

	changed = CheckUpdateDuration(true, &pVal3, &val2, &val2, fftypes.FFDuration(4444*time.Second))
	assert.Equal(t, fftypes.FFDuration(2222*time.Second), *pVal3)
	assert.True(t, changed) // because it was already changed

	changed = CheckUpdateDuration(false, &pVal3, &val2, &val2, fftypes.FFDuration(4444*time.Second))
	assert.Equal(t, fftypes.FFDuration(2222*time.Second), *pVal3)
	assert.False(t, changed) // the value hasn't changed

	changed = CheckUpdateDuration(false, &pVal3, &val1, nil, fftypes.FFDuration(4444*time.Second))
	assert.Equal(t, fftypes.FFDuration(1111*time.Second), *pVal3) // val1 won
	assert.False(t, changed)                                      // which was the current value
}

func TestCheckUpdateEnum(t *testing.T) {
	var val1 fftypes.FFEnum = fftypes.FFEnum("val1")
	var val2 fftypes.FFEnum = fftypes.FFEnum("val2")
	var pVal3 *fftypes.FFEnum

	changed := CheckUpdateEnum(false, &pVal3, nil, nil, fftypes.FFEnum("def1"))
	assert.Equal(t, fftypes.FFEnum("def1"), *pVal3) // the default won
	assert.True(t, changed)

	changed = CheckUpdateEnum(false, &pVal3, &val1, &val2, fftypes.FFEnum("def2"))
	assert.Equal(t, fftypes.FFEnum("val2"), *pVal3) // val2 won
	assert.True(t, changed)

	changed = CheckUpdateEnum(true, &pVal3, &val2, &val2, fftypes.FFEnum("def2"))
	assert.Equal(t, fftypes.FFEnum("val2"), *pVal3)
	assert.True(t, changed) // because it was already changed

	changed = CheckUpdateEnum(false, &pVal3, &val2, &val2, fftypes.FFEnum("def2"))
	assert.Equal(t, fftypes.FFEnum("val2"), *pVal3)
	assert.False(t, changed) // the value hasn't changed

	changed = CheckUpdateEnum(false, &pVal3, &val1, nil, fftypes.FFEnum("def2"))
	assert.Equal(t, fftypes.FFEnum("val1"), *pVal3) // val1 won
	assert.False(t, changed)                        // which was the current value
}

func TestCheckUpdateStringMap(t *testing.T) {
	val1 := map[string]string{"key1": "val1"}
	val2 := map[string]string{"key2": "val2"}
	var pVal3 map[string]string

	changed := CheckUpdateStringMap(false, &pVal3, val1, val2)
	assert.Equal(t, map[string]string{"key2": "val2"}, pVal3) // val2 won
	assert.True(t, changed)

	changed = CheckUpdateStringMap(true, &pVal3, val2, val2)
	assert.Equal(t, map[string]string{"key2": "val2"}, pVal3)
	assert.True(t, changed) // because it was already changed

	changed = CheckUpdateStringMap(false, &pVal3, val2, val2)
	assert.Equal(t, map[string]string{"key2": "val2"}, pVal3)
	assert.False(t, changed) // the value hasn't changed

	changed = CheckUpdateStringMap(false, &pVal3, val1, nil)
	assert.Equal(t, map[string]string{"key1": "val1"}, pVal3) // val1 won
	assert.False(t, changed)                                  // which was the current value
}

func TestMarshalUnmarshalEventOK(t *testing.T) {

	type customInfo struct {
		InfoKey1 string `json:"key1"`
	}

	e := &EventWithContext{
		StandardContext: EventContext{
			StreamID:       NewULID(),
			ListenerName:   "listener1",
			EthCompatSubID: NewULID(),
			ListenerType:   ListenerTypeEvents,
		},
		Event: &ffcapi.Event{
			ID: ffcapi.EventID{
				ListenerID:       fftypes.NewUUID(),
				BlockHash:        "0x12345",
				BlockNumber:      12345,
				TransactionHash:  "0x23456",
				TransactionIndex: 10,
				LogIndex:         1,
				Signature:        "ev()",
			},
			Info: &customInfo{
				InfoKey1: "val1",
			},
			Data: fftypes.JSONAnyPtr(`{"dk1":"dv1"}`),
		},
	}

	b, err := json.Marshal(&e)
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"blockHash":"0x12345",
		"blockNumber":"12345",
		"data": {"dk1":"dv1"},
		"key1":"val1",
		"listenerId":"`+e.Event.ID.ListenerID.String()+`",
		"listenerName":"listener1",
		"listenerType": "events",
		"logIndex":"1",
		"signature":"ev()",
		"subId":"`+e.StandardContext.EthCompatSubID.String()+`",
		"streamId":"`+e.StandardContext.StreamID.String()+`",
		"transactionHash":"0x23456",
		"transactionIndex":"10"
	}`, string(b))

	var e2 *EventWithContext
	err = json.Unmarshal(b, &e2)
	assert.NoError(t, err)

	assert.Equal(t, e.Event.ID.ListenerID, e2.Event.ID.ListenerID)
	assert.Equal(t, e.StandardContext.StreamID, e2.StandardContext.StreamID)
	assert.Equal(t, e.Event.Data, e2.Event.Data)
	assert.Equal(t, "val1", e2.Event.Info.(fftypes.JSONObject).GetString("key1"))

}

func TestMarshalUnmarshalBlockEventOK(t *testing.T) {

	type customInfo struct {
		InfoKey1 string `json:"key1"`
	}

	e := &EventWithContext{
		StandardContext: EventContext{
			StreamID:       NewULID(),
			ListenerName:   "listener1",
			EthCompatSubID: NewULID(),
			ListenerType:   ListenerTypeBlocks,
		},
		BlockEvent: &ffcapi.BlockEvent{
			ListenerID: fftypes.NewUUID(),
			BlockInfo: ffcapi.BlockInfo{
				BlockHash:         "0x12345",
				BlockNumber:       fftypes.NewFFBigInt(12345),
				ParentHash:        "0x23456",
				TransactionHashes: []string{},
			},
		},
	}

	b, err := json.Marshal(&e)
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"blockHash":"0x12345",
		"parentHash": "0x23456",
		"blockNumber":"12345",
		"listenerId":"`+e.BlockEvent.ListenerID.String()+`",
		"listenerName":"listener1",
		"listenerType": "blocks",
		"transactionHashes": [],
		"subId":"`+e.StandardContext.EthCompatSubID.String()+`",
		"streamId":"`+e.StandardContext.StreamID.String()+`"
	}`, string(b))

	var e2 *EventWithContext
	err = json.Unmarshal(b, &e2)
	assert.NoError(t, err)

	assert.Equal(t, e.BlockEvent.ListenerID, e2.BlockEvent.ListenerID)
	assert.Equal(t, e.StandardContext.StreamID, e2.StandardContext.StreamID)

}

func TestMarshalUnmarshalEmptyInfoOk(t *testing.T) {

	e := &EventWithContext{}

	_, err := json.Marshal(&e)
	assert.NoError(t, err)

}

func TestUnmarshalFail(t *testing.T) {

	e := &EventWithContext{}

	err := json.Unmarshal([]byte(`!bad JSON`), &e)
	assert.Error(t, err)

}

func TestEventStreamCheckpoint(t *testing.T) {

	cp := &EventStreamCheckpoint{
		StreamID: fftypes.NewUUID(),
		Listeners: CheckpointListeners{
			*fftypes.NewUUID(): json.RawMessage([]byte(`{"some":"data"}`)),
		},
	}
	assert.Equal(t, cp.StreamID.String(), cp.GetID())
	t1 := fftypes.Now()
	cp.SetCreated(t1)
	assert.Equal(t, t1, cp.FirstCheckpoint)
	t2 := fftypes.Now()
	cp.SetUpdated(t2)
	assert.Equal(t, t2, cp.Time)

	v, err := cp.Listeners.Value()
	assert.NoError(t, err)

	cp1 := &EventStreamCheckpoint{}
	err = cp1.Listeners.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, cp.Listeners, cp1.Listeners)

	cp1 = &EventStreamCheckpoint{}
	err = cp1.Listeners.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, cp1.Listeners)

	cp1 = &EventStreamCheckpoint{}
	err = cp1.Listeners.Scan(12345)
	assert.Regexp(t, "FF21082", err)

}

func TestConfirmationFromBlock(t *testing.T) {

	bi := &BlockInfo{
		BlockNumber: fftypes.FFuint64(12345),
		BlockHash:   "hash1",
		ParentHash:  "parent1",
	}
	c := ConfirmationFromBlock(bi)
	assert.Equal(t, bi.BlockNumber, c.BlockNumber)
	assert.Equal(t, bi.BlockHash, c.BlockHash)
	assert.Equal(t, bi.ParentHash, c.ParentHash)

}

func TestEventStreamFields(t *testing.T) {

	es := &EventStream{
		ID: fftypes.NewUUID(),
	}
	assert.Equal(t, es.ID.String(), es.GetID())
	t1 := fftypes.Now()
	es.SetCreated(t1)
	assert.Equal(t, t1, es.Created)
	t2 := fftypes.Now()
	es.SetUpdated(t2)
	assert.Equal(t, t2, es.Updated)
}

func TestWebSocketConfigSerialization(t *testing.T) {

	var wc *WebSocketConfig
	v, err := wc.Value()
	assert.Nil(t, v)
	assert.NoError(t, err)

	wc = &WebSocketConfig{
		DistributionMode: &DistributionModeBroadcast,
	}
	v, err = wc.Value()
	assert.NotNil(t, v)
	assert.NoError(t, err)

	wc1 := &WebSocketConfig{}
	err = wc1.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, DistributionModeBroadcast, *wc1.DistributionMode)

	wc2 := &WebSocketConfig{}
	err = wc2.Scan(string(v.([]byte)))
	assert.NoError(t, err)
	assert.Equal(t, DistributionModeBroadcast, *wc1.DistributionMode)

	var wc3 *WebSocketConfig
	err = wc3.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, wc3)

}

func TestWebhookConfigSerialization(t *testing.T) {

	var wc *WebhookConfig
	v, err := wc.Value()
	assert.Nil(t, v)
	assert.NoError(t, err)

	u := "http://example.com"
	wc = &WebhookConfig{
		URL: &u,
	}
	v, err = wc.Value()
	assert.NotNil(t, v)
	assert.NoError(t, err)

	wc1 := &WebhookConfig{}
	err = wc1.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", *wc1.URL)

	wc2 := &WebhookConfig{}
	err = wc2.Scan(string(v.([]byte)))
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", *wc1.URL)

	var wc3 *WebhookConfig
	err = wc3.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, wc3)

}

func TestListenerFields(t *testing.T) {

	l := &Listener{
		ID: fftypes.NewUUID(),
	}
	assert.Equal(t, l.ID.String(), l.GetID())
	t1 := fftypes.Now()
	l.SetCreated(t1)
	assert.Equal(t, t1, l.Created)
	t2 := fftypes.Now()
	l.SetUpdated(t2)
	assert.Equal(t, t2, l.Updated)

	assert.Empty(t, l.SignatureString())
	sv := "sig"
	l.Signature = &sv
	assert.Equal(t, "sig", l.SignatureString())

	v, err := l.Filters.Value()
	assert.NoError(t, err)
	assert.Nil(t, v)

	l.Filters = ListenerFilters{
		*fftypes.JSONAnyPtr(`{"some":"data"}`),
	}
	v, err = l.Filters.Value()
	assert.NoError(t, err)
	assert.NotEmpty(t, v, v.([]byte))

	l2 := &Listener{Filters: ListenerFilters{}}
	err = l2.Filters.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, l.Filters, l2.Filters)

}
