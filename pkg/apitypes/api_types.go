// Copyright © 2025 Kaleido, Inc.
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
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"

	"github.com/hyperledger/firefly-common/pkg/dbsql"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/jsonmap"
	"github.com/hyperledger/firefly-transaction-manager/internal/tmmsgs"
	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
)

type DistributionMode = fftypes.FFEnum

var (
	DistributionModeBroadcast   = fftypes.FFEnumValue("distmode", "broadcast")
	DistributionModeLoadBalance = fftypes.FFEnumValue("distmode", "load_balance")
)

type EventStreamType = fftypes.FFEnum

var (
	EventStreamTypeWebhook   = fftypes.FFEnumValue("estype", "webhook")
	EventStreamTypeWebSocket = fftypes.FFEnumValue("estype", "websocket")
)

type ErrorHandlingType = fftypes.FFEnum

var (
	ErrorHandlingTypeBlock = fftypes.FFEnumValue("ehtype", "block")
	ErrorHandlingTypeSkip  = fftypes.FFEnumValue("ehtype", "skip")
)

type EventStream struct {
	ID        *fftypes.UUID    `ffstruct:"eventstream" json:"id"`
	Created   *fftypes.FFTime  `ffstruct:"eventstream" json:"created"`
	Updated   *fftypes.FFTime  `ffstruct:"eventstream" json:"updated"`
	Name      *string          `ffstruct:"eventstream" json:"name,omitempty"`
	Suspended *bool            `ffstruct:"eventstream" json:"suspended,omitempty"`
	Type      *EventStreamType `ffstruct:"eventstream" json:"type,omitempty" ffenum:"estype"`

	ErrorHandling     *ErrorHandlingType  `ffstruct:"eventstream" json:"errorHandling"`
	BatchSize         *uint64             `ffstruct:"eventstream" json:"batchSize"`
	BatchTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"batchTimeout"`
	RetryTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"retryTimeout"`
	BlockedRetryDelay *fftypes.FFDuration `ffstruct:"eventstream" json:"blockedRetryDelay"`

	EthCompatBatchTimeoutMS       *uint64 `ffstruct:"eventstream" json:"batchTimeoutMS,omitempty"`       // input only, for backwards compatibility
	EthCompatRetryTimeoutSec      *uint64 `ffstruct:"eventstream" json:"retryTimeoutSec,omitempty"`      // input only, for backwards compatibility
	EthCompatBlockedRetryDelaySec *uint64 `ffstruct:"eventstream" json:"blockedRetryDelaySec,omitempty"` // input only, for backwards compatibility

	Webhook   *WebhookConfig   `ffstruct:"eventstream" json:"webhook,omitempty"`
	WebSocket *WebSocketConfig `ffstruct:"eventstream" json:"websocket,omitempty"`
}

func (es *EventStream) GetID() string {
	return es.ID.String()
}

func (es *EventStream) SetCreated(t *fftypes.FFTime) {
	es.Created = t
}

func (es *EventStream) SetUpdated(t *fftypes.FFTime) {
	es.Updated = t
}

type EventStreamStatus string

const (
	EventStreamStatusStarted  EventStreamStatus = "started"
	EventStreamStatusStopping EventStreamStatus = "stopping"
	EventStreamStatusStopped  EventStreamStatus = "stopped"
	EventStreamStatusDeleted  EventStreamStatus = "deleted"
)

type EventStreamWithStatus struct {
	EventStream
	Status EventStreamStatus `ffstruct:"eventstream" json:"status"`
}

type CheckpointListeners map[fftypes.UUID]json.RawMessage

type EventStreamCheckpoint struct {
	StreamID        *fftypes.UUID       `json:"streamId"`
	Time            *fftypes.FFTime     `json:"time"`            // this is the latest checkpoint time
	FirstCheckpoint *fftypes.FFTime     `json:"firstCheckpoint"` // this is the initial create time
	Listeners       CheckpointListeners `json:"listeners"`
}

func jsonScan(src, target interface{}) error {
	switch src := src.(type) {
	case string:
		return json.Unmarshal([]byte(src), target)
	case []byte:
		return json.Unmarshal(src, target)
	case nil:
		return nil
	default:
		return i18n.NewError(context.Background(), tmmsgs.MsgSQLScanFailed, src)
	}
}

func jsonValue(target interface{}) (driver.Value, error) {
	return json.Marshal(target)
}

func (l *CheckpointListeners) Scan(val any) error {
	return jsonScan(val, l)
}

func (l CheckpointListeners) Value() (driver.Value, error) {
	return jsonValue(l)
}

func (cp *EventStreamCheckpoint) GetID() string {
	return cp.StreamID.String()
}

func (cp *EventStreamCheckpoint) SetCreated(t *fftypes.FFTime) {
	cp.FirstCheckpoint = t
}

func (cp *EventStreamCheckpoint) SetUpdated(t *fftypes.FFTime) {
	cp.Time = t
}

type WebhookConfig struct {
	URL                        *string             `ffstruct:"whconfig" json:"url,omitempty"`
	Headers                    map[string]string   `ffstruct:"whconfig" json:"headers,omitempty"`
	TLSkipHostVerify           *bool               `ffstruct:"whconfig" json:"tlsSkipHostVerify,omitempty"`
	RequestTimeout             *fftypes.FFDuration `ffstruct:"whconfig" json:"requestTimeout,omitempty"`
	EthCompatRequestTimeoutSec *int64              `ffstruct:"whconfig" json:"requestTimeoutSec,omitempty"` // input only, for backwards compatibility
}

// Store in DB as JSON
func (wc *WebhookConfig) Scan(src interface{}) error {
	return jsonScan(src, wc)
}

// Store in DB as JSON
func (wc *WebhookConfig) Value() (driver.Value, error) {
	if wc == nil {
		return nil, nil
	}
	return jsonValue(wc)
}

type WebSocketConfig struct {
	DistributionMode *DistributionMode `ffstruct:"wsconfig" json:"distributionMode,omitempty"`
}

// Store in DB as JSON
func (wc *WebSocketConfig) Scan(src interface{}) error {
	return jsonScan(src, wc)
}

// Store in DB as JSON
func (wc *WebSocketConfig) Value() (driver.Value, error) {
	if wc == nil {
		return nil, nil
	}
	return jsonValue(wc)
}

type ListenerFilters []fftypes.JSONAny

// Store in DB as JSON
func (lf *ListenerFilters) Scan(src interface{}) error {
	return jsonScan(src, lf)
}

// Store in DB as JSON
func (lf ListenerFilters) Value() (driver.Value, error) {
	if lf == nil {
		return nil, nil
	}
	return jsonValue(lf)
}

type ListenerType = fftypes.FFEnum

var (
	ListenerTypeEvents = fftypes.FFEnumValue("fftm_listener_type", "events")
	ListenerTypeBlocks = fftypes.FFEnumValue("fftm_listener_type", "blocks")
)

type Listener struct {
	ID               *fftypes.UUID    `ffstruct:"listener" json:"id,omitempty"`
	Created          *fftypes.FFTime  `ffstruct:"listener" json:"created"`
	Updated          *fftypes.FFTime  `ffstruct:"listener" json:"updated"`
	Type             *ListenerType    `ffstruct:"listener" json:"type" ffenum:"fftm_listener_type"`
	Name             *string          `ffstruct:"listener" json:"name"`
	StreamID         *fftypes.UUID    `ffstruct:"listener" json:"stream" ffexcludeoutput:"true"`
	EthCompatAddress *string          `ffstruct:"listener" json:"address,omitempty"`
	EthCompatEvent   *fftypes.JSONAny `ffstruct:"listener" json:"event,omitempty"`
	EthCompatMethods *fftypes.JSONAny `ffstruct:"listener" json:"methods,omitempty"`
	Filters          ListenerFilters  `ffstruct:"listener" json:"filters"`
	Options          *fftypes.JSONAny `ffstruct:"listener" json:"options"`
	Signature        *string          `ffstruct:"listener" json:"signature,omitempty" ffexcludeinput:"true"`
	FromBlock        *string          `ffstruct:"listener" json:"fromBlock,omitempty"`
}

func (l *Listener) SignatureString() string {
	if l.Signature == nil {
		return ""
	}
	return *l.Signature
}

func (l *Listener) GetID() string {
	return l.ID.String()
}

func (l *Listener) SetCreated(t *fftypes.FFTime) {
	l.Created = t
}

func (l *Listener) SetUpdated(t *fftypes.FFTime) {
	l.Updated = t
}

type ListenerWithStatus struct {
	Listener
	ffcapi.EventListenerHWMResponse
}

type LiveStatus struct {
	ffcapi.LiveResponse
}

type ReadyStatus struct {
	ffcapi.ReadyResponse
}

type LiveAddressBalance struct {
	ffcapi.AddressBalanceResponse
}

type LiveGasPrice struct {
	ffcapi.GasPriceEstimateResponse
}

// CheckUpdateString helper merges supplied configuration, with a base, and applies a default if unset
func CheckUpdateString(changed bool, merged **string, oldValue *string, newValue *string, defValue string) bool {
	if newValue != nil {
		*merged = newValue
	} else {
		*merged = oldValue
	}
	if *merged == nil {
		v := defValue
		*merged = &v
		return true
	}
	return changed || oldValue == nil || *oldValue != **merged
}

// CheckUpdateBool helper merges supplied configuration, with a base, and applies a default if unset
func CheckUpdateBool(changed bool, merged **bool, oldValue *bool, newValue *bool, defValue bool) bool {
	if newValue != nil {
		*merged = newValue
	} else {
		*merged = oldValue
	}
	if *merged == nil {
		v := defValue
		*merged = &v
		return true
	}
	return changed || oldValue == nil || *oldValue != **merged
}

// CheckUpdateUint64 helper merges supplied configuration, with a base, and applies a default if unset
func CheckUpdateUint64(changed bool, merged **uint64, oldValue *uint64, newValue *uint64, defValue int64) bool {
	if newValue != nil {
		*merged = newValue
	} else {
		*merged = oldValue
	}
	if *merged == nil {
		//nolint:gosec
		v := uint64(defValue)
		*merged = &v
		return true
	}
	return changed || oldValue == nil || *oldValue != **merged
}

// CheckUpdateDuration helper merges supplied configuration, with a base, and applies a default if unset
func CheckUpdateDuration(changed bool, merged **fftypes.FFDuration, oldValue *fftypes.FFDuration, newValue *fftypes.FFDuration, defValue fftypes.FFDuration) bool {
	if newValue != nil {
		*merged = newValue
	} else {
		*merged = oldValue
	}
	if *merged == nil {
		v := defValue
		*merged = &v
		return true
	}
	return changed || oldValue == nil || *oldValue != **merged
}

// CheckUpdateEnum helper merges supplied configuration, with a base, and applies a default if unset
func CheckUpdateEnum(changed bool, merged **fftypes.FFEnum, oldValue *fftypes.FFEnum, newValue *fftypes.FFEnum, defValue fftypes.FFEnum) bool {
	if newValue != nil {
		*merged = newValue
	} else {
		*merged = oldValue
	}
	if *merged == nil {
		v := defValue
		*merged = &v
		return true
	}
	return changed || oldValue == nil || *oldValue != **merged
}

// CheckUpdateStringMap helper merges supplied configuration, with a base, and applies a default if unset
func CheckUpdateStringMap(changed bool, merged *map[string]string, oldValue map[string]string, newValue map[string]string) bool {
	if newValue != nil {
		*merged = newValue
		changed = changed || (oldValue == nil)
	} else {
		*merged = oldValue
		return false // new was nil, we cannot have changed
	}
	if changed {
		return true
	}
	// We need to compare otherwise
	jsonOld, _ := json.Marshal(oldValue)
	jsonNew, _ := json.Marshal(newValue)
	return !bytes.Equal(jsonOld, jsonNew)
}

type EventContext struct {
	StreamID       *fftypes.UUID `json:"streamId,omitempty"`     // the ID of the event stream for this event
	EthCompatSubID *fftypes.UUID `json:"subId,omitempty"`        // ID of the listener - EthCompat "subscription" naming
	ListenerName   string        `json:"listenerName,omitempty"` // name of the listener
	ListenerType   ListenerType  `json:"listenerType,omitempty"`
}

type EventBatch struct {
	BatchNumber int64               `json:"batchNumber"`
	Events      []*EventWithContext `json:"events"`
}

type EventBatchWithConfirm struct {
	EventBatch
	Confirm chan error
}

// EventWithContext is what is delivered
// There is custom serialization to flatten the whole structure, so all the custom `info` fields from the
// connector are alongside the required context fields.
// The `data` is kept separate
type EventWithContext struct {
	StandardContext EventContext
	Event           *ffcapi.Event
	BlockEvent      *ffcapi.BlockEvent
}

func (e *EventWithContext) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	if e.Event != nil {
		if e.Event.Info != nil {
			jsonmap.AddJSONFieldsToMap(reflect.ValueOf(e.Event.Info), m)
		}
		jsonmap.AddJSONFieldsToMap(reflect.ValueOf(&e.Event.ID), m)
		m["data"] = e.Event.Data
	} else if e.BlockEvent != nil {
		jsonmap.AddJSONFieldsToMap(reflect.ValueOf(&e.BlockEvent), m)
	}
	jsonmap.AddJSONFieldsToMap(reflect.ValueOf(&e.StandardContext), m)
	return json.Marshal(m)
}

// Note on unmarshal info will be a map with all the fields (except "data")
func (e *EventWithContext) UnmarshalJSON(b []byte) error {
	var m fftypes.JSONObject
	err := json.Unmarshal(b, &m)
	if err == nil && m != nil {
		if m.GetString("listenerType") == string(ListenerTypeBlocks) {
			e.BlockEvent = &ffcapi.BlockEvent{}
			err = json.Unmarshal(b, &e.BlockEvent)
		} else {
			e.Event = &ffcapi.Event{Info: m}
			data := m["data"]
			delete(m, "data")
			if data != nil {
				b, _ := json.Marshal(&data)
				e.Event.Data = fftypes.JSONAnyPtrBytes(b)
			}
			err = json.Unmarshal(b, &e.Event.ID)
		}
		if err == nil {
			err = json.Unmarshal(b, &e.StandardContext)
		}
	}
	return err
}

func ConfirmationFromBlock(block *BlockInfo) *Confirmation {
	return &Confirmation{
		BlockNumber: block.BlockNumber,
		BlockHash:   block.BlockHash,
		ParentHash:  block.ParentHash,
	}
}

type ConfirmationsNotification struct {
	// Confirmed marks we've reached the confirmation threshold
	Confirmed bool
	// NewFork is true when NewConfirmations is a complete list of confirmations.
	// Otherwise, Confirmations is an additive delta on top of a previous list of confirmations.
	NewFork bool
	// Confirmations is the list of confirmations being notified - assured to be non-nil.
	// The list will only be populated if chainTrackingMode is set to full.
	Confirmations []*Confirmation
	// CurrentConfirmationCount is the actual number of confirmations for the transaction
	CurrentConfirmationCount uint64 `json:"currentConfirmationCount"`

	// TargetConfirmationCount is the target number of confirmations to confirm the transaction
	TargetConfirmationCount uint64 `json:"targetConfirmationCount"`
}

type Confirmation struct {
	BlockNumber fftypes.FFuint64 `json:"blockNumber"`
	BlockHash   string           `json:"blockHash"`
	ParentHash  string           `json:"parentHash"`
}

type ConfirmationRecord struct {
	dbsql.ResourceBase        // default persistence headers for this micro object
	TransactionID      string `json:"transaction"` // owning transaction
	*Confirmation
}
