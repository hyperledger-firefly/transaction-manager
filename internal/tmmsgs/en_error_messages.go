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

package tmmsgs

import (
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"golang.org/x/text/language"
)

var ffe = func(key, translation string, statusHint ...int) i18n.ErrorMessageKey {
	return i18n.FFE(language.AmericanEnglish, key, translation, statusHint...)
}

//revive:disable
var (
	MsgInvalidOutputType                       = ffe("FF21010", "Invalid output type: %s")
	MsgConnectorError                          = ffe("FF21012", "Connector failed request. requestId=%s reason=%s error: %s")
	MsgConnectorInvalidContentType             = ffe("FF21013", "Connector failed request. requestId=%s invalid response content type: %s")
	MsgInvalidConfirmationRequest              = ffe("FF21016", "Invalid confirmation request %+v")
	MsgCoreError                               = ffe("FF21017", "Error from core status=%d: %s")
	MsgConfigParamNotSet                       = ffe("FF21018", "Configuration parameter '%s' must be set")
	DeprecatedMsgPolicyEngineNotRegistered     = ffe("FF21019", "(Deprecated) No policy engine registered with name '%s'")                // deprecated
	DeprecatedMsgNoGasConfigSetForPolicyEngine = ffe("FF21020", "(Deprecated) A fixed gas price must be set when not using a gas oracle") // deprecated
	MsgErrorQueryingGasOracleAPI               = ffe("FF21021", "Error from gas station API [%d]: %s")
	MsgInvalidRequestErr                       = ffe("FF21022", "Invalid '%s' request: %s", http.StatusBadRequest)
	MsgUnsupportedRequestType                  = ffe("FF21023", "Unsupported request type: %s", http.StatusBadRequest)
	MsgMissingGOTemplate                       = ffe("FF21024", "Missing template for processing response from Gas Oracle REST API")
	MsgBadGOTemplate                           = ffe("FF21025", "Invalid Go template: %s")
	MsgGasOracleResultError                    = ffe("FF21026", "Error processing result from gas station API via template")
	MsgStreamStateError                        = ffe("FF21027", "Event stream is in %s state", http.StatusConflict)
	MsgMissingName                             = ffe("FF21028", "Name is required", http.StatusBadRequest)
	MsgInvalidStreamType                       = ffe("FF21029", "Invalid event stream type '%s'", http.StatusBadRequest)
	MsgMissingWebhookURL                       = ffe("FF21030", "'url' is required for webhook configuration", http.StatusBadRequest)
	MsgStopFailedUpdatingESConfig              = ffe("FF21031", "Failed to stop event stream to apply updated configuration: %s")
	MsgStartFailedUpdatingESConfig             = ffe("FF21032", "Failed to restart event stream while applying updated configuration: %s")
	MsgBlockWebhookAddress                     = ffe("FF21033", "Cannot send Webhook POST to address '%s' for host '%s'")
	MsgInvalidDistributionMode                 = ffe("FF21034", "Invalid distribution mode for WebSocket: %s", http.StatusBadRequest)
	MsgWebhookFailedStatus                     = ffe("FF21035", "Webhook request failed with status %d")
	MsgWSErrorFromClient                       = ffe("FF21036", "Error received from WebSocket client: %s")
	MsgWebSocketClosed                         = ffe("FF21037", "WebSocket '%s' closed")
	MsgWebSocketInterruptedSend                = ffe("FF21038", "Interrupted waiting for WebSocket connection to send event")
	MsgWebSocketInterruptedReceive             = ffe("FF21039", "Interrupted waiting for WebSocket acknowledgment")
	MsgBadListenerOptions                      = ffe("FF21040", "Invalid listener options: %s", http.StatusBadRequest)
	MsgInvalidHost                             = ffe("FF21041", "Cannot send Webhook POST to host '%s': %s")
	MsgWebhookErr                              = ffe("FF21042", "Webhook request failed: %s")
	MsgUnknownPersistence                      = ffe("FF21043", "Unknown persistence type '%s'")
	MsgInvalidLimit                            = ffe("FF21044", "Invalid limit string '%s': %s")
	MsgStreamNotFound                          = ffe("FF21045", "Event stream '%v' not found", http.StatusNotFound)
	MsgListenerNotFound                        = ffe("FF21046", "Event listener '%v' not found", http.StatusNotFound)
	MsgDuplicateStreamName                     = ffe("FF21047", "Duplicate event stream name '%s' used by stream '%s'", http.StatusConflict)
	MsgMissingID                               = ffe("FF21048", "ID is required", http.StatusBadRequest)
	MsgPersistenceInitFail                     = ffe("FF21049", "Failed to initialize '%s' persistence: %s")
	MsgLevelDBPathMissing                      = ffe("FF21050", "Path must be supplied for LevelDB persistence")
	MsgFilterUpdateNotAllowed                  = ffe("FF21051", "Event filters cannot be updated after a listener is created. Previous signature: '%s'. New signature: '%s'")
	MsgResetStreamNotFound                     = ffe("FF21052", "Attempt to reset listener '%s', which is not currently registered on stream '%s'", http.StatusNotFound)
	MsgPersistenceMarshalFailed                = ffe("FF21053", "JSON serialization failed while writing to persistence")
	MsgPersistenceUnmarshalFailed              = ffe("FF21054", "JSON parsing failed while reading from persistence")
	MsgPersistenceReadFailed                   = ffe("FF21055", "Failed to read key '%s' from persistence")
	MsgPersistenceWriteFailed                  = ffe("FF21056", "Failed to read key '%s' from persistence")
	MsgPersistenceDeleteFailed                 = ffe("FF21057", "Failed to delete key '%s' from persistence")
	MsgPersistenceInitFailed                   = ffe("FF21058", "Failed to initialize persistence at path '%s'")
	MsgPersistenceTXIncomplete                 = ffe("FF21059", "Transaction is missing indexed fields")
	MsgNotStarted                              = ffe("FF21060", "Connector has not fully started yet", http.StatusServiceUnavailable)
	MsgPaginationErrTxNotFound                 = ffe("FF21062", "The ID specified in the 'after' option (for pagination) must match an existing transaction: '%s'", http.StatusNotFound)
	MsgTXConflictSignerPending                 = ffe("FF21063", "Only one of 'signer' and 'pending' can be supplied when querying transactions", http.StatusBadRequest)
	MsgInvalidSortDirection                    = ffe("FF21064", "Sort direction must be 'asc'/'ascending' or 'desc'/'descending': '%s'", http.StatusBadRequest)
	MsgDuplicateID                             = ffe("FF21065", "ID '%s' is not unique", http.StatusConflict)
	MsgTransactionFailed                       = ffe("FF21066", "Transaction execution failed")
	MsgTransactionNotFound                     = ffe("FF21067", "Transaction '%s' not found", http.StatusNotFound)
	DeprecatedMsgPolicyEngineRequestTimeout    = ffe("FF21068", "(Deprecated) The policy engine did not acknowledge the request after %.2fs", 408) // deprecated
	DeprecatedMsgPolicyEngineRequestInvalid    = ffe("FF21069", "(Deprecated) Invalid policy engine request type '%d'")                            // deprecated
	MsgTransactionHandlerNotRegistered         = ffe("FF21070", "No transaction handler registered with name '%s'")
	MsgNoGasConfigSetForTransactionHandler     = ffe("FF21071", "A fixed gas price must be set when not using a gas oracle")
	MsgTransactionHandlerRequestTimeout        = ffe("FF21072", "The transaction handler did not acknowledge the request after %.2fs", 408)
	MsgTransactionHandlerRequestInvalid        = ffe("FF21073", "Invalid transaction handler request type '%d'")
	MsgTransactionHandlerResponseNoSequenceID  = ffe("FF21074", "Transaction handler failed to allocate a sequence ID for request '%s'", http.StatusInternalServerError)
	MsgPersistenceSequenceIDNotAllowed         = ffe("FF21075", "New transaction should not have sequence ID")
	MsgInvalidJSONGasObject                    = ffe("FF21076", "Failed to parse response from Gas Oracle REST API as a JSON object")
	MsgTHMetricsInvalidName                    = ffe("FF21077", "Transaction handler metrics registration name can only contain lowercase letters and underscore. Actual name:%s")
	MsgTHMetricsHelpTextMissing                = ffe("FF21078", "Transaction handler metrics registration help text must be provided")
	MsgTHMetricsDuplicateName                  = ffe("FF21080", "Transaction handler metrics registration invalid name already registered: %s")
	MsgInvalidNonRichQuery                     = ffe("FF21081", "Rich query is not supported by persistence")
	MsgSQLScanFailed                           = ffe("FF21082", "Failed to read value from DB (%T)")
	MsgShuttingDown                            = ffe("FF21083", "Connector shutdown initiated", 500)
	MsgTransactionPersistenceError             = ffe("FF21084", "Failed to persist transaction data", 500)
	MsgOpNotSupportedWithoutRichQuery          = ffe("FF21085", "Not supported: The connector must be configured with a rich query database to support this operation", 501)
	MsgTransactionOpInvalid                    = ffe("FF21086", "Transaction operation is missing required fields", 400)
	MsgBlockListenerAlreadyStarted             = ffe("FF21087", "Block listener %s is already started", http.StatusConflict)
	MsgBlockListenerNotStarted                 = ffe("FF21088", "Block listener %s not started", http.StatusConflict)
	MsgBadListenerType                         = ffe("FF21089", "Invalid listener type: %s", http.StatusBadRequest)
	MsgFromBlockInvalid                        = ffe("FF21090", "From block invalid. Must be 'earliest', 'latest' or a decimal: %s", http.StatusBadRequest)
	MsgStreamAPIManaged                        = ffe("FF21091", "Event stream '%v' is API managed and cannot be started directly", http.StatusBadRequest)
	MsgStreamAPIManagedNameNoIDOrType          = ffe("FF21092", "API managed streams must have a name, but no ID or type", http.StatusBadRequest)
	MsgUpdatePayloadEmpty                      = ffe("FF21093", "Update transaction must have a non-empty payload", http.StatusBadRequest)
	MsgTxHandlerUnsupportedFieldForUpdate      = ffe("FF21094", "Update '%s' in the transaction is not supported by the transaction handler", http.StatusBadRequest)
	MsgConfirmedBlockListenerUnsupportedMode   = ffe("FF21095", "Block listener is not supported when chain tracking mode is '%s'", http.StatusBadRequest)
	MsgTransactionReceiptMissingBlockHash      = ffe("FF21096", "Transaction receipt missing block hash")
	MsgTransactionReceiptBlockHashMismatch     = ffe("FF21097", "Transaction receipt block hash mismatch. Expected %s, got %s")
)
