// Copyright © 2024 - 2026 Kaleido, Inc.
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

package txhandler

import (
	"context"
	"time"

	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

type NextNonceCallback func(ctx context.Context, signer string) (uint64, error)

type SortDirection int

const (
	SortDirectionAscending SortDirection = iota
	SortDirectionDescending
)

type TransactionPersistence interface {
	ListTransactionsByCreateTime(ctx context.Context, after *apitypes.ManagedTX, limit int, dir SortDirection) ([]*apitypes.ManagedTX, error)         // reverse create time order
	ListTransactionsByNonce(ctx context.Context, signer string, after *fftypes.FFBigInt, limit int, dir SortDirection) ([]*apitypes.ManagedTX, error) // reverse nonce order within signer
	ListTransactionsPending(ctx context.Context, afterSequenceID string, limit int, dir SortDirection) ([]*apitypes.ManagedTX, error)                 // reverse insertion order, only those in pending state
	GetTransactionByID(ctx context.Context, txID string) (*apitypes.ManagedTX, error)
	GetTransactionByIDWithStatus(ctx context.Context, txID string, history bool) (*apitypes.TXWithStatus, error)
	GetTransactionByNonce(ctx context.Context, signer string, nonce *fftypes.FFBigInt) (*apitypes.ManagedTX, error)
	InsertTransactionPreAssignedNonce(ctx context.Context, tx *apitypes.ManagedTX) error
	InsertTransactionWithNextNonce(ctx context.Context, tx *apitypes.ManagedTX, lookupNextNonce NextNonceCallback) error
	InsertTransactionsWithNextNonce(ctx context.Context, txs []*apitypes.ManagedTX, lookupNextNonce NextNonceCallback) []error
	UpdateTransaction(ctx context.Context, txID string, updates *apitypes.TXUpdates) error
	DeleteTransaction(ctx context.Context, txID string) error

	GetTransactionReceipt(ctx context.Context, txID string) (receipt *ffcapi.TransactionReceiptResponse, err error)
	SetTransactionReceipt(ctx context.Context, txID string, receipt *ffcapi.TransactionReceiptResponse) error

	GetTransactionConfirmations(ctx context.Context, txID string) ([]*apitypes.Confirmation, error)
	AddTransactionConfirmations(ctx context.Context, txID string, clearExisting bool, confirmations ...*apitypes.Confirmation) error
}

type RichQuery interface {
	ListStreams(ctx context.Context, filter ffapi.AndFilter) ([]*apitypes.EventStream, *ffapi.FilterResult, error)
	ListListeners(ctx context.Context, filter ffapi.AndFilter) ([]*apitypes.Listener, *ffapi.FilterResult, error)
	ListTransactions(ctx context.Context, filter ffapi.AndFilter) ([]*apitypes.ManagedTX, *ffapi.FilterResult, error)
	ListTransactionConfirmations(ctx context.Context, txID string, filter ffapi.AndFilter) ([]*apitypes.ConfirmationRecord, *ffapi.FilterResult, error)
	ListTransactionHistory(ctx context.Context, txID string, filter ffapi.AndFilter) ([]*apitypes.TXHistoryRecord, *ffapi.FilterResult, error)
	ListStreamListeners(ctx context.Context, streamID *fftypes.UUID, filter ffapi.AndFilter) ([]*apitypes.Listener, *ffapi.FilterResult, error)

	NewStreamFilter(ctx context.Context) ffapi.FilterBuilder
	NewListenerFilter(ctx context.Context) ffapi.FilterBuilder
	NewTransactionFilter(ctx context.Context) ffapi.FilterBuilder
	NewConfirmationFilter(ctx context.Context) ffapi.FilterBuilder
	NewTxHistoryFilter(ctx context.Context) ffapi.FilterBuilder
}

type TransactionCompletions interface {
	ListTxCompletionsByCreateTime(ctx context.Context, after *int64, limit int, dir SortDirection) ([]*apitypes.TXCompletion, error)
	WaitTxCompletionUpdates(ctx context.Context, timeBeforeLastPoll time.Time) bool
}

type TransactionHistoryPersistence interface {
	AddSubStatusAction(ctx context.Context, txID string, subStatus apitypes.TxSubStatus, action apitypes.TxAction, info *fftypes.JSONAny, err *fftypes.JSONAny, actionOccurred *fftypes.FFTime) error
}

type TransactionMetrics interface {
	// functions for declaring new metrics
	InitTxHandlerCounterMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool)
	InitTxHandlerCounterMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool)
	InitTxHandlerGaugeMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool)
	InitTxHandlerGaugeMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool)
	InitTxHandlerHistogramMetric(ctx context.Context, metricName string, helpText string, buckets []float64, withDefaultLabels bool)
	InitTxHandlerHistogramMetricWithLabels(ctx context.Context, metricName string, helpText string, buckets []float64, labelNames []string, withDefaultLabels bool)
	InitTxHandlerSummaryMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool)
	InitTxHandlerSummaryMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool)

	// functions for use existing metrics
	SetTxHandlerGaugeMetric(ctx context.Context, metricName string, number float64, defaultLabels *metric.FireflyDefaultLabels)
	SetTxHandlerGaugeMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels)
	IncTxHandlerCounterMetric(ctx context.Context, metricName string, defaultLabels *metric.FireflyDefaultLabels)
	IncTxHandlerCounterMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels)
	ObserveTxHandlerHistogramMetric(ctx context.Context, metricName string, number float64, defaultLabels *metric.FireflyDefaultLabels)
	ObserveTxHandlerHistogramMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels)
	ObserveTxHandlerSummaryMetric(ctx context.Context, metricName string, number float64, defaultLabels *metric.FireflyDefaultLabels)
	ObserveTxHandlerSummaryMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels)
}

type Toolkit struct {
	// Connector toolkit contains methods to interact with the plugged-in JSON-RPC endpoint of a Blockchain network
	Connector ffcapi.API

	// Transaction History toolkit contains methods to easily manage and set historical status of a Managed Transaction
	TXHistory TransactionHistoryPersistence

	// TransactionPersistence toolkit contains methods to persist Managed Transaction objects into the plugged-in persistence service
	TXPersistence TransactionPersistence

	// When a rich-query enabled Database is available (PSQL) the full rich query interface for all objects is passed to the policy engine.
	// If not available, this will be nil.
	RichQuery RichQuery

	// Metric toolkit contains methods to emit Managed Transaction specific metrics using the plugged-in metrics service
	MetricsManager TransactionMetrics

	// Event Handler toolkit contains methods to handle a defined set of events when processing managed transactions
	EventHandler ManagedTxEventHandler
}

// Handler checks received transaction process events and dispatch them to an event
// manager accordingly.
type ManagedTxEventHandler interface {
	HandleEvent(ctx context.Context, e apitypes.ManagedTransactionEvent) error
}

// Transaction Handler owns the lifecycle of ManagedTransaction records
// Transaction Manager delegates all transaction specific operations to transaction apart from the triggers (REST API call for actions,
// Event stream for events) of those operations. This design allows the Transaction Handler to apply customized logic at different stages
// of transaction life cycles.
// The Transaction Handler interface consists of x important responsibilities:
// 1. Functions for handling its own lifecycle. (e.g. Init, Start)
//
//  2. Functions for handling events:
//     When implementing functions in this category, developers need to keep concurrency in mind. Because multiple invocations of a single
//     function can be process in parallel in any given time, it's recommended to use channels to ensure they are not blocking each other.
//     - inbound events: these events are received by transaction handler
//     a. instructional events: transaction handler need to implement the logic to make those event happen on blockchain. (e.g. creating
//     a new transaction)
//     b. informational events: transaction handler need to update its own record when an event has already happened to a related
//     blockchain transaction of its managed transaction
//     - outbound events: these events are emitted by transaction handler. apitypes.ManagedTransactionEventType contains a list of
//     managed transaction events that can be emitted by transaction handler.
type TransactionHandler interface {
	// Lifecycle functions

	// Init - setting a set of initialized toolkit plugins in the constructed transaction handler object. Safe checks & initialization
	//        can take place inside this function as well. It also enables toolkit plugins to be able to embed a reference to its parent
	//        transaction handler instance.
	Init(ctx context.Context, toolkit *Toolkit)

	// Start - starting the transaction handler to handle inbound events.
	// It takes in a context, of which upon cancellation will stop the transaction handler.
	// It returns a read-only channel. When this channel gets closed, it indicates transaction handler has been stopped gracefully.
	// It returns an error when failed to start.
	Start(ctx context.Context) (done <-chan struct{}, err error)

	// Event handling functions
	// Instructional events:
	// HandleNewTransaction - handles event of adding new transactions onto blockchain
	HandleNewTransaction(ctx context.Context, txReq *apitypes.TransactionRequest) (mtx *apitypes.ManagedTX, submissionRejected bool, err error)
	// HandleNewContractDeployment - handles event of adding new smart contract deployment onto blockchain
	HandleNewContractDeployment(ctx context.Context, txReq *apitypes.ContractDeployRequest) (mtx *apitypes.ManagedTX, submissionRejected bool, err error)
	// HandleCancelTransaction - handles event of cancelling a managed transaction
	HandleCancelTransaction(ctx context.Context, txID string) (mtx *apitypes.ManagedTX, err error)
	// HandleSuspendTransaction - handles event of suspending a managed transaction
	HandleSuspendTransaction(ctx context.Context, txID string) (mtx *apitypes.ManagedTX, err error)
	// HandleResumeTransaction - handles event of resuming a suspended managed transaction
	HandleResumeTransaction(ctx context.Context, txID string) (mtx *apitypes.ManagedTX, err error)
	// HandleTransactionUpdate - handles event of updating a managed transaction
	HandleTransactionUpdate(ctx context.Context, txID string, update apitypes.TXUpdatesExternal) (mtx *apitypes.ManagedTX, err error)

	// HandleSubmissions - handles batch of new submissions onto blockchain
	HandleSubmissions(ctx context.Context, submissions []*apitypes.SubmissionRequest) (responses []*apitypes.SubmissionResponse)

	// Informational events:
	// HandleTransactionConfirmations - handles confirmations of blockchain transactions for a managed transaction
	HandleTransactionConfirmations(ctx context.Context, txID string, notification *apitypes.ConfirmationsNotification) (err error)
	// HandleTransactionReceiptReceived - handles receipt of blockchain transactions for a managed transaction
	HandleTransactionReceiptReceived(ctx context.Context, txID string, receipt *ffcapi.TransactionReceiptResponse) (err error)
}
