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

package simple

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"sync"
	"time"

	sprig "github.com/Masterminds/sprig/v3"
	resty "github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/retry"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig" // shouldn't need this if you are developing a customized transaction handler
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"   // replace with your own messages if you are developing a customized transaction handler
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

const metricsCounterTransactionProcessOperationsTotal = "tx_process_operation_total"
const metricsCounterTransactionProcessOperationsTotalDescription = "Number of transaction process operations occurred grouped by operation name"

const metricsLabelNameOperation = "operation"

const metricsHistogramTransactionProcessOperationsDuration = "tx_process_duration_seconds"
const metricsHistogramTransactionProcessOperationsDurationDescription = "Duration of transaction process grouped by operation name"

// UpdateType informs FFTM whether the transaction needs an update to be persisted after this execution of the policy engine
type UpdateType int

const (
	None   UpdateType = iota // Instructs that no update is necessary
	Update                   // Instructs that the transaction should be updated in persistence
	Delete                   // Instructs that the transaction should be removed completely from persistence - generally only returned when TX status is TxStatusDeleteRequested
)

// RunContext is the context for an individual run of the simple policy loop, for an individual transaction.
// - Built from a snapshot of the mux-protected inflight state on input
// - Capturing the results from the run on output
type RunContext struct {
	// Input
	context.Context
	TX            *apitypes.ManagedTX
	Receipt       *ffcapi.TransactionReceiptResponse
	Confirmations *apitypes.ConfirmationsNotification
	Confirmed     bool
	SyncAction    policyEngineAPIRequestType
	ProcessTx     bool
	// Input/output
	SubStatus apitypes.TxSubStatus
	Info      *simplePolicyInfo // must be updated in-place and set UpdatedInfo to true as well as UpdateType = Update
	// Output
	UpdateType     UpdateType
	UpdatedInfo    bool
	TXUpdates      apitypes.TXUpdates
	HistoryUpdates []func(p txhandler.TransactionHistoryPersistence) error
}

func (ctx *RunContext) SetSubStatus(subStatus apitypes.TxSubStatus) {
	ctx.SubStatus = subStatus
}

func (ctx *RunContext) AddSubStatusAction(action apitypes.TxAction, info *fftypes.JSONAny, err *fftypes.JSONAny, actionOccurred *fftypes.FFTime) {
	subStatus := ctx.SubStatus // capture at time of action
	ctx.HistoryUpdates = append(ctx.HistoryUpdates, func(p txhandler.TransactionHistoryPersistence) error {
		return p.AddSubStatusAction(ctx, ctx.TX.ID, subStatus, action, info, err, actionOccurred)
	})
}

type TransactionHandlerFactory struct{}

func (f *TransactionHandlerFactory) Name() string {
	return "simple"
}

// simpleTransactionHandler is a base transaction handler forming an example for extension:
// - It offers three ways of calculating gas price: use a fixed number, use the built-in API of a ethereum connector, use a RESTful gas oracle
// - It resubmits the transaction based on a configured interval until it succeed or fail
func (f *TransactionHandlerFactory) NewTransactionHandler(ctx context.Context, conf config.Section) (txhandler.TransactionHandler, error) {
	gasOracleConfig := conf.SubSection(GasOracleConfig)
	sth := &simpleTransactionHandler{
		resubmitInterval: conf.GetDuration(ResubmitInterval),
		fixedGasPrice:    fftypes.JSONAnyPtr(conf.GetString(FixedGasPrice)),

		gasOracleMethod:        gasOracleConfig.GetString(GasOracleMethod),
		gasOracleQueryInterval: gasOracleConfig.GetDuration(GasOracleQueryInterval),
		gasOracleMode:          gasOracleConfig.GetString(GasOracleMode),

		inflightStale:  make(chan bool, 1),
		inflightUpdate: make(chan bool, 1),
	}

	// check whether we are using deprecated configuration
	if config.GetString(tmconfig.TransactionsHandlerName) == "" {
		log.L(ctx).Warnf("Initializing transaction handler with deprecated configurations. Please use 'transactions.handler' instead")
		sth.maxInFlight = config.GetInt(tmconfig.DeprecatedTransactionsMaxInFlight)
		sth.policyLoopInterval = config.GetDuration(tmconfig.DeprecatedPolicyLoopInterval)
		sth.retry = &retry.Retry{
			InitialDelay: config.GetDuration(tmconfig.DeprecatedPolicyLoopRetryInitDelay),
			MaximumDelay: config.GetDuration(tmconfig.DeprecatedPolicyLoopRetryMaxDelay),
			Factor:       config.GetFloat64(tmconfig.DeprecatedPolicyLoopRetryFactor),
		}
	} else {
		// if not, use the new transaction handler configurations
		sth.maxInFlight = conf.GetInt(MaxInFlight)
		sth.policyLoopInterval = conf.GetDuration(Interval)
		sth.retry = &retry.Retry{
			InitialDelay: conf.GetDuration(RetryInitDelay),
			MaximumDelay: conf.GetDuration(RetryMaxDelay),
			Factor:       conf.GetFloat64(RetryFactor),
		}
	}

	switch sth.gasOracleMode {
	case GasOracleModeConnector:
		// No initialization required
	case GasOracleModeRESTAPI:
		goc, err := ffresty.New(ctx, gasOracleConfig)
		if err != nil {
			return nil, err
		}
		sth.gasOracleClient = goc
		templateString := gasOracleConfig.GetString(GasOracleTemplate)
		if templateString == "" {
			return nil, i18n.NewError(ctx, tmmsgs.MsgMissingGOTemplate)
		}
		template, err := template.New("").Funcs(sprig.FuncMap()).Parse(templateString)
		if err != nil {
			return nil, i18n.NewError(ctx, tmmsgs.MsgBadGOTemplate, err)
		}
		sth.gasOracleTemplate = template
	default:
		if sth.fixedGasPrice.IsNil() {
			return nil, i18n.NewError(ctx, tmmsgs.MsgNoGasConfigSetForTransactionHandler)
		}
	}
	return sth, nil
}

type simpleTransactionHandler struct {
	ctx              context.Context
	toolkit          *txhandler.Toolkit
	fixedGasPrice    *fftypes.JSONAny
	resubmitInterval time.Duration

	gasOracleMode          string
	gasOracleClient        *resty.Client
	gasOracleMethod        string
	gasOracleTemplate      *template.Template
	gasOracleQueryInterval time.Duration
	gasOracleQueryValue    *fftypes.JSONAny
	gasOracleLastQueryTime *fftypes.FFTime

	policyLoopInterval      time.Duration
	policyLoopDone          chan struct{}
	inflightStale           chan bool
	inflightUpdate          chan bool
	mux                     sync.RWMutex
	inflightRWMux           sync.RWMutex
	inflight                []*pendingState
	policyEngineAPIRequests []*policyEngineAPIRequest
	maxInFlight             int
	retry                   *retry.Retry
}

type pendingState struct {
	mtx                     *apitypes.ManagedTX
	trackingTransactionHash string
	lastPolicyCycle         time.Time
	receipt                 *ffcapi.TransactionReceiptResponse
	info                    *simplePolicyInfo
	confirmed               bool
	confirmations           *apitypes.ConfirmationsNotification
	receiptNotify           *fftypes.FFTime
	confirmNotify           *fftypes.FFTime
	remove                  bool
	subStatus               apitypes.TxSubStatus
	// This mutex only works in a slice when the slice contains a pointer to this struct
	// appends to a slice copy memory but when storing pointers it does not
	mux sync.Mutex
}

type simplePolicyInfo struct {
	LastWarnTime *fftypes.FFTime `json:"lastWarnTime"`
}

func (sth *simpleTransactionHandler) Init(ctx context.Context, toolkit *txhandler.Toolkit) {
	sth.toolkit = toolkit

	// init metrics
	sth.initSimpleHandlerMetrics(ctx)
}

func (sth *simpleTransactionHandler) Start(ctx context.Context) (done <-chan struct{}, err error) {
	if sth.ctx == nil { // only start once
		sth.ctx = ctx // set the context for policy loop
		sth.policyLoopDone = make(chan struct{})
		sth.markInflightStale()
		go sth.policyLoop()
	}
	return sth.policyLoopDone, nil
}

func (sth *simpleTransactionHandler) requestIDPreCheck(ctx context.Context, reqHeaders *apitypes.RequestHeaders) (string, error) {
	// The request ID is the primary ID, and should be supplied by the user for idempotence.
	txID := reqHeaders.ID
	if txID == "" {
		// However, we will generate one for them if not supplied
		txID = fftypes.NewUUID().String()
		return txID, nil
	}

	// If it's supplied, we take the cost of a pre-check against the database for idempotency
	// before we do any processing.
	// The DB layer will protect us, but between now and then we might query the blockchain
	// to estimate gas and return unexpected 500 failures (rather than 409s)
	existing, err := sth.toolkit.TXPersistence.GetTransactionByID(ctx, txID)
	if err != nil {
		return "", err
	}
	if existing != nil {
		return "", i18n.NewError(ctx, tmmsgs.MsgDuplicateID, txID)
	}
	return txID, nil
}

// prepareTransaction prepares a transaction request by validating and preparing transaction data.
// It returns a prepared ManagedTX object that is ready for persistence, along with rejection status and error.
// This function does not persist the transaction - that should be done via createManagedTx.
func (sth *simpleTransactionHandler) prepareTransaction(ctx context.Context, txReq *apitypes.TransactionRequest) (*apitypes.ManagedTX, bool, error) {
	log.L(ctx).Tracef("prepareTransaction preparing request: %+v", txReq)
	txID, err := sth.requestIDPreCheck(ctx, &txReq.Headers)
	if err != nil {
		log.L(ctx).Errorf("prepareTransaction invalid tx ID for transaction request: %+v", txReq)
		return nil, false, err
	}

	// Prepare the transaction, which will mean we have a transaction that should be submittable.
	// If we fail at this stage, we don't need to write any state as we are sure we haven't submitted
	// anything to the blockchain itself.
	prepared, reason, err := sth.toolkit.Connector.TransactionPrepare(ctx, &ffcapi.TransactionPrepareRequest{
		TransactionInput: txReq.TransactionInput,
	})
	if err != nil {
		log.L(ctx).Errorf("prepareTransaction transaction prepare failed: %+v", err)
		return nil, ffcapi.MapSubmissionRejected(reason), err
	}
	log.L(ctx).Tracef("prepareTransaction prepared transaction with ID %s", txID)

	// Create the ManagedTX object without persisting it yet
	mtx := sth.createManagedTxObject(txID, &txReq.TransactionHeaders, prepared.Gas, prepared.TransactionData)
	return mtx, false, nil
}

// prepareContractDeployment prepares a contract deployment request by validating and preparing deployment data.
// It returns a prepared ManagedTX object that is ready for persistence, along with rejection status and error.
// This function does not persist the transaction - that should be done via createManagedTx.
func (sth *simpleTransactionHandler) prepareContractDeployment(ctx context.Context, txReq *apitypes.ContractDeployRequest) (*apitypes.ManagedTX, bool, error) {
	log.L(ctx).Tracef("prepareContractDeployment preparing request: %+v", txReq)
	txID, err := sth.requestIDPreCheck(ctx, &txReq.Headers)
	if err != nil {
		log.L(ctx).Errorf("prepareContractDeployment invalid tx ID for contract deployment request: %+v", txReq)
		return nil, false, err
	}

	// Prepare the transaction, which will mean we have a transaction that should be submittable.
	// If we fail at this stage, we don't need to write any state as we are sure we haven't submitted
	// anything to the blockchain itself.
	prepared, reason, err := sth.toolkit.Connector.DeployContractPrepare(ctx, &txReq.ContractDeployPrepareRequest)
	if err != nil {
		log.L(ctx).Errorf("prepareContractDeployment deploy contract prepare failed: %+v", err)
		return nil, ffcapi.MapSubmissionRejected(reason), err
	}
	log.L(ctx).Tracef("prepareContractDeployment prepared contract deployment with ID %s", txID)

	// Create the ManagedTX object without persisting it yet
	mtx := sth.createManagedTxObject(txID, &txReq.TransactionHeaders, prepared.Gas, prepared.TransactionData)
	return mtx, false, nil
}

func (sth *simpleTransactionHandler) HandleNewTransaction(ctx context.Context, txReq *apitypes.TransactionRequest) (mtx *apitypes.ManagedTX, submissionRejected bool, err error) {
	// Prepare the transaction
	preparedMtx, rejected, err := sth.prepareTransaction(ctx, txReq)
	if err != nil || rejected {
		return nil, rejected, err
	}

	// Persist the single transaction
	mtx, err = sth.createManagedTx(ctx, preparedMtx)
	return mtx, false, err
}

func (sth *simpleTransactionHandler) HandleNewContractDeployment(ctx context.Context, txReq *apitypes.ContractDeployRequest) (mtx *apitypes.ManagedTX, submissionRejected bool, err error) {
	// Prepare the contract deployment
	preparedMtx, rejected, err := sth.prepareContractDeployment(ctx, txReq)
	if err != nil || rejected {
		return nil, rejected, err
	}

	// Persist the single transaction
	mtx, err = sth.createManagedTx(ctx, preparedMtx)
	return mtx, false, err
}

func (sth *simpleTransactionHandler) HandleCancelTransaction(ctx context.Context, txID string) (mtx *apitypes.ManagedTX, err error) {
	res := sth.policyEngineAPIRequest(ctx, &policyEngineAPIRequest{
		requestType: ActionDelete,
		txID:        txID,
	})
	return res.tx, res.err
}

func (sth *simpleTransactionHandler) HandleSuspendTransaction(ctx context.Context, txID string) (mtx *apitypes.ManagedTX, err error) {
	res := sth.policyEngineAPIRequest(ctx, &policyEngineAPIRequest{
		requestType: ActionSuspend,
		txID:        txID,
	})
	return res.tx, res.err
}

func (sth *simpleTransactionHandler) HandleTransactionUpdate(ctx context.Context, txID string, updates apitypes.TXUpdatesExternal) (mtx *apitypes.ManagedTX, err error) {
	res := sth.policyEngineAPIRequest(ctx, &policyEngineAPIRequest{
		requestType: ActionUpdate,
		txID:        txID,
		txUpdates:   updates,
	})
	return res.tx, res.err
}

func (sth *simpleTransactionHandler) HandleResumeTransaction(ctx context.Context, txID string) (mtx *apitypes.ManagedTX, err error) {
	res := sth.policyEngineAPIRequest(ctx, &policyEngineAPIRequest{
		requestType: ActionResume,
		txID:        txID,
	})
	return res.tx, res.err
}

// createManagedTxObject creates a ManagedTX object without persisting it.
// This is used for batch operations where transactions are inserted together.
func (sth *simpleTransactionHandler) createManagedTxObject(txID string, txHeaders *ffcapi.TransactionHeaders, gas *fftypes.FFBigInt, transactionData string) *apitypes.ManagedTX {
	if gas != nil {
		txHeaders.Gas = gas
	}
	now := fftypes.Now()
	return &apitypes.ManagedTX{
		ID:                 txID, // on input the request ID must be the namespaced operation ID
		Created:            now,
		Updated:            now,
		TransactionHeaders: *txHeaders,
		TransactionData:    transactionData,
		Status:             apitypes.TxStatusPending,
		PolicyInfo:         fftypes.JSONAnyPtr(`{}`),
	}
}

func (sth *simpleTransactionHandler) createManagedTx(ctx context.Context, mtx *apitypes.ManagedTX) (*apitypes.ManagedTX, error) {
	// Sequencing ID will be added as part of persistence logic - so we have a deterministic order of transactions
	// Note: We must ensure persistence happens this within the nonce lock, to ensure that the nonce sequence and the
	//       global transaction sequence line up.
	err := sth.toolkit.TXPersistence.InsertTransactionWithNextNonce(ctx, mtx, func(ctx context.Context, signer string) (uint64, error) {
		log.L(ctx).Tracef("Getting next nonce for signer %s", signer)
		nextNonceRes, _, err := sth.toolkit.Connector.NextNonceForSigner(ctx, &ffcapi.NextNonceForSignerRequest{
			Signer: signer,
		})
		if err != nil {
			log.L(ctx).Errorf("Getting next nonce for signer %s failed: %+v", signer, err)
			return 0, err
		}
		log.L(ctx).Tracef("Getting next nonce for signer %s succeeded: %s, converting to uint: %d", signer, nextNonceRes.Nonce.String(), nextNonceRes.Nonce.Uint64())
		return nextNonceRes.Nonce.Uint64(), nil
	})
	if err == nil {
		err = sth.toolkit.TXHistory.AddSubStatusAction(ctx, mtx.ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, fftypes.JSONAnyPtr(`{"nonce":"`+mtx.Nonce.String()+`"}`), nil, fftypes.Now())
	}
	if err != nil {
		return nil, err
	}
	log.L(ctx).Infof("Tracking transaction %s at nonce %s / %d", mtx.ID, mtx.TransactionHeaders.From, mtx.Nonce.Int64())
	sth.markInflightStale()

	return mtx, nil
}

func (sth *simpleTransactionHandler) submitTX(ctx *RunContext) (reason ffcapi.ErrorReason, err error) {
	mtx := ctx.TX

	mtx.GasPrice, err = sth.getGasPrice(ctx, sth.toolkit.Connector)
	if err != nil {
		ctx.AddSubStatusAction(apitypes.TxActionRetrieveGasPrice, nil, fftypes.JSONAnyPtr(`{"error":"`+err.Error()+`"}`), fftypes.Now())
		return "", err
	}
	ctx.AddSubStatusAction(apitypes.TxActionRetrieveGasPrice, fftypes.JSONAnyPtr(`{"gasPrice":`+string(*mtx.GasPrice)+`}`), nil, fftypes.Now())

	sendTX := &ffcapi.TransactionSendRequest{
		TransactionHeaders: mtx.TransactionHeaders,
		GasPrice:           mtx.GasPrice,
		TransactionData:    mtx.TransactionData,
	}
	sendTX.TransactionHeaders.Nonce = (*fftypes.FFBigInt)(mtx.Nonce.Int())
	sendTX.TransactionHeaders.Gas = (*fftypes.FFBigInt)(mtx.Gas.Int())
	log.L(ctx).Debugf("Sending transaction %s at nonce %s / %d (lastSubmit=%s)", mtx.ID, mtx.TransactionHeaders.From, mtx.Nonce.Int64(), mtx.LastSubmit)
	transactionSendStartTime := time.Now()
	res, reason, err := sth.toolkit.Connector.TransactionSend(ctx, sendTX)
	sth.incTransactionOperationCounter(ctx, mtx.Namespace(ctx), "transaction_submission")
	sth.recordTransactionOperationDuration(ctx, mtx.Namespace(ctx), "transaction_submission", time.Since(transactionSendStartTime).Seconds())
	if err == nil {
		ctx.AddSubStatusAction(apitypes.TxActionSubmitTransaction, fftypes.JSONAnyPtr(`{"reason":"`+string(reason)+`"}`), nil, fftypes.Now())
		mtx.TransactionHash = res.TransactionHash
		mtx.LastSubmit = fftypes.Now()
		// Need to persist back as we've successfully submitted
		ctx.UpdateType = Update
		ctx.TXUpdates.TransactionHash = &res.TransactionHash
		ctx.TXUpdates.LastSubmit = mtx.LastSubmit
		ctx.TXUpdates.GasPrice = mtx.GasPrice
	} else {
		ctx.AddSubStatusAction(apitypes.TxActionSubmitTransaction, fftypes.JSONAnyPtr(`{"reason":"`+string(reason)+`"}`), fftypes.JSONAnyPtr(`{"error":"`+err.Error()+`"}`), fftypes.Now())
		// We have some simple rules for handling reasons from the connector, which could be enhanced by extending the connector.
		switch reason {
		case ffcapi.ErrorKnownTransaction, ffcapi.ErrorReasonNonceTooLow:
			// If we already have a transaction hash, this is fine - we just return as if we submitted it
			if mtx.TransactionHash != "" {
				log.L(ctx).Debugf("Transaction %s at nonce %s / %d known with hash: %s (%s)", mtx.ID, mtx.TransactionHeaders.From, mtx.Nonce.Int64(), mtx.TransactionHash, err)
				return "", nil
			}
			// Note: to cover the edge case where we had a timeout or other failure during the initial TransactionSend,
			//       a policy engine implementation would need to be able to re-calculate the hash that we would expect for the transaction.
			//       This would require a new FFCAPI API to calculate that hash, which requires the connector to perform the signing
			//       without submission to the node. For example using `eth_signTransaction` for EVM JSON/RPC.
			return reason, err
		default:
			return reason, err
		}
	}
	log.L(ctx).Infof("Transaction %s at nonce %s / %d submitted. Hash: %s", mtx.ID, mtx.TransactionHeaders.From, mtx.Nonce.Int64(), mtx.TransactionHash)
	ctx.SetSubStatus(apitypes.TxSubStatusTracking)
	return "", nil
}

func (sth *simpleTransactionHandler) processTransaction(ctx *RunContext) (err error) {

	// Simply policy engine allows deletion of the transaction without additional checks ( ensuring the TX has not been submitted / gap filling the nonce etc. )
	mtx := ctx.TX
	if mtx.DeleteRequested != nil {
		ctx.UpdateType = Delete
		return nil
	}

	if mtx.FirstSubmit == nil {
		// Submit the first time
		if _, err := sth.submitTX(ctx); err != nil {
			return err
		}
		mtx.FirstSubmit = mtx.LastSubmit
		ctx.TXUpdates.FirstSubmit = mtx.FirstSubmit
		return nil

	} else if ctx.Receipt == nil {

		// A more sophisticated policy engine would look at the reason for the lack of a receipt, and consider taking progressive
		// action such as increasing the gas cost slowly over time. This simple example shows how the policy engine
		// can use the FireFly core operation as a store for its historical state/decisions (in this case the last time we warned).
		lastWarnTime := ctx.Info.LastWarnTime
		if lastWarnTime == nil {
			lastWarnTime = mtx.FirstSubmit
		}
		now := fftypes.Now()
		if now.Time().Sub(*lastWarnTime.Time()) > sth.resubmitInterval {
			secsSinceSubmit := float64(now.Time().Sub(*mtx.FirstSubmit.Time())) / float64(time.Second)
			log.L(ctx).Infof("Transaction %s at nonce %s / %d has not been mined after %.2fs", mtx.ID, mtx.TransactionHeaders.From, mtx.Nonce.Int64(), secsSinceSubmit)
			ctx.UpdateType = Update
			ctx.UpdatedInfo = true
			ctx.Info.LastWarnTime = now
			// We do a resubmit at this point - as it might no longer be in the TX pool
			ctx.AddSubStatusAction(apitypes.TxActionTimeout, nil, nil, fftypes.Now())
			ctx.SetSubStatus(apitypes.TxSubStatusStale)
			if reason, err := sth.submitTX(ctx); err != nil {
				if reason != ffcapi.ErrorKnownTransaction {
					return err
				}
			}
			return nil
		}

	}
	// No action in the case we have a receipt
	return nil
}

// getGasPrice either uses a fixed gas price, or invokes a gas station API
func (sth *simpleTransactionHandler) getGasPrice(ctx context.Context, cAPI ffcapi.API) (gasPrice *fftypes.JSONAny, err error) {
	if sth.gasOracleQueryValue != nil && sth.gasOracleLastQueryTime != nil &&
		time.Since(*sth.gasOracleLastQueryTime.Time()) < sth.gasOracleQueryInterval {
		return sth.gasOracleQueryValue, nil
	}
	switch sth.gasOracleMode {
	case GasOracleModeRESTAPI:
		// Make a REST call against an endpoint, and extract a value/structure to pass to the connector
		gasPrice, err := sth.getGasPriceAPI(ctx)
		if err != nil {
			return nil, err
		}
		sth.gasOracleQueryValue = gasPrice
		sth.gasOracleLastQueryTime = fftypes.Now()
		return sth.gasOracleQueryValue, nil
	case GasOracleModeConnector:
		// Call the connector
		res, _, err := cAPI.GasPriceEstimate(ctx, &ffcapi.GasPriceEstimateRequest{})
		if err != nil {
			return nil, err
		}
		sth.gasOracleQueryValue = res.GasPrice
		sth.gasOracleLastQueryTime = fftypes.Now()
		return sth.gasOracleQueryValue, nil
	default:
		// Disabled - just a fixed value - note that the fixed value can be any JSON structure,
		// as interpreted by the connector. For example EVMConnect support a simple value, or a
		// post EIP-1559 structure.
		return sth.fixedGasPrice, nil
	}
}

func (sth *simpleTransactionHandler) getGasPriceAPI(ctx context.Context) (gasPrice *fftypes.JSONAny, err error) {
	res, err := sth.gasOracleClient.R().
		Execute(sth.gasOracleMethod, "")
	if err != nil {
		return nil, i18n.WrapError(ctx, err, tmmsgs.MsgErrorQueryingGasOracleAPI, -1, err.Error())
	}
	if res.IsError() {
		return nil, i18n.WrapError(ctx, err, tmmsgs.MsgErrorQueryingGasOracleAPI, res.StatusCode(), res.RawResponse)
	}
	// Parse the response body as JSON
	var data map[string]interface{}
	err = json.Unmarshal(res.Body(), &data)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, tmmsgs.MsgInvalidJSONGasObject)
	}
	buff := new(bytes.Buffer)
	err = sth.gasOracleTemplate.Execute(buff, data)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, tmmsgs.MsgGasOracleResultError)
	}
	return fftypes.JSONAnyPtr(buff.String()), nil
}
