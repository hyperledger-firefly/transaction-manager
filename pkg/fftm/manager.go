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

package fftm

import (
	"context"
	"sync"

	"github.com/gorilla/mux"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/httpserver"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/hyperledger-firefly/transaction-manager/internal/blocklistener"
	"github.com/hyperledger-firefly/transaction-manager/internal/confirmations"
	"github.com/hyperledger-firefly/transaction-manager/internal/events"
	"github.com/hyperledger-firefly/transaction-manager/internal/metrics"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence/leveldb"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence/postgres"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/internal/ws"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/eventapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	txRegistry "github.com/hyperledger-firefly/transaction-manager/pkg/txhandler/registry"
)

type Manager interface {
	Start() error
	Close()

	APIRouter() *mux.Router

	// API managed event streams have checkpoints stored externally.
	// Events are access by calling PollAPIMangedStream() on the returned stream.
	// - The spec must have a name, but no UUID or type.
	// - The name must be unique to managed API streams (separate namespace to persisted ones)
	// - Multiple calls with the same ID will return the same object.
	// - Use "isNew" to determine if the event stream was freshly initialized from the listener array
	// - If not freshly initialized, the listeners array ignored (you can choose to do check the existing listener list yourself)
	//
	// Checkpoints are commonly tied to individual listeners, so it is critical that the caller manages
	// deterministically the IDs of the listeners passed in. They cannot be empty (an error will be returned)
	GetAPIManagedEventStream(spec *apitypes.EventStream, listeners []*apitypes.Listener) (isNew bool, es eventapi.EventStream, err error)

	// Resources will be used by the stream in the background, so if your object is deleted
	// then this function should be called to clean-up any in-memory state (if there is any
	// possibility you previously called GetAPIManagedEventStream)
	CleanupAPIManagedEventStream(name string) error

	// Access directly to the transaction completions polling interface, which allows blocking-polling interactions with an
	// ordered stream of transaction completions
	TransactionCompletions() txhandler.TransactionCompletions

	// Ability to query a transaction by ID to get full status
	GetTransactionByIDWithStatus(ctx context.Context, txID string, withHistory bool) (transaction *apitypes.TXWithStatus, err error)

	// Ability to reconcile confirmations for a transaction
	// This function uses the single in-memory canonical chain to check and build the confirmation map of a given transaction hash
	ReconcileConfirmationsForTransaction(ctx context.Context, txHash string, existingConfirmations []*ffcapi.MinimalBlockInfo, targetConfirmationCount uint64) (*ffcapi.ConfirmationUpdateResult, error)

	// Ability to submit new transactions into the transaction handler for management/submission
	TransactionHandler() txhandler.TransactionHandler

	// MetricsRegistry returns the metrics registry for this manager for customizing metrics
	MetricsRegistry() metric.MetricsRegistry
}

type manager struct {
	ctx              context.Context
	cancelCtx        func()
	confirmations    confirmations.Manager
	txHandler        txhandler.TransactionHandler
	apiRouter        *mux.Router
	apiServer        httpserver.HTTPServer
	monitoringServer httpserver.HTTPServer
	wsServer         ws.WebSocketServer
	persistence      persistence.Persistence
	richQueryEnabled bool

	connector ffcapi.API
	toolkit   *txhandler.Toolkit

	mux                      sync.Mutex
	eventStreams             map[fftypes.UUID]events.Stream
	streamsByName            map[string]*fftypes.UUID
	apiStreamsByName         map[string]*fftypes.UUID
	blockListenerDone        chan struct{}
	txHandlerDone            <-chan struct{}
	started                  bool
	apiServerDone            chan error
	monitoringServerDone     chan error
	monitoringEnabled        bool
	deprecatedMetricsEnabled bool
	metricsManager           metrics.Metrics
}

func InitConfig() {
	tmconfig.Reset()
	events.InitDefaults()
}

func NewManager(ctx context.Context, connector ffcapi.API) (Manager, error) {
	var err error
	m := newManager(ctx, connector)
	if err = m.initPersistence(ctx); err != nil {
		return nil, err
	}
	if err = m.initServices(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func newManager(ctx context.Context, connector ffcapi.API) *manager {
	m := &manager{
		connector:                connector,
		apiServerDone:            make(chan error),
		monitoringServerDone:     make(chan error),
		deprecatedMetricsEnabled: config.GetBool(tmconfig.DeprecatedMetricsEnabled),
		monitoringEnabled:        config.GetBool(tmconfig.MonitoringEnabled),
		eventStreams:             make(map[fftypes.UUID]events.Stream),
		streamsByName:            make(map[string]*fftypes.UUID),
		apiStreamsByName:         make(map[string]*fftypes.UUID),
		metricsManager:           metrics.NewMetricsManager(ctx),
	}
	m.toolkit = &txhandler.Toolkit{
		Connector:      m.connector,
		MetricsManager: m.metricsManager,
	}
	m.ctx, m.cancelCtx = context.WithCancel(ctx)
	return m
}

func (m *manager) initServices(ctx context.Context) (err error) {
	m.confirmations = confirmations.NewBlockConfirmationManager(ctx, m.connector, "receipts", m.metricsManager)
	m.wsServer = ws.NewWebSocketServer(ctx)
	m.apiRouter = m.router(m.monitoringEnabled || m.deprecatedMetricsEnabled)
	m.apiServer, err = httpserver.NewHTTPServer(ctx, "api", m.apiRouter, m.apiServerDone, tmconfig.APIConfig, tmconfig.CorsConfig)
	if err != nil {
		return err
	}

	// check whether a policy engine name is provided
	if config.GetString(tmconfig.TransactionsHandlerName) == "" {
		log.L(ctx).Warnf("The 'policyengine' config key has been deprecated. Please use 'transactions.handler' instead")
		m.txHandler, err = txRegistry.NewTransactionHandler(ctx, tmconfig.DeprecatedPolicyEngineBaseConfig, config.GetString(tmconfig.DeprecatedPolicyEngineName))
	} else {
		// if not, fall back to use the deprecated policy engine
		m.txHandler, err = txRegistry.NewTransactionHandler(ctx, tmconfig.TransactionHandlerBaseConfig, config.GetString(tmconfig.TransactionsHandlerName))
	}

	if err != nil {
		return err
	}
	m.toolkit.EventHandler = NewManagedTransactionEventHandler(ctx, m.confirmations, m.wsServer, m.txHandler)
	m.txHandler.Init(ctx, m.toolkit)

	return m.initMonitoringRouter(ctx)
}

func (m *manager) initMonitoringRouter(ctx context.Context) (err error) {
	// metrics service must be initialized after transaction handler
	// in case the transaction handler has logic in the Init function
	// to add more metrics
	if m.monitoringEnabled {
		m.monitoringServer, err = httpserver.NewHTTPServer(ctx, "monitoring", m.createMonitoringMuxRouter(), m.monitoringServerDone, tmconfig.MonitoringConfig, tmconfig.CorsConfig)
	} else if m.deprecatedMetricsEnabled {
		m.monitoringServer, err = httpserver.NewHTTPServer(ctx, "metrics", m.createMonitoringMuxRouter(), m.monitoringServerDone, tmconfig.DeprecatedMetricsConfig, tmconfig.CorsConfig)
	}
	return err
}

func (m *manager) initPersistence(ctx context.Context) (err error) {
	pType := config.GetString(tmconfig.PersistenceType)
	nonceStateTimeout := config.GetDuration(tmconfig.TransactionsNonceStateTimeout)
	switch pType {
	case "leveldb":
		if m.persistence, err = leveldb.NewLevelDBPersistence(ctx, nonceStateTimeout); err != nil {
			return i18n.NewError(ctx, tmmsgs.MsgPersistenceInitFail, pType, err)
		}
	case "postgres":
		if m.persistence, err = postgres.NewPostgresPersistence(ctx, tmconfig.PostgresSection, nonceStateTimeout); err != nil {
			return i18n.NewError(ctx, tmmsgs.MsgPersistenceInitFail, pType, err)
		}
		if !config.GetBool(tmconfig.APISimpleQuery) {
			m.richQueryEnabled = true
			m.toolkit.RichQuery = m.persistence.RichQuery()
		}
	default:
		return i18n.NewError(ctx, tmmsgs.MsgUnknownPersistence, pType)
	}
	m.toolkit.TXPersistence = m.persistence
	m.toolkit.TXHistory = m.persistence
	return nil
}

func (m *manager) Start() error {
	go httpserver.RunDebugServer(m.ctx, tmconfig.DebugConfig)

	if err := m.restoreStreams(); err != nil {
		return err
	}

	blReq := &ffcapi.NewBlockListenerRequest{ListenerContext: m.ctx, ID: fftypes.NewUUID()}
	blReq.BlockListener, m.blockListenerDone = blocklistener.BufferChannel(m.ctx, m.confirmations)
	_, _, err := m.connector.NewBlockListener(m.ctx, blReq)
	if err != nil {
		return err
	}

	go m.runAPIServer()
	if m.monitoringEnabled || m.deprecatedMetricsEnabled {
		go m.runMonitoringServer()
	}
	go m.confirmations.Start()

	m.txHandlerDone, err = m.txHandler.Start(m.ctx)
	if err != nil {
		return err
	}
	m.started = true
	return nil
}

func (m *manager) TransactionHandler() txhandler.TransactionHandler {
	return m.txHandler
}

func (m *manager) TransactionCompletions() txhandler.TransactionCompletions {
	return m.persistence.TransactionCompletions()
}

func (m *manager) MetricsRegistry() metric.MetricsRegistry {
	return m.metricsManager.MetricsRegistry()
}

func (m *manager) APIRouter() *mux.Router {
	return m.apiRouter
}

func (m *manager) Close() {
	m.cancelCtx()
	if m.started {
		m.started = false
		<-m.apiServerDone
		if m.monitoringEnabled || m.deprecatedMetricsEnabled {
			<-m.monitoringServerDone
		}
		<-m.txHandlerDone
		<-m.blockListenerDone

		streams := []events.Stream{}
		m.mux.Lock()
		for _, s := range m.eventStreams {
			streams = append(streams, s)
		}
		m.mux.Unlock()
		for _, s := range streams {
			_ = s.Stop(m.ctx)
		}
	}
	m.persistence.Close(m.ctx)
}
