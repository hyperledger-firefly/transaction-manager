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

package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsTransactionManagerComponentName = "transaction_manager"

// REST api-server and transaction handler are sub-subsystem
var metricsTransactionHandlerSubsystemName = "th"
var metricsEventsSubsystemName = "events"
var metricsRESTAPIServerSubSystemName = "api_server_rest"

type metricsManager struct {
	ctx                     context.Context
	metricsEnabled          bool
	metricsRegistry         metric.MetricsRegistry
	txHandlerMetricsManager metric.MetricsManager
	eventsMetricsManager    metric.MetricsManager
	timeMap                 map[string]time.Time
}

func NewMetricsManager(ctx context.Context) Metrics {
	metricsRegistry := metric.NewPrometheusMetricsRegistry(metricsTransactionManagerComponentName)
	txHandlerMetricsManager, _ := metricsRegistry.NewMetricsManagerForSubsystem(ctx, metricsTransactionHandlerSubsystemName)
	eventsMetricsManager, _ := metricsRegistry.NewMetricsManagerForSubsystem(ctx, metricsEventsSubsystemName)
	_ = metricsRegistry.NewHTTPMetricsInstrumentationsForSubsystem(
		ctx,
		metricsRESTAPIServerSubSystemName,
		true,
		prometheus.DefBuckets,
		map[string]string{},
	)
	mm := &metricsManager{
		ctx:                     ctx,
		metricsEnabled:          config.GetBool(tmconfig.DeprecatedMetricsEnabled) || config.GetBool(tmconfig.MonitoringEnabled),
		timeMap:                 make(map[string]time.Time),
		metricsRegistry:         metricsRegistry,
		eventsMetricsManager:    eventsMetricsManager,
		txHandlerMetricsManager: txHandlerMetricsManager,
	}

	mm.InitEventMetrics()
	return mm
}

func (mm *metricsManager) IsMetricsEnabled() bool {
	return mm.metricsEnabled
}

func (mm *metricsManager) HTTPHandler() http.Handler {
	httpHandler, err := mm.metricsRegistry.HTTPHandler(mm.ctx, promhttp.HandlerOpts{})
	if err != nil {
		panic(err)
	}
	return httpHandler
}

func (mm *metricsManager) GetAPIServerRESTHTTPMiddleware() func(next http.Handler) http.Handler {
	httpMiddleware, _ := mm.metricsRegistry.GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(mm.ctx, metricsRESTAPIServerSubSystemName)
	return httpMiddleware
}

type Metrics interface {
	IsMetricsEnabled() bool

	// HTTPHandler returns the HTTP handler of this metrics registry
	HTTPHandler() http.Handler

	GetAPIServerRESTHTTPMiddleware() func(next http.Handler) http.Handler

	// MetricsRegistry returns the metrics registry for this metrics manager
	MetricsRegistry() metric.MetricsRegistry

	// functions for transaction handler to define and emit metrics
	// Transaction handler metrics are defined and emitted by transaction handlers
	TransactionHandlerMetrics

	// functions for event stream, confirmation manager to emit metrics
	EventMetricsEmitter
}

// Transaction handler metrics are defined and emitted by transaction handlers
type TransactionHandlerMetrics interface {
	txhandler.TransactionMetrics
}

func (mm *metricsManager) MetricsRegistry() metric.MetricsRegistry {
	return mm.metricsRegistry
}

func (mm *metricsManager) InitTxHandlerCounterMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewCounterMetric(ctx, metricName, helpText, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerCounterMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewCounterMetricWithLabels(ctx, metricName, helpText, labelNames, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerGaugeMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewGaugeMetric(ctx, metricName, helpText, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerGaugeMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewGaugeMetricWithLabels(ctx, metricName, helpText, labelNames, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerHistogramMetric(ctx context.Context, metricName string, helpText string, buckets []float64, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewHistogramMetric(ctx, metricName, helpText, buckets, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerHistogramMetricWithLabels(ctx context.Context, metricName string, helpText string, buckets []float64, labelNames []string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewHistogramMetricWithLabels(ctx, metricName, helpText, buckets, labelNames, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerSummaryMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewSummaryMetric(ctx, metricName, helpText, withDefaultLabels)
	}
}
func (mm *metricsManager) InitTxHandlerSummaryMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.NewSummaryMetricWithLabels(ctx, metricName, helpText, labelNames, withDefaultLabels)
	}
}

// functions for use existing metrics
func (mm *metricsManager) SetTxHandlerGaugeMetric(ctx context.Context, metricName string, number float64, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.SetGaugeMetric(ctx, metricName, number, defaultLabels)
	}
}
func (mm *metricsManager) SetTxHandlerGaugeMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.SetGaugeMetricWithLabels(ctx, metricName, number, labels, defaultLabels)
	}
}

func (mm *metricsManager) IncTxHandlerCounterMetric(ctx context.Context, metricName string, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.IncCounterMetric(ctx, metricName, defaultLabels)
	}
}
func (mm *metricsManager) IncTxHandlerCounterMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.IncCounterMetricWithLabels(ctx, metricName, labels, defaultLabels)
	}
}
func (mm *metricsManager) ObserveTxHandlerHistogramMetric(ctx context.Context, metricName string, number float64, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.ObserveHistogramMetric(ctx, metricName, number, defaultLabels)
	}
}
func (mm *metricsManager) ObserveTxHandlerHistogramMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.ObserveHistogramMetricWithLabels(ctx, metricName, number, labels, defaultLabels)
	}
}

func (mm *metricsManager) ObserveTxHandlerSummaryMetric(ctx context.Context, metricName string, number float64, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.ObserveSummaryMetric(ctx, metricName, number, defaultLabels)
	}
}
func (mm *metricsManager) ObserveTxHandlerSummaryMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *metric.FireflyDefaultLabels) {
	if mm.metricsEnabled {
		mm.txHandlerMetricsManager.ObserveSummaryMetricWithLabels(ctx, metricName, number, labels, defaultLabels)
	}
}
