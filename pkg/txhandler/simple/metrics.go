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

package simple

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/metric"
)

func (sth *simpleTransactionHandler) initSimpleHandlerMetrics(ctx context.Context) {
	sth.toolkit.MetricsManager.InitTxHandlerCounterMetricWithLabels(ctx, metricsCounterTransactionProcessOperationsTotal, metricsCounterTransactionProcessOperationsTotalDescription, []string{metricsLabelNameOperation}, true)
	sth.toolkit.MetricsManager.InitTxHandlerHistogramMetricWithLabels(ctx, metricsHistogramTransactionProcessOperationsDuration, metricsHistogramTransactionProcessOperationsDurationDescription, []float64{} /*fallback to default buckets*/, []string{metricsLabelNameOperation}, true)
	sth.toolkit.MetricsManager.InitTxHandlerGaugeMetric(ctx, metricsGaugeTransactionsInflightUsed, metricsGaugeTransactionsInflightUsedDescription, false)
	sth.toolkit.MetricsManager.InitTxHandlerGaugeMetric(ctx, metricsGaugeTransactionsInflightFree, metricsGaugeTransactionsInflightFreeDescription, false)
}

func (sth *simpleTransactionHandler) setTransactionInflightQueueMetrics(ctx context.Context) {
	sth.toolkit.MetricsManager.SetTxHandlerGaugeMetric(ctx, metricsGaugeTransactionsInflightUsed, float64(len(sth.inflight)), nil)
	sth.toolkit.MetricsManager.SetTxHandlerGaugeMetric(ctx, metricsGaugeTransactionsInflightFree, float64(sth.maxInFlight-len(sth.inflight)), nil)
}

func (sth *simpleTransactionHandler) incTransactionOperationCounter(ctx context.Context, fireflyNamespace string, operationName string) {
	sth.toolkit.MetricsManager.IncTxHandlerCounterMetricWithLabels(ctx, metricsCounterTransactionProcessOperationsTotal, map[string]string{metricsLabelNameOperation: operationName}, &metric.FireflyDefaultLabels{Namespace: fireflyNamespace})
}

func (sth *simpleTransactionHandler) recordTransactionOperationDuration(ctx context.Context, fireflyNamespace string, operationName string, durationInSeconds float64) {
	sth.toolkit.MetricsManager.ObserveTxHandlerHistogramMetricWithLabels(ctx, metricsHistogramTransactionProcessOperationsDuration, durationInSeconds, map[string]string{metricsLabelNameOperation: operationName}, &metric.FireflyDefaultLabels{Namespace: fireflyNamespace})
}
