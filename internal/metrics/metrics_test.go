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

package metrics

import (
	"context"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/stretchr/testify/assert"
)

func newTestMetricsManager(t *testing.T) (*metricsManager, func()) {
	tmconfig.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	mmi := NewMetricsManager(ctx)
	mm := mmi.(*metricsManager)
	assert.Equal(t, len(mm.timeMap), 0)
	httpMiddleware := mm.GetAPIServerRESTHTTPMiddleware()
	assert.NotNil(t, httpMiddleware)
	httpHandler := mm.HTTPHandler()
	assert.NotNil(t, httpHandler)
	return mm, cancel
}

func TestTransactionHandlerMetricsToolkit(t *testing.T) {
	ctx := context.Background()
	mm, cancel := newTestMetricsManager(t)
	defer cancel()
	mm.metricsEnabled = true
	mm.InitTxHandlerCounterMetric(ctx, "tx_request", "Transactions requests handled", false)
	mm.InitTxHandlerCounterMetricWithLabels(ctx, "tx_process", "Transaction processed", []string{"status"}, false)
	mm.InitTxHandlerGaugeMetric(ctx, "tx_stalled", "Transactions that are stuck in a loop", false)
	mm.InitTxHandlerGaugeMetricWithLabels(ctx, "tx_inflight", "Transactions that are in flight", []string{"stage"}, false)
	mm.InitTxHandlerHistogramMetric(ctx, "tx_timeout_seconds", "Duration of timed out transactions", []float64{}, false)
	mm.InitTxHandlerHistogramMetricWithLabels(ctx, "tx_stage_seconds", "Duration of each transaction stage", []float64{}, []string{"stage"}, false)
	mm.InitTxHandlerSummaryMetric(ctx, "tx_request_bytes", "Request size of timed out transactions", false)
	mm.InitTxHandlerSummaryMetricWithLabels(ctx, "tx_retry_bytes", "Retry request size of each transaction stage", []string{"stage"}, false)

	mm.IncTxHandlerCounterMetric(ctx, "tx_request", nil)
	mm.IncTxHandlerCounterMetricWithLabels(ctx, "tx_process", map[string]string{"status": "success"}, nil)
	mm.SetTxHandlerGaugeMetric(ctx, "tx_stalled", 2, nil)
	mm.SetTxHandlerGaugeMetricWithLabels(ctx, "tx_inflight", 2, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveTxHandlerHistogramMetric(ctx, "tx_timeout_seconds", 2000, nil)
	mm.ObserveTxHandlerHistogramMetricWithLabels(ctx, "tx_stage_seconds", 2000, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveTxHandlerSummaryMetric(ctx, "tx_request_bytes", 2000, nil)
	mm.ObserveTxHandlerSummaryMetricWithLabels(ctx, "tx_retry_bytes", 2000, map[string]string{"stage": "singing:)"}, nil)
}

func TestTransactionHandlerMetricsToolkitSwallowErrors(t *testing.T) {
	ctx := context.Background()
	mm, cancel := newTestMetricsManager(t)
	defer cancel()
	mm.metricsEnabled = true

	// swallow init errors
	mm.InitTxHandlerCounterMetric(ctx, "txInvalidName", "Invalid name", false)
	mm.InitTxHandlerGaugeMetric(ctx, "tx_duplicate", "Duplicate registration", false)
	mm.InitTxHandlerGaugeMetric(ctx, "tx_duplicate", "Duplicate registration", false)
	mm.InitTxHandlerGaugeMetric(ctx, "tx_no_help_text", "", false)

	// swallow emit errors, none of the metrics below are registered
	mm.IncTxHandlerCounterMetric(ctx, "tx_not_exist", nil)
	mm.IncTxHandlerCounterMetricWithLabels(ctx, "tx_not_exist", map[string]string{"status": "success"}, nil)
	mm.SetTxHandlerGaugeMetric(ctx, "tx_not_exist", 2, nil)
	mm.SetTxHandlerGaugeMetricWithLabels(ctx, "tx_not_exist", 2, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveTxHandlerHistogramMetric(ctx, "tx_not_exist", 2000, nil)
	mm.ObserveTxHandlerHistogramMetricWithLabels(ctx, "tx_not_exist", 2000, map[string]string{"stage": "singing:)"}, nil)
	mm.ObserveTxHandlerSummaryMetric(ctx, "tx_not_exist", 2000, nil)
	mm.ObserveTxHandlerSummaryMetricWithLabels(ctx, "tx_not_exist", 2000, map[string]string{"stage": "singing:)"}, nil)
}

func TestIsMetricsEnabledTrue(t *testing.T) {
	mm, cancel := newTestMetricsManager(t)
	defer cancel()
	mm.metricsEnabled = true
	assert.Equal(t, mm.IsMetricsEnabled(), true)
}

func TestIsMetricsEnabledFalse(t *testing.T) {
	mm, cancel := newTestMetricsManager(t)
	defer cancel()
	mm.metricsEnabled = false
	assert.Equal(t, mm.IsMetricsEnabled(), false)
}
