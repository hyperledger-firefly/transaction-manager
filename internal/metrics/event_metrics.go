// Copyright © 2024 Kaleido, Inc.
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

import "context"

const metricsLabelStatus = "status"
const metricsLabelType = "type"

const metricsGaugeBlockHashBatchSize = "block_hash_batch_size"
const metricsGaugeBlockHashBatchSizeDescription = "Number of block hashes batched in a single batch process event"

const mtrCounterReceiptCheckTotal = "receipt_check_total"
const mtrCounterReceiptCheckTotalDescription = "Number of receipt check happened grouped by receipt check status"
const mtrHistogramReceiptCheckDuration = "receipt_check_duration_seconds"
const mtrHistogramReceiptCheckDurationDescription = "Duration of individual receipt check grouped by receipt check status"

const mtrCounterNotificationQueuedTotal = "notification_queued_total"
const mtrCounterNotificationQueuedTotalDescription = "Number of notification queued grouped by notification type"
const mtrHistogramNotificationQueueingDuration = "notification_queueing_duration_seconds"
const mtrHistogramNotificationQueueingDurationDescription = "Duration of adding a notification into the queue grouped by notification type"

const mtrCounterNotificationProcessedTotal = "notification_processed_total"
const mtrCounterNotificationProcessedTotalDescription = "Number of notification processed grouped by notification type"
const mtrHistogramNotificationProcessDuration = "notification_process_seconds"
const mtrHistogramNotificationProcessDurationDescription = "Duration of processing a notification grouped by notification type"

const mtrCounterBlockHashProcessedTotal = "block_hash_processed_total"
const mtrCounterBlockHashProcessedTotalDescription = "Number of block hashes processed by confirmation manager"
const mtrHistogramBlockHashProcessDuration = "block_hash_process_duration_seconds"
const mtrHistogramBlockHashProcessDurationDescription = "Duration of processing a block hash received by confirmation manager"

const mtrCounterBlockHashQueuedTotal = "block_hash_queued_total"
const mtrCounterBlockHashQueuedTotalDescription = "Number of block hashes queued for confirmation manager"
const mtrHistogramBlockHashQueueingDuration = "block_hash_queueing_duration_seconds"
const mtrHistogramBlockHashQueueingDurationDescription = "Duration of confirmation manager fetch a queued block hash"

const mtrCounterConfirmedTotal = "confirmation_processed_total"
const mtrCounterConfirmedTotalDescription = "Number of transactions confirmed"
const mtrHistogramConfirmDuration = "confirmation_process_duration_seconds"
const mtrHistogramConfirmDurationDescription = "Duration of confirm a transaction"

const mtrCounterEventRedetectedTotal = "event_redetected_total"
const mtrCounterEventRedetectedTotalDescription = "Number of confirmed events dropped as re-detections, being behind the checkpoint of an event already acknowledged by the receiver"

const mtrCounterReceiptTotal = "receipt_processed_total"
const mtrCounterReceiptTotalDescription = "Number of transaction receipts notified"
const mtrHistogramReceiptDuration = "receipt_process_duration_seconds"
const mtrHistogramReceiptDurationDescription = "Duration of notify a transaction receipt"

type EventMetricsEmitter interface {
	EventStreamMetricsEmitter
	ConfirmationMetricsEmitter
}

type EventStreamMetricsEmitter interface {
	RecordEventRedetectedMetric(ctx context.Context)
}

type ConfirmationMetricsEmitter interface {
	RecordBlockHashBatchSizeMetric(ctx context.Context, size float64)
	RecordConfirmationMetrics(ctx context.Context, durationInSeconds float64)
	RecordReceiptMetrics(ctx context.Context, durationInSeconds float64)
	RecordBlockHashProcessMetrics(ctx context.Context, durationInSeconds float64)
	RecordBlockHashQueueingMetrics(ctx context.Context, durationInSeconds float64)
	RecordNotificationProcessMetrics(ctx context.Context, notificationType string, durationInSeconds float64)
	RecordNotificationQueueingMetrics(ctx context.Context, notificationType string, durationInSeconds float64)
	ReceiptCheckerMetricsEmitter
}

func (mm *metricsManager) RecordBlockHashBatchSizeMetric(ctx context.Context, size float64) {
	mm.eventsMetricsManager.SetGaugeMetric(ctx, metricsGaugeBlockHashBatchSize, size, nil)
}

type ReceiptCheckerMetricsEmitter interface {
	RecordReceiptCheckMetrics(ctx context.Context, status string, durationInSeconds float64)
}

func (mm *metricsManager) RecordReceiptCheckMetrics(ctx context.Context, status string, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetricWithLabels(ctx, mtrCounterReceiptCheckTotal, map[string]string{metricsLabelStatus: status}, nil)
	mm.eventsMetricsManager.ObserveHistogramMetricWithLabels(ctx, mtrHistogramReceiptCheckDuration, durationInSeconds, map[string]string{metricsLabelStatus: status}, nil)
}

func (mm *metricsManager) RecordBlockHashProcessMetrics(ctx context.Context, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetric(ctx, mtrCounterBlockHashProcessedTotal, nil)
	mm.eventsMetricsManager.ObserveHistogramMetric(ctx, mtrHistogramBlockHashProcessDuration, durationInSeconds, nil)
}

func (mm *metricsManager) RecordBlockHashQueueingMetrics(ctx context.Context, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetric(ctx, mtrCounterBlockHashQueuedTotal, nil)
	mm.eventsMetricsManager.ObserveHistogramMetric(ctx, mtrHistogramBlockHashQueueingDuration, durationInSeconds, nil)
}

func (mm *metricsManager) RecordNotificationProcessMetrics(ctx context.Context, notificationType string, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetricWithLabels(ctx, mtrCounterNotificationProcessedTotal, map[string]string{metricsLabelType: notificationType}, nil)
	mm.eventsMetricsManager.ObserveHistogramMetricWithLabels(ctx, mtrHistogramNotificationProcessDuration, durationInSeconds, map[string]string{metricsLabelType: notificationType}, nil)
}

func (mm *metricsManager) RecordNotificationQueueingMetrics(ctx context.Context, notificationType string, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetricWithLabels(ctx, mtrCounterNotificationQueuedTotal, map[string]string{metricsLabelType: notificationType}, nil)
	mm.eventsMetricsManager.ObserveHistogramMetricWithLabels(ctx, mtrHistogramNotificationQueueingDuration, durationInSeconds, map[string]string{metricsLabelType: notificationType}, nil)
}

func (mm *metricsManager) RecordConfirmationMetrics(ctx context.Context, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetric(ctx, mtrCounterConfirmedTotal, nil)
	mm.eventsMetricsManager.ObserveHistogramMetric(ctx, mtrHistogramConfirmDuration, durationInSeconds, nil)
}

func (mm *metricsManager) RecordEventRedetectedMetric(ctx context.Context) {
	mm.eventsMetricsManager.IncCounterMetric(ctx, mtrCounterEventRedetectedTotal, nil)
}

func (mm *metricsManager) RecordReceiptMetrics(ctx context.Context, durationInSeconds float64) {
	mm.eventsMetricsManager.IncCounterMetric(ctx, mtrCounterReceiptTotal, nil)
	mm.eventsMetricsManager.ObserveHistogramMetric(ctx, mtrHistogramReceiptDuration, durationInSeconds, nil)
}

func (mm *metricsManager) InitEventMetrics() {

	mm.eventsMetricsManager.NewGaugeMetric(mm.ctx, metricsGaugeBlockHashBatchSize, metricsGaugeBlockHashBatchSizeDescription, false)

	mm.eventsMetricsManager.NewCounterMetricWithLabels(mm.ctx, mtrCounterReceiptCheckTotal, mtrCounterReceiptCheckTotalDescription, []string{metricsLabelStatus}, false)
	mm.eventsMetricsManager.NewHistogramMetricWithLabels(mm.ctx, mtrHistogramReceiptCheckDuration, mtrHistogramReceiptCheckDurationDescription, []float64{} /*fallback to default buckets*/, []string{metricsLabelStatus}, false)

	mm.eventsMetricsManager.NewCounterMetricWithLabels(mm.ctx, mtrCounterNotificationQueuedTotal, mtrCounterNotificationQueuedTotalDescription, []string{metricsLabelType}, false)
	mm.eventsMetricsManager.NewHistogramMetricWithLabels(mm.ctx, mtrHistogramNotificationQueueingDuration, mtrHistogramNotificationQueueingDurationDescription, []float64{} /*fallback to default buckets*/, []string{metricsLabelType}, false)

	mm.eventsMetricsManager.NewCounterMetricWithLabels(mm.ctx, mtrCounterNotificationProcessedTotal, mtrCounterNotificationProcessedTotalDescription, []string{metricsLabelType}, false)
	mm.eventsMetricsManager.NewHistogramMetricWithLabels(mm.ctx, mtrHistogramNotificationProcessDuration, mtrHistogramNotificationProcessDurationDescription, []float64{} /*fallback to default buckets*/, []string{metricsLabelType}, false)

	mm.eventsMetricsManager.NewCounterMetric(mm.ctx, mtrCounterBlockHashProcessedTotal, mtrCounterBlockHashProcessedTotalDescription, false)
	mm.eventsMetricsManager.NewHistogramMetric(mm.ctx, mtrHistogramBlockHashProcessDuration, mtrHistogramBlockHashProcessDurationDescription, []float64{} /*fallback to default buckets*/, false)

	mm.eventsMetricsManager.NewCounterMetric(mm.ctx, mtrCounterBlockHashQueuedTotal, mtrCounterBlockHashQueuedTotalDescription, false)
	mm.eventsMetricsManager.NewHistogramMetric(mm.ctx, mtrHistogramBlockHashQueueingDuration, mtrHistogramBlockHashQueueingDurationDescription, []float64{} /*fallback to default buckets*/, false)

	mm.eventsMetricsManager.NewCounterMetric(mm.ctx, mtrCounterConfirmedTotal, mtrCounterConfirmedTotalDescription, false)
	mm.eventsMetricsManager.NewHistogramMetric(mm.ctx, mtrHistogramConfirmDuration, mtrHistogramConfirmDurationDescription, []float64{} /*fallback to default buckets*/, false)

	mm.eventsMetricsManager.NewCounterMetric(mm.ctx, mtrCounterEventRedetectedTotal, mtrCounterEventRedetectedTotalDescription, false)

	mm.eventsMetricsManager.NewCounterMetric(mm.ctx, mtrCounterReceiptTotal, mtrCounterReceiptTotalDescription, false)
	mm.eventsMetricsManager.NewHistogramMetric(mm.ctx, mtrHistogramReceiptDuration, mtrHistogramReceiptDurationDescription, []float64{} /*fallback to default buckets*/, false)

}
