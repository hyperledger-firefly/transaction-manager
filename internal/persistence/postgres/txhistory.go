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

package postgres

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func (p *sqlPersistence) newTXHistoryCollection() *dbsql.CrudBase[*apitypes.TXHistoryRecord] {
	collection := &dbsql.CrudBase[*apitypes.TXHistoryRecord]{
		DB:    p.db,
		Table: "txhistory",
		Columns: []string{
			dbsql.ColumnID,
			"tx_id",
			"status",
			"action",
			"count",
			"time",
			"last_occurrence",
			"error",
			"error_time",
			"info",
		},
		FilterFieldMap: map[string]string{
			"sequence":       p.db.SequenceColumn(),
			"transaction":    "tx_id",
			"substatus":      "status",
			"lastoccurrence": "last_occurrence",
			"lasterror":      "error",
			"lasterrortime":  "error_time",
			"lastinfo":       "info",
			"occurrences":    "count",
		},
		PatchDisabled: true,
		TimesDisabled: true,
		NilValue:      func() *apitypes.TXHistoryRecord { return nil },
		NewInstance:   func() *apitypes.TXHistoryRecord { return &apitypes.TXHistoryRecord{} },
		GetFieldPtr: func(inst *apitypes.TXHistoryRecord, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case "tx_id":
				return &inst.TransactionID
			case "status":
				return &inst.SubStatus
			case "action":
				return &inst.Action
			case "count":
				return &inst.OccurrenceCount
			case "time":
				return &inst.Time
			case "last_occurrence":
				return &inst.LastOccurrence
			case "error":
				return &inst.LastError
			case "error_time":
				return &inst.LastErrorTime
			case "info":
				return &inst.LastInfo
			}
			return nil
		},
	}
	collection.Validate()
	return collection
}

func (p *sqlPersistence) NewTxHistoryFilter(ctx context.Context) ffapi.FilterBuilder {
	return persistence.TXHistoryFilters.NewFilter(ctx)
}

func (p *sqlPersistence) ListTransactionHistory(ctx context.Context, txID string, filter ffapi.AndFilter) ([]*apitypes.TXHistoryRecord, *ffapi.FilterResult, error) {
	return p.txHistory.GetMany(ctx, filter.Condition(filter.Builder().Eq("transaction", txID)))
}

func (p *sqlPersistence) AddSubStatusAction(ctx context.Context, txID string, subStatus apitypes.TxSubStatus, action apitypes.TxAction, info *fftypes.JSONAny, errInfo *fftypes.JSONAny, actionOccurred *fftypes.FFTime) error {
	// Dispatch to TX writer

	op := newTransactionOperation(txID)
	op.historyRecord = &apitypes.TXHistoryRecord{
		ID:            fftypes.NewUUID(),
		TransactionID: txID,
		SubStatus:     subStatus,
		TxHistoryActionEntry: apitypes.TxHistoryActionEntry{
			OccurrenceCount: 1,
			Time:            actionOccurred,
			LastOccurrence:  actionOccurred,
			Action:          action,
			LastInfo:        persistence.JSONOrString(info),    // guard against bad JSON
			LastError:       persistence.JSONOrString(errInfo), // guard against bad JSON
		},
	}
	if errInfo != nil {
		op.historyRecord.LastErrorTime = actionOccurred
	}
	p.writer.queue(ctx, op)
	return nil // completely async
}

func (p *sqlPersistence) compressHistory(ctx context.Context, txID string) error {
	result, err := p.buildHistorySummary(ctx, txID, false, 0, func(from, to *apitypes.TXHistoryRecord) error {
		update := persistence.TXHistoryFilters.NewUpdate(ctx).
			Set("occurrences", to.OccurrenceCount+1). // increment the count
			Set("time", from.Time)                    // move the time on the newer record to be the time of the older record merged in
		if err := p.txHistory.Update(ctx, to.ID.String(), update); err != nil {
			return err
		}
		return p.txHistory.Delete(ctx, from.ID.String())
	})
	if err != nil {
		return err
	}
	log.L(ctx).Infof("Compressed history for %s complete: before=%d after=%d", txID, result.recordsBefore, result.recordsAfter)
	return nil
}

type historyResult struct {
	entries       []*apitypes.TxHistoryStateTransitionEntry
	recordsBefore int
	recordsAfter  int
}

// buildHistorySummary builds a compressed summary of actions, grouped within subStatus changes, and with redundant actions removed.
func (p *sqlPersistence) buildHistorySummary(ctx context.Context, txID string, buildResult bool, resultLimit int, persistMerge func(from, to *apitypes.TXHistoryRecord) error) (*historyResult, error) {
	r := &historyResult{}
	if buildResult {
		r.entries = []*apitypes.TxHistoryStateTransitionEntry{}
	}
	skip := 0
	pageSize := 50
	var lastRecordSameSubStatus *apitypes.TXHistoryRecord
	for {
		filter := persistence.TXHistoryFilters.
			//nolint:gosec // Safe conversion as pageSize is always positive
			NewFilterLimit(ctx, uint64(pageSize)).Eq("transaction", txID).
			//nolint:gosec // Safe conversion as skip is always positive
			Skip(uint64(skip))
		page, _, err := p.txHistory.GetMany(ctx, filter)
		if err != nil {
			return nil, err
		}
		for _, h := range page {
			if lastRecordSameSubStatus == nil || lastRecordSameSubStatus.SubStatus != h.SubStatus {
				lastRecordSameSubStatus = nil // we've changed subStatus
				if buildResult {
					r.entries = append(r.entries, &apitypes.TxHistoryStateTransitionEntry{
						Time:    h.Time,
						Status:  h.SubStatus,
						Actions: []*apitypes.TxHistoryActionEntry{},
					})
				}
			}
			r.recordsBefore++
			if lastRecordSameSubStatus == nil || lastRecordSameSubStatus.Action != h.Action {
				// Actions are also in descending order
				if buildResult {
					actionEntry := &apitypes.TxHistoryActionEntry{
						Time:            h.Time,
						LastOccurrence:  h.LastOccurrence,
						Action:          h.Action,
						OccurrenceCount: h.OccurrenceCount,
						LastInfo:        h.LastInfo,
						LastError:       h.LastError,
						LastErrorTime:   h.LastErrorTime,
					}
					statusEntry := r.entries[len(r.entries)-1]
					statusEntry.Actions = append(statusEntry.Actions, actionEntry)
				}
				// We've moved forwards
				r.recordsAfter++
				lastRecordSameSubStatus = h
			} else {
				// We can compress these records together. We might have a callback to persist this, if we're
				// running this function as part of a history compression.
				if persistMerge != nil {
					if err := persistMerge(h, lastRecordSameSubStatus); err != nil {
						log.L(ctx).Errorf("Merging status record %s into %s failed: %s", h.ID, lastRecordSameSubStatus.ID, err)
						return nil, err
					}
				}
				lastRecordSameSubStatus.OccurrenceCount++
				if buildResult {
					statusEntry := r.entries[len(r.entries)-1]
					actionEntry := statusEntry.Actions[len(statusEntry.Actions)-1]
					actionEntry.OccurrenceCount++
					actionEntry.Time = h.Time
				}
			}
		}
		// Done with this page
		skip += pageSize
		// We return when we run out of input, or we hit the target maximum output records
		if len(page) != pageSize || (resultLimit > 0 && len(r.entries) >= resultLimit) {
			return r, nil
		}
	}

}
