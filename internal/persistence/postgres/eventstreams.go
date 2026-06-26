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

package postgres

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

func (p *sqlPersistence) newEventStreamsCollection(forMigration bool) *dbsql.CrudBase[*apitypes.EventStream] {
	collection := &dbsql.CrudBase[*apitypes.EventStream]{
		DB:    p.db,
		Table: "eventstreams",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"name",
			"suspended",
			"stream_type",
			"error_handling",
			"batch_size",
			"batch_timeout",
			"retry_timeout",
			"blocked_retry_timeout",
			"webhook_config",
			"websocket_config",
		},
		FilterFieldMap: map[string]string{
			"sequence":            p.db.SequenceColumn(),
			"type":                "stream_type",
			"errorhandling":       "error_handling",
			"batchsize":           "batch_size",
			"batchtimeout":        "batch_timeout",
			"retrytimeout":        "retry_timeout",
			"blockedretrytimeout": "blocked_retry_timeout",
			"webhook":             "webhook_config",
			"websocket":           "webhsocket_config",
		},
		TimesDisabled: forMigration,
		NilValue:      func() *apitypes.EventStream { return nil },
		NewInstance:   func() *apitypes.EventStream { return &apitypes.EventStream{} },
		GetFieldPtr: func(inst *apitypes.EventStream, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "name":
				return &inst.Name
			case "suspended":
				return &inst.Suspended
			case "stream_type":
				return &inst.Type
			case "error_handling":
				return &inst.ErrorHandling
			case "batch_size":
				return &inst.BatchSize
			case "batch_timeout":
				return &inst.BatchTimeout
			case "retry_timeout":
				return &inst.RetryTimeout
			case "blocked_retry_timeout":
				return &inst.BlockedRetryDelay
			case "webhook_config":
				return &inst.Webhook
			case "websocket_config":
				return &inst.WebSocket
			}
			return nil
		},
	}
	collection.Validate()
	return collection
}

func (p *sqlPersistence) NewStreamFilter(ctx context.Context) ffapi.FilterBuilder {
	return persistence.EventStreamFilters.NewFilter(ctx)
}

func (p *sqlPersistence) ListStreams(ctx context.Context, filter ffapi.AndFilter) ([]*apitypes.EventStream, *ffapi.FilterResult, error) {
	return p.eventStreams.GetMany(ctx, filter)
}

func (p *sqlPersistence) ListStreamsByCreateTime(ctx context.Context, after *fftypes.UUID, limit int, dir txhandler.SortDirection) ([]*apitypes.EventStream, error) {
	var afterSeq *int64
	if after != nil {
		seq, err := p.eventStreams.GetSequenceForID(ctx, after.String())
		if err != nil {
			return nil, err
		}
		afterSeq = &seq
	}
	filter := p.seqAfterFilter(ctx, persistence.EventStreamFilters, afterSeq, limit, dir)
	streams, _, err := p.eventStreams.GetMany(ctx, filter)
	return streams, err
}

func (p *sqlPersistence) GetStream(ctx context.Context, streamID *fftypes.UUID) (*apitypes.EventStream, error) {
	return p.eventStreams.GetByID(ctx, streamID.String())
}

func (p *sqlPersistence) WriteStream(ctx context.Context, spec *apitypes.EventStream) error {
	_, err := p.eventStreams.Upsert(ctx, spec, dbsql.UpsertOptimizationNew)
	return err
}

func (p *sqlPersistence) DeleteStream(ctx context.Context, streamID *fftypes.UUID) error {
	return p.eventStreams.Delete(ctx, streamID.String())
}
