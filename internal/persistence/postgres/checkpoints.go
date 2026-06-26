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
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func (p *sqlPersistence) newCheckpointCollection(forMigration bool) *dbsql.CrudBase[*apitypes.EventStreamCheckpoint] {
	collection := &dbsql.CrudBase[*apitypes.EventStreamCheckpoint]{
		DB:    p.db,
		Table: "checkpoints",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"listeners",
		},
		FilterFieldMap: map[string]string{
			"sequence": p.db.SequenceColumn(),
			"streamid": "id",
		},
		PatchDisabled: true,
		TimesDisabled: forMigration,
		NilValue:      func() *apitypes.EventStreamCheckpoint { return nil },
		NewInstance:   func() *apitypes.EventStreamCheckpoint { return &apitypes.EventStreamCheckpoint{} },
		GetFieldPtr: func(inst *apitypes.EventStreamCheckpoint, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				// The UUID of the stream is the ID column
				return &inst.StreamID
			case dbsql.ColumnCreated:
				// non-standard field name, for historical reasons
				return &inst.FirstCheckpoint
			case dbsql.ColumnUpdated:
				// non-standard field name, for historical reasons
				return &inst.Time
			case "listeners":
				return &inst.Listeners
			}
			return nil
		},
	}
	collection.Validate()
	return collection
}

func (p *sqlPersistence) WriteCheckpoint(ctx context.Context, checkpoint *apitypes.EventStreamCheckpoint) error {
	// Checkpoints are written as upserts optimized for existing, as apart from the very first one
	// they are a replace.
	_, err := p.checkpoints.Upsert(ctx, checkpoint, dbsql.UpsertOptimizationExisting)
	return err
}

func (p *sqlPersistence) GetCheckpoint(ctx context.Context, streamID *fftypes.UUID) (*apitypes.EventStreamCheckpoint, error) {
	return p.checkpoints.GetByID(ctx, streamID.String())
}

func (p *sqlPersistence) DeleteCheckpoint(ctx context.Context, streamID *fftypes.UUID) error {
	return p.checkpoints.Delete(ctx, streamID.String())
}
