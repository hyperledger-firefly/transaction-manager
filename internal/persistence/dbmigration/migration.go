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

package dbmigration

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

const (
	paginationLimit = 50
)

type dbMigration struct {
	source persistence.Persistence
	target persistence.Persistence
}

func (m *dbMigration) run(ctx context.Context) error {

	if err := m.migrateEventStreams(ctx); err != nil {
		return err
	}

	if err := m.migrateListeners(ctx); err != nil {
		return err
	}

	return m.migrateTransactions(ctx)

}

func (m *dbMigration) migrateEventStreams(ctx context.Context) error {

	log.L(ctx).Infof("Migrating event streams")
	var after *fftypes.UUID
	count := 0
	for {
		page, err := m.source.ListStreamsByCreateTime(ctx, after, paginationLimit, txhandler.SortDirectionAscending)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			log.L(ctx).Infof("Migrated %d event streams", count)
			return nil
		}
		for _, es := range page {
			if err := m.migrateEventStream(ctx, es); err != nil {
				return err
			}
			count++
		}
		after = page[len(page)-1].ID
	}

}

func (m *dbMigration) migrateEventStream(ctx context.Context, es *apitypes.EventStream) error {
	log.L(ctx).Infof("Migrating event stream %s", es.ID)

	existingES, err := m.target.GetStream(ctx, es.ID)
	if err != nil {
		return err
	}
	if existingES == nil {
		if es.Created == nil {
			es.Created = fftypes.Now()
		}
		if es.Updated == nil {
			es.Updated = es.Created
		}
		log.L(ctx).Infof("Writing event stream %s to target", es.ID)
		if err := m.target.WriteStream(ctx, es); err != nil {
			return err
		}
	}

	cp, err := m.source.GetCheckpoint(ctx, es.ID)
	if err != nil {
		return err
	}
	existingCP, err := m.target.GetCheckpoint(ctx, es.ID)
	if err != nil {
		return err
	}
	if cp != nil && existingCP == nil {
		// LevelDB didn't have timestamps in checkpoints
		if cp.FirstCheckpoint == nil {
			cp.FirstCheckpoint = fftypes.Now()
		}
		if cp.Time == nil {
			cp.Time = cp.FirstCheckpoint
		}
		log.L(ctx).Infof("Writing checkpoint %s to target", cp.StreamID)
		if err := m.target.WriteCheckpoint(ctx, cp); err != nil {
			return err
		}
	}
	return nil
}

func (m *dbMigration) migrateListeners(ctx context.Context) error {

	log.L(ctx).Infof("Migrating listeners")
	var after *fftypes.UUID
	count := 0
	for {
		page, err := m.source.ListListenersByCreateTime(ctx, after, paginationLimit, txhandler.SortDirectionAscending)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			log.L(ctx).Infof("Migrated %d listeners", count)
			return nil
		}
		for _, l := range page {
			if err := m.migrateListener(ctx, l); err != nil {
				return err
			}
			count++
		}
		after = page[len(page)-1].ID
	}

}

func (m *dbMigration) migrateListener(ctx context.Context, l *apitypes.Listener) error {
	log.L(ctx).Infof("Migrating listener %s", l.ID)

	existingL, err := m.target.GetListener(ctx, l.ID)
	if err != nil {
		return err
	}
	if existingL == nil {
		log.L(ctx).Infof("Writing listener %s to target", l.ID)
		if l.Created == nil {
			l.Created = fftypes.Now()
		}
		if l.Updated == nil {
			l.Updated = l.Created
		}
		if err := m.target.WriteListener(ctx, l); err != nil {
			return err
		}
	}

	return err
}

func (m *dbMigration) migrateTransactions(ctx context.Context) error {

	log.L(ctx).Infof("Migrating transactions")
	var after *apitypes.ManagedTX
	count := 0
	for {
		page, err := m.source.ListTransactionsByCreateTime(ctx, after, paginationLimit, txhandler.SortDirectionAscending)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			log.L(ctx).Infof("Migrated %d transactions", count)
			return nil
		}
		for _, mtx := range page {
			if err := m.migrateTransaction(ctx, mtx); err != nil {
				return err
			}
			count++
		}
		after = page[len(page)-1]
	}

}

func (m *dbMigration) migrateTransaction(ctx context.Context, mtx *apitypes.ManagedTX) error {
	log.L(ctx).Infof("Migrating transaction %s", mtx.ID)

	tx, err := m.source.GetTransactionByIDWithStatus(ctx, mtx.ID, false /* we do not attempt to migrate the history */)
	if err != nil {
		return err
	}

	existingTX, err := m.target.GetTransactionByID(ctx, tx.ID)
	if err != nil {
		return err
	}
	if existingTX == nil {
		log.L(ctx).Infof("Writing transaction %s to target", tx.ID)
		if tx.Created == nil {
			tx.Created = fftypes.Now()
		}
		if tx.Updated == nil {
			tx.Updated = tx.Created
		}
		if err := m.target.InsertTransactionPreAssignedNonce(ctx, tx.ManagedTX); err != nil {
			return err
		}

		if tx.Receipt != nil {
			log.L(ctx).Infof("Writing transaction receipt for %s to target", tx.ID)
			if err := m.target.SetTransactionReceipt(ctx, tx.ID, tx.Receipt); err != nil {
				return err
			}
		}

		if len(tx.Confirmations) > 0 {
			log.L(ctx).Infof("Writing %d transaction confirmations for %s to target", len(tx.Confirmations), tx.ID)
			if err := m.target.AddTransactionConfirmations(ctx, tx.ID, true, tx.Confirmations...); err != nil {
				return err
			}

		}
	}

	return err
}
