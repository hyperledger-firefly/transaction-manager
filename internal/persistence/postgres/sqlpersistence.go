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
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

const (
	ConfigTXWriterCacheSlots                = "txwriter.cacheSlots"
	ConfigTXWriterHistorySummaryLimit       = "txwriter.historySummaryLimit"
	ConfigTXWriterHistoryCompactionInterval = "txwriter.historyCompactionInterval"
	ConfigTXWriterCount                     = "txwriter.count"
	ConfigTXWriterBatchTimeout              = "txwriter.batchTimeout"
	ConfigTXWriterBatchSize                 = "txwriter.batchSize"

	defaultConnectionLimitPostgreSQL = 50
)

type sqlPersistence struct {
	db     *dbsql.Database
	writer *transactionWriter

	transactions  *dbsql.CrudBase[*apitypes.ManagedTX]
	checkpoints   *dbsql.CrudBase[*apitypes.EventStreamCheckpoint]
	confirmations *dbsql.CrudBase[*apitypes.ConfirmationRecord]
	receipts      *dbsql.CrudBase[*apitypes.ReceiptRecord]
	txHistory     *dbsql.CrudBase[*apitypes.TXHistoryRecord]
	eventStreams  *dbsql.CrudBase[*apitypes.EventStream]
	listeners     *dbsql.CrudBase[*apitypes.Listener]

	historySummaryLimit int
	nonceStateTimeout   time.Duration

	txCompletionsLock    sync.Mutex
	txCompletionsTime    time.Time
	txCompletionsWaiters []chan struct{}
}

// InitConfig gets called after config reset to initialize the config structure
func InitConfig(conf config.Section) {
	psql = &Postgres{}
	psql.Database.InitConfig(psql, conf)
	conf.SetDefault(dbsql.SQLConfMaxConnections, defaultConnectionLimitPostgreSQL)

	conf.AddKnownKey(ConfigTXWriterCacheSlots, 1000)
	conf.AddKnownKey(ConfigTXWriterHistorySummaryLimit, 50) // returned on TX status
	conf.AddKnownKey(ConfigTXWriterHistoryCompactionInterval, "0" /* disabled by default */)
	conf.AddKnownKey(ConfigTXWriterCount, 5)
	conf.AddKnownKey(ConfigTXWriterBatchTimeout, "10ms")
	conf.AddKnownKey(ConfigTXWriterBatchSize, 100)
}

func newSQLPersistence(bgCtx context.Context, db *dbsql.Database, conf config.Section, nonceStateTimeout time.Duration, codeOptions ...CodeUsageOptions) (p *sqlPersistence, err error) {
	p = &sqlPersistence{
		db: db,
	}

	forMigration := false
	for _, co := range codeOptions {
		if co == ForMigration {
			forMigration = true
		}
	}

	p.eventStreams = p.newEventStreamsCollection(forMigration)
	p.checkpoints = p.newCheckpointCollection(forMigration)
	p.listeners = p.newListenersCollection(forMigration)
	p.transactions = p.newTransactionCollection(forMigration)
	p.confirmations = p.newConfirmationsCollection()
	p.receipts = p.newReceiptsCollection()
	p.txHistory = p.newTXHistoryCollection()

	p.historySummaryLimit = conf.GetInt(ConfigTXWriterHistorySummaryLimit)
	p.nonceStateTimeout = nonceStateTimeout

	if p.writer, err = newTransactionWriter(bgCtx, p, conf); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *sqlPersistence) RichQuery() persistence.RichQuery {
	return p
}

func (p *sqlPersistence) TransactionCompletions() persistence.TransactionCompletions {
	return p
}

func (p *sqlPersistence) seqAfterFilter(ctx context.Context, qf *ffapi.QueryFields, after *int64, limit int, dir txhandler.SortDirection, conditions ...ffapi.Filter) (filter ffapi.Filter) {
	//nolint:gosec // Safe conversion as limit is always positive
	fb := qf.NewFilterLimit(ctx, uint64(limit))
	if after != nil {
		if dir == txhandler.SortDirectionDescending {
			conditions = append(conditions, fb.Lt("sequence", *after))
		} else {
			conditions = append(conditions, fb.Gt("sequence", *after))
		}
	}
	filter = fb.And(conditions...)
	if dir == txhandler.SortDirectionDescending {
		filter = filter.Sort("-sequence")
	} else {
		filter = filter.Sort("sequence")
	}
	return filter
}

func (p *sqlPersistence) Close(_ context.Context) {
	// Quiesce the writers first - will flush out in-flight
	p.writer.stop()
	// Then close the DB
	p.db.Close()
}
