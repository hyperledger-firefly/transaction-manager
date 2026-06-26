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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func newMockSQLPersistence(t *testing.T, init ...func(dbconf config.Section)) (context.Context, *sqlPersistence, sqlmock.Sqlmock, func()) {

	ctx, cancelCtx := context.WithCancel(context.Background())
	db, dbm := dbsql.NewMockProvider().UTInit()

	config.RootConfigReset()
	dbconf := config.RootSection("utdb")
	InitConfig(dbconf)
	for _, fn := range init {
		fn(dbconf)
	}

	p, err := newSQLPersistence(ctx, &db.Database, dbconf, 1*time.Hour)
	assert.NoError(t, err)

	assert.NotNil(t, p.RichQuery())

	return ctx, p, dbm, func() {
		p.Close(ctx)
		cancelCtx()
	}

}

func TestNewSQLPersistenceForMigration(t *testing.T) {

	db, _ := dbsql.NewMockProvider().UTInit()

	config.RootConfigReset()
	dbconf := config.RootSection("utdb")
	InitConfig(dbconf)

	p, err := newSQLPersistence(context.Background(), &db.Database, dbconf, 1*time.Hour, ForMigration)
	assert.NoError(t, err)
	defer p.Close(context.Background())

	assert.True(t, p.transactions.TimesDisabled)
	assert.True(t, p.eventStreams.TimesDisabled)
	assert.True(t, p.listeners.TimesDisabled)

}

func TestNewSQLPersistenceTXWriterFail(t *testing.T) {

	db, _ := dbsql.NewMockProvider().UTInit()

	config.RootConfigReset()
	dbconf := config.RootSection("utdb")
	InitConfig(dbconf)
	dbconf.Set(ConfigTXWriterCacheSlots, -1)

	_, err := newSQLPersistence(context.Background(), &db.Database, dbconf, 1*time.Hour)
	assert.Regexp(t, "must provide a positive size", err)

}

func strPtr(s string) *string {
	return &s
}

func u64Ptr(i uint64) *uint64 {
	return &i
}

func ffDurationPtr(d time.Duration) *fftypes.FFDuration {
	return (*fftypes.FFDuration)(&d)
}
