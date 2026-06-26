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
	"database/sql"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func initTestPSQL(t *testing.T, initFunc ...func(config.Section)) (context.Context, *sqlPersistence, *migrate.Migrate, func()) {

	config.RootConfigReset()
	ctx, cancelCtx := context.WithCancel(context.Background())
	dbconf := config.RootSection("utdb")
	InitConfig(dbconf)
	for _, f := range initFunc {
		f(dbconf)
	}

	dbURL := func(dbname string) string {
		hostname := os.Getenv("POSTGRES_HOSTNAME")
		port := os.Getenv("POSTGRES_PORT")
		password := os.Getenv("POSTGRES_PASSWORD")
		if hostname == "" {
			hostname = "localhost"
		}
		if port == "" {
			port = "5432"
		}
		if password == "" {
			password = "f1refly"
		}
		return fmt.Sprintf("postgres://postgres:%s@%s:%s/%s?sslmode=disable", password, hostname, port, dbname)
	}
	utdbName := "ut_" + fftypes.NewUUID().String()

	// First create the database - using the super user
	adminDB, err := sql.Open("postgres", dbURL("postgres"))
	assert.NoError(t, err)
	_, err = adminDB.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, utdbName))
	assert.NoError(t, err)
	err = adminDB.Close()
	assert.NoError(t, err)

	dbconf.Set(dbsql.SQLConfDatasourceURL, dbURL(utdbName))
	dbconf.Set(dbsql.SQLConfMigrationsDirectory, path.Join("..", "..", "db", "migrations", "postgres"))

	p, err := NewPostgresPersistence(ctx, dbconf, 1*time.Hour)
	assert.NoError(t, err)

	driver, err := psql.GetMigrationDriver(psql.DB())
	assert.NoError(t, err)
	m, err := migrate.NewWithDatabaseInstance(
		"file://../../../db/migrations/postgres",
		utdbName,
		driver,
	)
	assert.NoError(t, err)

	err = m.Up()
	assert.NoError(t, err)

	return ctx, p.(*sqlPersistence), m, func() {
		cancelCtx()
		psql.Close()
		err := m.Drop()
		assert.NoError(t, err)
	}
}

func TestPSQLDownMigrations(t *testing.T) {

	ctx, _, m, done := initTestPSQL(t)
	defer done()

	// Test locking
	txctx, tx, ac, err := psql.BeginOrUseTx(ctx)
	assert.NoError(t, err)
	err = psql.AcquireLockTx(txctx, "mylock", tx)
	assert.NoError(t, err)
	psql.RollbackTx(ctx, tx, ac)

	// test down migration (up migration is in initTestPSQL)
	err = m.Down()
	assert.NoError(t, err)

}

func TestDBInitFail(t *testing.T) {

	config.RootConfigReset()
	dbconf := config.RootSection("utdb")
	InitConfig(dbconf)

	_, err := NewPostgresPersistence(context.Background(), dbconf, 1*time.Hour)
	assert.Regexp(t, "FF00183", err)

}
