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
	"database/sql"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence/postgres"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/stretchr/testify/assert"
)

func initPSQL(t *testing.T) (context.Context, func()) {
	tmconfig.Reset()

	// Configure a test postgres
	ctx, cancelCtx := context.WithCancel(context.Background())

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

	tmconfig.PostgresSection.Set(dbsql.SQLConfDatasourceURL, dbURL(utdbName))
	tmconfig.PostgresSection.Set(dbsql.SQLConfMigrationsDirectory, path.Join("..", "..", "db", "migrations", "postgres"))

	psql := &postgres.Postgres{}
	err = psql.Database.Init(ctx, psql, tmconfig.PostgresSection)
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

	return ctx, func() {
		cancelCtx()
		err := m.Drop()
		assert.NoError(t, err)
	}
}

func TestMigrateLevelDBToPostgres(t *testing.T) {
	ctx, done := initPSQL(t)
	defer done()

	// Configure a test LevelDB
	dir := t.TempDir()
	config.Set(tmconfig.PersistenceLevelDBPath, dir)

	// Run empty migration and check no errors
	err := MigrateLevelDBToPostgres(ctx)
	assert.NoError(t, err)
}

func TestMigrateLevelDBToPostgresFailPSQL(t *testing.T) {
	tmconfig.Reset()

	// Configure a test LevelDB
	dir := t.TempDir()
	config.Set(tmconfig.PersistenceLevelDBPath, dir)

	err := MigrateLevelDBToPostgres(context.Background())
	assert.Regexp(t, "FF21049", err)
}

func TestMigrateLevelDBToPostgresFailLevelDB(t *testing.T) {
	tmconfig.Reset()
	err := MigrateLevelDBToPostgres(context.Background())
	assert.Regexp(t, "FF21049", err)
}
