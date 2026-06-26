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

package cmd

import (
	"context"

	"github.com/hyperledger-firefly/transaction-manager/internal/persistence/dbmigration"
	"github.com/spf13/cobra"
)

func MigrateCommand(initConfig func() error) *cobra.Command {
	return buildMigrateCommand(initConfig)
}

func buildMigrateCommand(initConfig func() error) *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate <subcommand>",
		Short: "Migration tools",
	}
	migrateCmd.AddCommand(buildLeveldb2postgresCommand(initConfig))

	return migrateCmd
}

func buildLeveldb2postgresCommand(initConfig func() error) *cobra.Command {
	leveldb2postgresEventStreamsCmd := &cobra.Command{
		Use:   "leveldb2postgres",
		Short: "Migrate from LevelDB to PostgreSQL persistence",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := initConfig(); err != nil {
				return err
			}
			return dbmigration.MigrateLevelDBToPostgres(context.Background())
		},
	}
	return leveldb2postgresEventStreamsCmd
}
