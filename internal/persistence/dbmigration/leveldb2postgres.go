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
	"time"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence/leveldb"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence/postgres"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
)

func MigrateLevelDBToPostgres(ctx context.Context) (err error) {
	m := &dbMigration{}

	tmconfig.PostgresSection.Set(postgres.ConfigTXWriterBatchTimeout, 0) // single go-routine, no point in batching
	tmconfig.PostgresSection.Set(postgres.ConfigTXWriterCount, 1)
	nonceStateTimeout := 0 * time.Second
	if m.source, err = leveldb.NewLevelDBPersistence(ctx, nonceStateTimeout); err != nil {
		return i18n.NewError(ctx, tmmsgs.MsgPersistenceInitFail, "leveldb", err)
	}
	defer m.source.Close(ctx)
	if m.target, err = postgres.NewPostgresPersistence(ctx, tmconfig.PostgresSection, nonceStateTimeout, postgres.ForMigration); err != nil {
		return i18n.NewError(ctx, tmmsgs.MsgPersistenceInitFail, "postgres", err)
	}
	defer m.target.Close(ctx)

	return m.run(ctx)
}
