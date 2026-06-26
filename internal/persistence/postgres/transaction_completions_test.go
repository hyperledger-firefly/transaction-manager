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
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionCompletionsLockFail(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectBegin()
	mdb.ExpectExec("acquire lock txcompletions").WillReturnError(fmt.Errorf("pop"))

	err := p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		return p.writeTransactionCompletions(ctx, []*apitypes.TXCompletion{
			{
				ID:     "id1",
				Time:   fftypes.Now(),
				Status: apitypes.TxStatusSucceeded,
			},
		})
	})
	assert.Regexp(t, "FF00187.*pop", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestTransactionCompletionsInsertFail(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectBegin()
	mdb.ExpectExec("acquire lock txcompletions").WillReturnResult(driver.ResultNoRows)
	mdb.ExpectExec("INSERT.*transaction_completions").WillReturnError(fmt.Errorf("pop"))

	err := p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		return p.writeTransactionCompletions(ctx, []*apitypes.TXCompletion{
			{
				ID:     "id1",
				Time:   fftypes.Now(),
				Status: apitypes.TxStatusSucceeded,
			},
			{
				ID:     "id2",
				Time:   fftypes.Now(),
				Status: apitypes.TxStatusSucceeded,
			},
		})
	})
	assert.Regexp(t, "FF00245.*pop", err)

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestTransactionCompletionsInsertNotify(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	timeBeforePoll := time.Now()
	waiterDone := make(chan struct{})
	go func() {
		assert.True(t, p.WaitTxCompletionUpdates(ctx, timeBeforePoll))
		close(waiterDone)
	}()

	waiters := 0
	for waiters == 0 {
		p.txCompletionsLock.Lock()
		waiters = len(p.txCompletionsWaiters)
		p.txCompletionsLock.Unlock()
	}

	mdb.ExpectBegin()
	mdb.ExpectExec("acquire lock txcompletions").WillReturnResult(driver.ResultNoRows)
	mdb.ExpectExec("INSERT.*transaction_completions").WillReturnResult(driver.ResultNoRows)
	mdb.ExpectCommit()

	err := p.db.RunAsGroup(ctx, func(ctx context.Context) error {
		return p.writeTransactionCompletions(ctx, []*apitypes.TXCompletion{
			{
				ID:     "id1",
				Time:   fftypes.Now(),
				Status: apitypes.TxStatusSucceeded,
			},
		})
	})
	require.NoError(t, err)

	<-waiterDone

	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestTransactionCompletionsWakeNotifyOnClose(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, _, done := newMockSQLPersistence(t)
	defer done()

	p.notifyTxCompletions()

	ctx, cancelCtx := context.WithCancel(ctx)
	cancelCtx()

	require.False(t, p.WaitTxCompletionUpdates(ctx, time.Now()))
}

func TestTransactionCompletionsQueryFail(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*transaction_completions").WillReturnError(fmt.Errorf("pop"))

	_, err := p.ListTxCompletionsByCreateTime(ctx, nil, 10, txhandler.SortDirectionAscending)
	require.Regexp(t, "FF00176.*pop", err)
}

func TestTransactionCompletionsScanFail(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx, p, mdb, done := newMockSQLPersistence(t)
	defer done()

	mdb.ExpectQuery("SELECT.*transaction_completions").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(false))

	_, err := p.TransactionCompletions().ListTxCompletionsByCreateTime(ctx, nil, 10, txhandler.SortDirectionAscending)
	require.Regexp(t, "FF00182", err)
}
