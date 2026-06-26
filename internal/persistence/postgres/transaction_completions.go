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
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

// This is deliberately an optimized lower level SQL function rather than a full CRUD.
// These are very lightweight records designed only to back an event stream.
func (p *sqlPersistence) writeTransactionCompletions(ctx context.Context, txCompletions []*apitypes.TXCompletion) error {

	if len(txCompletions) == 0 {
		return nil
	}

	tx := dbsql.GetTXFromContext(ctx)

	// We need a lock to ensure that if our transaction allocates sequences, then those numbered sequences will
	// be committed in the same order as the transactions to the DB.
	// This is critical for code that is polling the table to be able to track a single increasing sequence
	// (noting gaps in the sequence can still occur for rollbacks).
	err := p.db.AcquireLockTx(ctx, "txcompletions", tx)
	if err != nil {
		return err
	}

	// Build and execute the SQL as a single statement
	queryString := new(strings.Builder)
	queryString.WriteString("INSERT INTO transaction_completions (id, time, status) VALUES ")
	values := make([]any, len(txCompletions)*3)
	for i, txc := range txCompletions {
		if i > 0 {
			queryString.WriteRune(',')
		}
		iBase := i * 3
		queryString.WriteString(fmt.Sprintf("($%d,$%d,$%d)", iBase+1, iBase+2, iBase+3))
		values[iBase+0] = txc.ID
		values[iBase+1] = txc.Time
		values[iBase+2] = txc.Status
	}
	queryString.WriteString(" ON CONFLICT DO NOTHING")
	_, err = p.db.ExecTx(ctx, "transaction_completions", tx, queryString.String(), values)

	tx.AddPostCommitHook(p.notifyTxCompletions)

	return err

}

// We only support a single very constrained query currently against the transaction completions,
// designed to allow you to paginate through. You can use WaitTxCompletionUpdates once you run out of
// updates to wait for new ones to arrive.
func (p *sqlPersistence) ListTxCompletionsByCreateTime(ctx context.Context, after *int64, limit int, dir txhandler.SortDirection) ([]*apitypes.TXCompletion, error) {

	q := sq.Select("seq", "id", "time", "status").From("transaction_completions")
	if dir == txhandler.SortDirectionAscending {
		if after != nil {
			q = q.Where("seq > ?", *after)
		}
		q = q.OrderBy("seq ASC")
	} else {
		if after != nil {
			q = q.Where("seq < ?", *after)
		}
		q = q.OrderBy("seq DESC")
	}
	//nolint:gosec // Safe conversion as limit is always positive
	q = q.Limit(uint64(limit))
	rows, _, err := p.db.QueryTx(ctx, "transaction_completions", nil, q)
	if err != nil {
		return nil, err
	}
	results := make([]*apitypes.TXCompletion, 0, limit)
	for rows.Next() {
		var res apitypes.TXCompletion
		if err := rows.Scan(&res.Sequence, &res.ID, &res.Time, &res.Status); err != nil {
			return nil, i18n.WrapError(ctx, err, i18n.MsgDBReadErr, "transaction_completions")
		}
		results = append(results, &res)
	}
	return results, err

}

func (p *sqlPersistence) notifyTxCompletions() {
	p.txCompletionsLock.Lock()
	defer p.txCompletionsLock.Unlock()
	p.txCompletionsTime = time.Now()
	for _, c := range p.txCompletionsWaiters {
		close(c)
	}
	p.txCompletionsWaiters = nil
}

func (p *sqlPersistence) WaitTxCompletionUpdates(ctx context.Context, timeBeforeLastPoll time.Time) bool {
	// Possible there's already an update by the time we arrived locked in here...
	p.txCompletionsLock.Lock()
	var waitChl chan struct{}
	if timeBeforeLastPoll.After(p.txCompletionsTime) {
		// ... if there isn't already updates, then register a new waiter
		waitChl = make(chan struct{})
		p.txCompletionsWaiters = append(p.txCompletionsWaiters, waitChl)
	}
	p.txCompletionsLock.Unlock()

	if waitChl != nil {
		select {
		case <-waitChl:
		case <-ctx.Done():
			return false
		}
	}
	return true
}
