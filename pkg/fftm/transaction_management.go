// Copyright © 2023 - 2025 Kaleido, Inc.
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

package fftm

import (
	"context"
	"net/http"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

func (m *manager) ReconcileConfirmationsForTransaction(ctx context.Context, txHash string, existingConfirmations []*ffcapi.MinimalBlockInfo, targetConfirmationCount uint64) (*ffcapi.ConfirmationUpdateResult, error) {
	return m.connector.ReconcileConfirmationsForTransaction(ctx, txHash, existingConfirmations, targetConfirmationCount)
}

func (m *manager) GetTransactionByIDWithStatus(ctx context.Context, txID string, withHistory bool) (transaction *apitypes.TXWithStatus, err error) {
	tx, err := m.persistence.GetTransactionByIDWithStatus(ctx, txID, withHistory)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, i18n.NewError(ctx, tmmsgs.MsgTransactionNotFound, txID)
	}
	return tx, nil
}

func (m *manager) getTransactions(ctx context.Context, afterStr, limitStr, signer string, pending bool, dirString string) (transactions []*apitypes.ManagedTX, err error) {
	limit, err := m.parseLimit(ctx, limitStr)
	if err != nil {
		return nil, err
	}
	var dir txhandler.SortDirection
	switch strings.ToLower(dirString) {
	case "", "desc", "descending":
		dir = txhandler.SortDirectionDescending // descending is default
	case "asc", "ascending":
		dir = txhandler.SortDirectionAscending
	default:
		return nil, i18n.NewError(ctx, tmmsgs.MsgInvalidSortDirection, dirString)
	}
	var afterTx *apitypes.ManagedTX
	if afterStr != "" {
		// Get the transaction, as we need this to exist to pick the right field depending on the index that's been chosen
		afterTx, err = m.persistence.GetTransactionByID(ctx, afterStr)
		if err != nil {
			return nil, err
		}
		if afterTx == nil {
			return nil, i18n.NewError(ctx, tmmsgs.MsgPaginationErrTxNotFound, afterStr)
		}
	}
	switch {
	case signer != "" && pending:
		return nil, i18n.NewError(ctx, tmmsgs.MsgTXConflictSignerPending)
	case signer != "":
		var afterNonce *fftypes.FFBigInt
		if afterTx != nil {
			afterNonce = afterTx.Nonce
		}
		return m.persistence.ListTransactionsByNonce(ctx, signer, afterNonce, limit, dir)
	case pending:
		var afterSequence string
		if afterTx != nil {
			afterSequence = afterTx.SequenceID
		}
		return m.persistence.ListTransactionsPending(ctx, afterSequence, limit, dir)
	default:
		return m.persistence.ListTransactionsByCreateTime(ctx, afterTx, limit, dir)
	}

}

func (m *manager) updateTransaction(ctx context.Context, txID string, txUpdate *apitypes.TXUpdatesExternal) (transaction *apitypes.ManagedTX, err error) {
	if txUpdate == nil {
		return nil, i18n.NewError(ctx, tmmsgs.MsgInvalidSortDirection)
	}
	updatedTx, err := m.txHandler.HandleTransactionUpdate(ctx, txID, *txUpdate)

	if err != nil {
		return nil, err
	}

	return updatedTx, nil

}

func (m *manager) requestTransactionDeletion(ctx context.Context, txID string) (status int, transaction *apitypes.ManagedTX, err error) {

	canceledTx, err := m.txHandler.HandleCancelTransaction(ctx, txID)

	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusAccepted, canceledTx, nil

}

func (m *manager) requestTransactionSuspend(ctx context.Context, txID string) (status int, transaction *apitypes.ManagedTX, err error) {

	canceledTx, err := m.txHandler.HandleSuspendTransaction(ctx, txID)

	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusAccepted, canceledTx, nil

}

func (m *manager) requestTransactionResume(ctx context.Context, txID string) (status int, transaction *apitypes.ManagedTX, err error) {

	canceledTx, err := m.txHandler.HandleResumeTransaction(ctx, txID)

	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusAccepted, canceledTx, nil

}
