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
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/mocks/persistencemocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDBMigrationOK(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	es := &apitypes.EventStream{ID: fftypes.NewUUID()}
	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{es}, nil)
	mdb1.On("ListStreamsByCreateTime", mock.Anything, es.ID, paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{}, nil)
	mdb2.On("GetStream", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteStream", mock.Anything, es).Return(nil)

	cp := &apitypes.EventStreamCheckpoint{StreamID: es.ID}
	mdb1.On("GetCheckpoint", mock.Anything, es.ID).Return(cp, nil)
	mdb2.On("GetCheckpoint", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteCheckpoint", mock.Anything, cp).Return(nil)

	l := &apitypes.Listener{ID: fftypes.NewUUID()}
	mdb1.On("ListListenersByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.Listener{l}, nil)
	mdb1.On("ListListenersByCreateTime", mock.Anything, l.ID, paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.Listener{}, nil)
	mdb2.On("GetListener", mock.Anything, l.ID).Return(nil, nil)
	mdb2.On("WriteListener", mock.Anything, l).Return(nil)

	tx := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{ID: fftypes.NewUUID().String()},
		Receipt: &ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{BlockHash: fftypes.NewRandB32().String()},
		},
		Confirmations: []*apitypes.Confirmation{{BlockHash: fftypes.NewRandB32().String()}},
	}
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{tx.ManagedTX}, nil)
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, tx.ManagedTX, paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{}, nil)
	mdb1.On("GetTransactionByIDWithStatus", mock.Anything, tx.ID, false).Return(tx, nil)
	mdb2.On("GetTransactionByID", mock.Anything, tx.ID).Return(nil, nil)
	mdb2.On("InsertTransactionPreAssignedNonce", mock.Anything, tx.ManagedTX).Return(nil)
	mdb2.On("SetTransactionReceipt", mock.Anything, tx.ID, tx.Receipt).Return(nil)
	mdb2.On("AddTransactionConfirmations", mock.Anything, tx.ID, true, tx.Confirmations[0]).Return(nil)

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.run(context.Background())
	assert.NoError(t, err)

}

func TestDBMigrationRunFailTransactions(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{}, nil)

	mdb1.On("ListListenersByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.Listener{}, nil)

	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{}, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.run(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestDBMigrationRunFailListeners(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{}, nil)

	mdb1.On("ListListenersByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.Listener{}, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.run(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestDBMigrationRunFailStreams(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{}, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.run(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateEventsStreamsFailWrite(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	es := &apitypes.EventStream{ID: fftypes.NewUUID()}
	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{es}, nil)
	mdb2.On("GetStream", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteStream", mock.Anything, es).Return(fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateEventStreams(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateEventsStreamsFailCheckExist(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	es := &apitypes.EventStream{ID: fftypes.NewUUID()}
	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{es}, nil)
	mdb2.On("GetStream", mock.Anything, es.ID).Return(nil, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateEventStreams(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateEventsStreamsFailWriteCheckpoint(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	es := &apitypes.EventStream{ID: fftypes.NewUUID()}
	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{es}, nil)
	mdb2.On("GetStream", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteStream", mock.Anything, es).Return(nil)

	cp := &apitypes.EventStreamCheckpoint{StreamID: es.ID}
	mdb1.On("GetCheckpoint", mock.Anything, es.ID).Return(cp, nil)
	mdb2.On("GetCheckpoint", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteCheckpoint", mock.Anything, cp).Return(fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateEventStreams(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateEventsStreamsFailCheckCheckpointExists(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	es := &apitypes.EventStream{ID: fftypes.NewUUID()}
	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{es}, nil)
	mdb2.On("GetStream", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteStream", mock.Anything, es).Return(nil)

	cp := &apitypes.EventStreamCheckpoint{StreamID: es.ID}
	mdb1.On("GetCheckpoint", mock.Anything, es.ID).Return(cp, nil)
	mdb2.On("GetCheckpoint", mock.Anything, es.ID).Return(nil, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateEventStreams(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateEventsStreamsFailGetCheckpoint(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	es := &apitypes.EventStream{ID: fftypes.NewUUID()}
	mdb1.On("ListStreamsByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.EventStream{es}, nil)
	mdb2.On("GetStream", mock.Anything, es.ID).Return(nil, nil)
	mdb2.On("WriteStream", mock.Anything, es).Return(nil)

	mdb1.On("GetCheckpoint", mock.Anything, es.ID).Return(nil, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateEventStreams(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateListenersFailWrite(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	l := &apitypes.Listener{ID: fftypes.NewUUID()}
	mdb1.On("ListListenersByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.Listener{l}, nil)
	mdb2.On("GetListener", mock.Anything, l.ID).Return(nil, nil)
	mdb2.On("WriteListener", mock.Anything, l).Return(fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateListeners(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateListenersFailCheckExists(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	l := &apitypes.Listener{ID: fftypes.NewUUID()}
	mdb1.On("ListListenersByCreateTime", mock.Anything, (*fftypes.UUID)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.Listener{l}, nil)
	mdb2.On("GetListener", mock.Anything, l.ID).Return(nil, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateListeners(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateTransactionsFailWriteConfirmations(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	tx := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{ID: fftypes.NewUUID().String()},
		Receipt: &ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{BlockHash: fftypes.NewRandB32().String()},
		},
		Confirmations: []*apitypes.Confirmation{{BlockHash: fftypes.NewRandB32().String()}},
	}
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{tx.ManagedTX}, nil)
	mdb1.On("GetTransactionByIDWithStatus", mock.Anything, tx.ID, false).Return(tx, nil)
	mdb2.On("GetTransactionByID", mock.Anything, tx.ID).Return(nil, nil)
	mdb2.On("InsertTransactionPreAssignedNonce", mock.Anything, tx.ManagedTX).Return(nil)
	mdb2.On("SetTransactionReceipt", mock.Anything, tx.ID, tx.Receipt).Return(nil)
	mdb2.On("AddTransactionConfirmations", mock.Anything, tx.ID, true, tx.Confirmations[0]).Return(fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateTransactions(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateTransactionsFailWriteReceipt(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	tx := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{ID: fftypes.NewUUID().String()},
		Receipt: &ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{BlockHash: fftypes.NewRandB32().String()},
		},
		Confirmations: []*apitypes.Confirmation{{BlockHash: fftypes.NewRandB32().String()}},
	}
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{tx.ManagedTX}, nil)
	mdb1.On("GetTransactionByIDWithStatus", mock.Anything, tx.ID, false).Return(tx, nil)
	mdb2.On("GetTransactionByID", mock.Anything, tx.ID).Return(nil, nil)
	mdb2.On("InsertTransactionPreAssignedNonce", mock.Anything, tx.ManagedTX).Return(nil)
	mdb2.On("SetTransactionReceipt", mock.Anything, tx.ID, tx.Receipt).Return(fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateTransactions(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateTransactionsFailWriteTx(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	tx := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{ID: fftypes.NewUUID().String()},
		Receipt: &ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{BlockHash: fftypes.NewRandB32().String()},
		},
		Confirmations: []*apitypes.Confirmation{{BlockHash: fftypes.NewRandB32().String()}},
	}
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{tx.ManagedTX}, nil)
	mdb1.On("GetTransactionByIDWithStatus", mock.Anything, tx.ID, false).Return(tx, nil)
	mdb2.On("GetTransactionByID", mock.Anything, tx.ID).Return(nil, nil)
	mdb2.On("InsertTransactionPreAssignedNonce", mock.Anything, tx.ManagedTX).Return(fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateTransactions(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateTransactionsFailCheckExists(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	tx := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{ID: fftypes.NewUUID().String()},
		Receipt: &ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{BlockHash: fftypes.NewRandB32().String()},
		},
		Confirmations: []*apitypes.Confirmation{{BlockHash: fftypes.NewRandB32().String()}},
	}
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{tx.ManagedTX}, nil)
	mdb1.On("GetTransactionByIDWithStatus", mock.Anything, tx.ID, false).Return(tx, nil)
	mdb2.On("GetTransactionByID", mock.Anything, tx.ID).Return(nil, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateTransactions(context.Background())
	assert.Regexp(t, "pop", err)

}

func TestMigrateTransactionsFailGetDetail(t *testing.T) {

	mdb1 := persistencemocks.NewPersistence(t)
	mdb2 := persistencemocks.NewPersistence(t)

	tx := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{ID: fftypes.NewUUID().String()},
		Receipt: &ffcapi.TransactionReceiptResponse{
			TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{BlockHash: fftypes.NewRandB32().String()},
		},
		Confirmations: []*apitypes.Confirmation{{BlockHash: fftypes.NewRandB32().String()}},
	}
	mdb1.On("ListTransactionsByCreateTime", mock.Anything, (*apitypes.ManagedTX)(nil), paginationLimit, txhandler.SortDirectionAscending).Return([]*apitypes.ManagedTX{tx.ManagedTX}, nil)
	mdb1.On("GetTransactionByIDWithStatus", mock.Anything, tx.ID, false).Return(tx, fmt.Errorf("pop"))

	m := dbMigration{
		source: mdb1,
		target: mdb2,
	}

	err := m.migrateTransactions(context.Background())
	assert.Regexp(t, "pop", err)

}
