// Copyright © 2022 Kaleido, Inc.
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
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/internal/confirmations"
	"github.com/hyperledger-firefly/transaction-manager/mocks/confirmationsmocks"
	"github.com/hyperledger-firefly/transaction-manager/mocks/txhandlermocks"
	"github.com/hyperledger-firefly/transaction-manager/mocks/wsmocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/mock"
)

func newTestManagedTransactionEventHandler() *ManagedTransactionEventHandler {
	eh := &ManagedTransactionEventHandler{
		Ctx: context.Background(),
	}
	return eh
}

func TestHandleTransactionProcessSuccessEvent(t *testing.T) {
	testTx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
		Status:     apitypes.TxStatusSucceeded,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x0000",
			Nonce: fftypes.NewFFBigInt(1),
		},
		TransactionHash: "0x1111",
	}
	eh := newTestManagedTransactionEventHandler()
	mws := &wsmocks.WebSocketServer{}
	mws.On("SendReply", mock.MatchedBy(func(r *apitypes.TransactionUpdateReply) bool {
		return r.Headers.RequestID == testTx.ID &&
			r.Headers.Type == apitypes.TransactionUpdateSuccess &&
			r.Status == testTx.Status &&
			r.ProtocolID == ""
	})).Return(nil).Once()
	eh.WsServer = mws

	eh.HandleEvent(context.Background(), apitypes.ManagedTransactionEvent{
		Type: apitypes.ManagedTXProcessSucceeded,
		Tx:   testTx,
	})

	mws.AssertExpectations(t)
}

func TestHandleTransactionProcessFailEvent(t *testing.T) {
	testTx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
		Status:     apitypes.TxStatusFailed,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x0000",
			Nonce: fftypes.NewFFBigInt(1),
		},
		TransactionHash: "0x1111",
	}
	receipt := &ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			ProtocolID:       fmt.Sprintf("%.12d/%.6d", fftypes.NewFFBigInt(12345).Int64(), fftypes.NewFFBigInt(10).Int64()),
			ContractLocation: fftypes.JSONAnyPtr(`{"address": "0x24746b95d118b2b4e8d07b06b1bad988fbf9415d"}`),
		},
	}
	eh := newTestManagedTransactionEventHandler()
	mws := &wsmocks.WebSocketServer{}
	mws.On("SendReply", mock.MatchedBy(func(r *apitypes.TransactionUpdateReply) bool {
		return r.Headers.RequestID == testTx.ID &&
			r.Headers.Type == apitypes.TransactionUpdateFailure &&
			r.Status == testTx.Status &&
			r.TransactionHash == testTx.TransactionHash &&
			r.ContractLocation == receipt.ContractLocation &&
			r.ProtocolID == receipt.ProtocolID
	})).Return(nil).Once()
	eh.WsServer = mws

	eh.HandleEvent(context.Background(), apitypes.ManagedTransactionEvent{
		Type:    apitypes.ManagedTXProcessFailed,
		Tx:      testTx,
		Receipt: receipt,
	})

	mws.AssertExpectations(t)
}

func TestHandleTransactionHashUpdateEventAddHash(t *testing.T) {
	eh := newTestManagedTransactionEventHandler()
	mcm := &confirmationsmocks.Manager{}
	mcm.On("Notify", mock.MatchedBy(func(n *confirmations.Notification) bool {
		return n.NotificationType == confirmations.NewTransaction
	})).Return(nil).Once()
	eh.ConfirmationManager = mcm
	eh.TxHandler = &txhandlermocks.TransactionHandler{}
	testTx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
		Status:     apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x0000",
			Nonce: fftypes.NewFFBigInt(1),
		},
		TransactionHash: "0x1111",
	}

	eh.HandleEvent(context.Background(), apitypes.ManagedTransactionEvent{
		Type: apitypes.ManagedTXTransactionHashAdded,
		Tx:   testTx,
	})

	mcm.AssertExpectations(t)
}

func TestHandleTransactionHashUpdateEventRemoveHash(t *testing.T) {
	eh := newTestManagedTransactionEventHandler()
	mcm := &confirmationsmocks.Manager{}
	mcm.On("Notify", mock.MatchedBy(func(n *confirmations.Notification) bool {
		return n.NotificationType == confirmations.RemovedTransaction
	})).Return(nil).Once()
	eh.ConfirmationManager = mcm
	testTx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
		Status:     apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x0000",
			Nonce: fftypes.NewFFBigInt(1),
		},
		TransactionHash: "0x1111",
	}

	eh.HandleEvent(context.Background(), apitypes.ManagedTransactionEvent{
		Type: apitypes.ManagedTXTransactionHashRemoved,
		Tx:   testTx,
	})

	mcm.AssertExpectations(t)
}

func TestHandleTransactionHashUpdateEventSwallowErrors(t *testing.T) {
	testTx := &apitypes.ManagedTX{
		ID:         fmt.Sprintf("ns1:%s", fftypes.NewUUID()),
		Created:    fftypes.Now(),
		SequenceID: apitypes.NewULID().String(),
		Status:     apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From:  "0x0000",
			Nonce: fftypes.NewFFBigInt(1),
		},
		TransactionHash: "0x2222",
	}
	eh := newTestManagedTransactionEventHandler()
	mth := txhandlermocks.TransactionHandler{}
	mth.On("HandleTransactionReceiptReceived", mock.Anything, testTx.ID, mock.Anything).Return(fmt.Errorf("boo")).Once()
	mth.On("HandleTransactionConfirmations", mock.Anything, testTx.ID, mock.Anything).Return(fmt.Errorf("boo")).Once()
	eh.TxHandler = &mth
	mc := &confirmationsmocks.Manager{}
	mc.On("Notify", mock.MatchedBy(func(n *confirmations.Notification) bool {
		return n.NotificationType == confirmations.NewTransaction
	})).Run(func(args mock.Arguments) {
		n := args[0].(*confirmations.Notification)
		n.Transaction.Receipt(context.Background(), &ffcapi.TransactionReceiptResponse{})
		n.Transaction.Confirmations(context.Background(), &apitypes.ConfirmationsNotification{
			Confirmed: true,
		})
	}).Return(nil)
	eh.ConfirmationManager = mc

	eh.HandleEvent(context.Background(), apitypes.ManagedTransactionEvent{
		Type: apitypes.ManagedTXTransactionHashAdded,
		Tx:   testTx,
	})

	mc.AssertExpectations(t)
	mth.AssertExpectations(t)
}
