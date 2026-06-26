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

package fftm

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/confirmations"
	"github.com/hyperledger-firefly/transaction-manager/internal/ws"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

type ManagedTransactionEventHandler struct {
	Ctx                 context.Context
	ConfirmationManager confirmations.Manager
	WsServer            ws.WebSocketServer
	TxHandler           txhandler.TransactionHandler
}

func NewManagedTransactionEventHandler(ctx context.Context, cm confirmations.Manager, ws ws.WebSocketServer, th txhandler.TransactionHandler) txhandler.ManagedTxEventHandler {
	eh := &ManagedTransactionEventHandler{
		Ctx:                 ctx,
		ConfirmationManager: cm,
		WsServer:            ws,
		TxHandler:           th,
	}
	return eh
}

func (eh *ManagedTransactionEventHandler) HandleEvent(_ context.Context, e apitypes.ManagedTransactionEvent) error {
	switch e.Type {
	case apitypes.ManagedTXProcessSucceeded:
		eh.sendWSReply(e.Tx, e.Receipt)
	case apitypes.ManagedTXProcessFailed:
		eh.sendWSReply(e.Tx, e.Receipt)
	case apitypes.ManagedTXDeleted:
		eh.sendWSReply(e.Tx, nil /* receipt never sent with delete */)
	case apitypes.ManagedTXTransactionHashAdded:
		if e.ConfirmationHandler == nil {
			// e.ConfirmationHandler was added to support extra variable passing to callbacks.
			// Default to the previous interface on the TxHandler
			e.ConfirmationHandler = eh.TxHandler.HandleTransactionConfirmations
		}
		if e.ReceiptHandler == nil {
			// e.ReceiptHandler was added to support extra variable passing to callbacks.
			// Default to the previous interface on the TxHandler
			e.ReceiptHandler = eh.TxHandler.HandleTransactionReceiptReceived
		}
		txID := e.Tx.ID
		return eh.ConfirmationManager.Notify(&confirmations.Notification{
			NotificationType: confirmations.NewTransaction,
			Transaction: &confirmations.TransactionInfo{
				TransactionHash: e.Tx.TransactionHash,
				Receipt: func(ctx context.Context, receipt *ffcapi.TransactionReceiptResponse) {
					if err := e.ReceiptHandler(ctx, txID, receipt); err != nil {
						log.L(ctx).Errorf("Receipt for transaction %s at nonce %s / %d - hash: %s was not handled due to %s", e.Tx.ID, e.Tx.TransactionHeaders.From, e.Tx.Nonce.Int64(), e.Tx.TransactionHash, err.Error())
					}
				},
				Confirmations: func(ctx context.Context, notification *apitypes.ConfirmationsNotification) {
					if err := e.ConfirmationHandler(ctx, txID, notification); err != nil {
						log.L(ctx).Errorf("Confirmation for transaction %s at nonce %s / %d - hash: %s was not handled due to %s", e.Tx.ID, e.Tx.TransactionHeaders.From, e.Tx.Nonce.Int64(), e.Tx.TransactionHash, err.Error())
					}
				},
			},
		})
	case apitypes.ManagedTXTransactionHashRemoved:
		return eh.ConfirmationManager.Notify(&confirmations.Notification{
			NotificationType: confirmations.RemovedTransaction,
			Transaction: &confirmations.TransactionInfo{
				TransactionHash: e.Tx.TransactionHash,
			},
		})
	}
	return nil
}

func (eh *ManagedTransactionEventHandler) sendWSReply(mtx *apitypes.ManagedTX, receipt *ffcapi.TransactionReceiptResponse) {
	wsr := &apitypes.TransactionUpdateReply{
		Headers: apitypes.ReplyHeaders{
			RequestID: mtx.ID,
		},
		Status:          mtx.Status,
		TransactionHash: mtx.TransactionHash,
	}

	if receipt != nil && receipt.ContractLocation != nil {
		wsr.ContractLocation = receipt.ContractLocation
	}

	if receipt != nil {
		wsr.ProtocolID = receipt.ProtocolID
	} else {
		wsr.ProtocolID = ""
	}

	switch mtx.Status {
	case apitypes.TxStatusSucceeded:
		wsr.Headers.Type = apitypes.TransactionUpdateSuccess
	case apitypes.TxStatusFailed:
		wsr.Headers.Type = apitypes.TransactionUpdateFailure
	}
	// Notify on the websocket - this is best-effort (there is no subscription/acknowledgement)
	eh.WsServer.SendReply(wsr)
}
