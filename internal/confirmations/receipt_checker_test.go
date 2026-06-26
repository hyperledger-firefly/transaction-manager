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

package confirmations

import (
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCheckReceiptNotFoundErr(t *testing.T) {

	bcm, mca := newTestBlockConfirmationManager()

	mca.On("TransactionReceipt", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			bcm.receiptChecker.closed = true // to exit
		}).
		Return(nil, ffcapi.ErrorReasonNotFound, fmt.Errorf("not found"))

	txHash := "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
	pending := &pendingItem{
		pType:           pendingTypeTransaction,
		transactionHash: txHash,
	}
	bcm.receiptChecker.schedule(pending, false)

	// We can run a worker loop and it will exit immediately, after the transaction receipt closes the receipt checker
	bcm.receiptChecker.workersDone = []chan struct{}{make(chan struct{})}
	bcm.receiptChecker.run(0)

	// We should have removed it
	assert.Zero(t, bcm.receiptChecker.entries.Len())

}

func TestCheckReceiptNotFoundNil(t *testing.T) {

	bcm, mca := newTestBlockConfirmationManager()

	mca.On("TransactionReceipt", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			bcm.receiptChecker.closed = true // to exit
		}).
		Return(nil, ffcapi.ErrorReasonNotFound, nil)

	txHash := "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
	pending := &pendingItem{
		pType:           pendingTypeTransaction,
		transactionHash: txHash,
	}
	bcm.receiptChecker.schedule(pending, false)

	// We can run a worker loop and it will exit immediately, after the transaction receipt closes the receipt checker
	bcm.receiptChecker.workersDone = []chan struct{}{make(chan struct{})}
	bcm.receiptChecker.run(0)

	// We should have removed it
	assert.Zero(t, bcm.receiptChecker.entries.Len())

}

func TestCheckReceiptFail(t *testing.T) {

	bcm, mca := newTestBlockConfirmationManager()

	count := 0
	mca.On("TransactionReceipt", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			count++
			if count == 2 {
				bcm.receiptChecker.closed = true // to exit
			}
		}).
		Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	txHash := "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
	pending := &pendingItem{
		pType:           pendingTypeTransaction,
		transactionHash: txHash,
	}
	bcm.receiptChecker.schedule(pending, false)

	// Run the worker loop, and it should go round twice - driving the retry logic.
	bcm.receiptChecker.workersDone = []chan struct{}{make(chan struct{})}
	bcm.receiptChecker.run(0)

	// We should have re-queued it
	assert.Equal(t, 1, bcm.receiptChecker.entries.Len())
	assert.Equal(t, pending, bcm.receiptChecker.entries.Front().Value)

}

func TestCheckReceiptDoubleQueueProtection(t *testing.T) {

	bcm, _ := newTestBlockConfirmationManager()

	txHash := "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
	pending := &pendingItem{
		pType:           pendingTypeTransaction,
		transactionHash: txHash,
	}
	bcm.receiptChecker.schedule(pending, false)
	bcm.receiptChecker.schedule(pending, false)
	assert.Equal(t, 1, bcm.receiptChecker.entries.Len())
	assert.Equal(t, pending, bcm.receiptChecker.entries.Front().Value)

	// Remove from the list
	bcm.receiptChecker.remove(pending)

	// Even if we attempt to re-add it now, as long as pending.queuedStale still not nil it will
	// not schedule. Important as we have the item out of the list between when it's dispatched
	// to a worker, and when it's successfully executed (or put back on the end of the list)
	bcm.receiptChecker.schedule(pending, false)
	assert.Zero(t, bcm.receiptChecker.entries.Len())
}
