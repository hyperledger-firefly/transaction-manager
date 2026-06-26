// Copyright © 2024 Kaleido, Inc.
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
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/metrics"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// receiptChecker asynchronously checks for receipts. It does not have a limited
// length queue as that would provide the potential for blocking the calling critical
// path routine. Instead it has a linked list.
//
// When receipt checkers hit errors (excluding a null result of course), they simply
// block in indefinite retry until they succeed or are shut down.
type receiptChecker struct {
	bcm            *blockConfirmationManager
	workerCount    int
	workersDone    []chan struct{}
	closed         bool
	cond           *sync.Cond
	entries        *list.List
	metricsEmitter metrics.ReceiptCheckerMetricsEmitter
	notify         func(*pendingItem, *ffcapi.TransactionReceiptResponse)
}

func newReceiptChecker(bcm *blockConfirmationManager, workerCount int, rcme metrics.ReceiptCheckerMetricsEmitter) *receiptChecker {
	rc := &receiptChecker{
		bcm:            bcm,
		workerCount:    workerCount,
		workersDone:    make([]chan struct{}, workerCount),
		metricsEmitter: rcme,
		notify: func(pending *pendingItem, receipt *ffcapi.TransactionReceiptResponse) {
			_ = bcm.Notify(&Notification{
				NotificationType: receiptArrived,
				pending:          pending,
				receipt:          receipt,
			})
		},
	}
	rc.entries = list.New()
	rc.cond = sync.NewCond(&sync.Mutex{})
	for i := 0; i < workerCount; i++ {
		rc.workersDone[i] = make(chan struct{})
		go rc.run(i)
	}
	return rc
}

func (rc *receiptChecker) waitNext() (p *pendingItem) {
	rc.cond.L.Lock()
	defer rc.cond.L.Unlock()
	var entry *list.Element
	for entry == nil {
		if rc.closed {
			return nil
		}
		entry = rc.entries.Front()
		if entry == nil {
			rc.cond.Wait()
		}
	}
	p = entry.Value.(*pendingItem)
	_ = rc.entries.Remove(entry) // remove from the list, but don't unset entry.queuedStale yet
	return p
}

func (rc *receiptChecker) run(i int) {
	defer close(rc.workersDone[i])
	ctx := log.WithLogFields(rc.bcm.ctx, "job", fmt.Sprintf("receiptchecker_%.3d", i))
	for {
		// We use the back-off retry handling of the retry loop to avoid tight loops,
		// but in the case of errors we re-queue the individual item to the back of the
		// queue so individual queued items do not get stuck for unrecoverable errors.
		err := rc.bcm.retry.Do(ctx, "receipt check", func(_ int) (bool, error) {
			startTime := time.Now()
			pending := rc.waitNext()
			if pending == nil {
				return false /* exit the retry loop with err */, i18n.NewError(ctx, tmmsgs.MsgShuttingDown)
			}

			res, reason, receiptErr := rc.bcm.connector.TransactionReceipt(ctx, &ffcapi.TransactionReceiptRequest{
				TransactionHash: pending.transactionHash,
			})
			if receiptErr != nil || res == nil {
				if receiptErr != nil && reason != ffcapi.ErrorReasonNotFound {
					log.L(ctx).Debugf("Failed to query receipt for transaction %s: %s", pending.transactionHash, receiptErr)
					// It's possible though that the node will return a non-recoverable error for this item.
					// So we push it to the back of the queue (we already removed it in waitNext, but left
					// queuedStale set on there to prevent it being re-queued externally).
					rc.cond.L.Lock()
					pending.queuedStale = rc.entries.PushBack(pending)
					rc.cond.L.Unlock()
					rc.metricsEmitter.RecordReceiptCheckMetrics(ctx, "retry", time.Since(startTime).Seconds())
					return true /* drive the retry delay mechanism before next de-queue */, receiptErr
				}
				log.L(ctx).Debugf("Receipt for transaction %s not yet available: %v", pending.transactionHash, receiptErr)
			}
			// Regardless of whether we got a receipt, update the pending item
			rc.cond.L.Lock()
			pending.queuedStale = nil // only unmark the entry now (even though we popped it in waitNext)
			pending.lastReceiptCheck = time.Now()
			rc.cond.L.Unlock()

			// Dispatch the receipt back to the main routine.
			if res != nil {
				rc.metricsEmitter.RecordReceiptCheckMetrics(ctx, "notified", time.Since(startTime).Seconds())
				rc.notify(pending, res)
			} else {
				rc.metricsEmitter.RecordReceiptCheckMetrics(ctx, "empty", time.Since(startTime).Seconds())
			}
			return false, nil
		})
		// Error means the context has closed
		if err != nil {
			log.L(ctx).Debugf("Receipt checker closing")
			return
		}
	}
}

func (rc *receiptChecker) schedule(pending *pendingItem, suspectedTimeout bool) {
	rc.cond.L.Lock()
	// Do a locked check again on the time, and check not already queued
	if pending.queuedStale != nil || (suspectedTimeout && time.Since(pending.lastReceiptCheck) < rc.bcm.staleReceiptTimeout) {
		rc.cond.L.Unlock()
		return
	}
	pending.queuedStale = rc.entries.PushBack(pending)
	pending.scheduledAtLeastOnce = true
	rc.cond.Signal()
	rc.cond.L.Unlock()
	// Log (outside the lock as it's a contended one)
	pendingKey := pending.getKey()
	log.L(rc.bcm.ctx).Infof("Marking receipt check stale for %s", pendingKey)
}

func (rc *receiptChecker) remove(pending *pendingItem) {
	rc.cond.L.Lock()
	if pending.queuedStale != nil {
		// Note this might already have been removed, in the window between waitNext and TransactionReceipt returning.
		// That's fine as the interface of list.Remove says so.
		_ = rc.entries.Remove(pending.queuedStale)
	}
	rc.cond.L.Unlock()
}

func (rc *receiptChecker) close() {
	rc.cond.L.Lock()
	rc.closed = true
	rc.cond.Broadcast()
	rc.cond.L.Unlock()
	for _, workerDone := range rc.workersDone {
		<-workerDone
	}
}
