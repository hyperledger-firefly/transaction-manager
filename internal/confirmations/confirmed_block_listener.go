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

package confirmations

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/retry"
	"github.com/hyperledger/firefly-transaction-manager/internal/tmconfig"
	"github.com/hyperledger/firefly-transaction-manager/internal/tmmsgs"
	"github.com/hyperledger/firefly-transaction-manager/pkg/apitypes"
	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
)

// confirmedBlockListener works differently to the main confirmation listener function,
// as an individual checkpoint-restart ordered stream of all blocks from the chain
// that have met a configured threshold of confirmations.
//
// Note that this builds upon the connector-specific block listener, which likely itself
// have some detailed handling of re-orgs at the front of the chian (the EVM one does).
//
// This implementation is thus deliberately simple assuming that when instability is found
// in the notifications it can simply wipe out its view and start again.
type confirmedBlockListener struct {
	bcm                   *blockConfirmationManager
	ctx                   context.Context
	cancelFunc            func()
	id                    *fftypes.UUID
	stateLock             sync.Mutex
	fromBlock             uint64
	waitingForFromBlock   bool
	rollingCheckpoint     *ffcapi.BlockListenerCheckpoint
	blocksSinceCheckpoint []*apitypes.BlockInfo
	newHeadToAdd          []*apitypes.BlockInfo // used by the notification routine when there are new blocks that add directly onto the end of the blocksSinceCheckpoint
	newBlockHashes        chan *ffcapi.BlockHashEvent
	dispatcherTap         chan struct{}
	eventStream           chan<- *ffcapi.ListenerEvent
	connector             ffcapi.API
	requiredConfirmations uint64
	retry                 *retry.Retry
	processorDone         chan struct{}
	dispatcherDone        chan struct{}
}

func (bcm *blockConfirmationManager) StartConfirmedBlockListener(ctx context.Context, id *fftypes.UUID, fromBlock string, checkpoint *ffcapi.BlockListenerCheckpoint, eventStream chan<- *ffcapi.ListenerEvent) error {
	_, err := bcm.startConfirmedBlockListener(ctx, id, fromBlock, checkpoint, eventStream)
	return err
}

func (bcm *blockConfirmationManager) startConfirmedBlockListener(fgCtx context.Context, id *fftypes.UUID, fromBlock string, checkpoint *ffcapi.BlockListenerCheckpoint, eventStream chan<- *ffcapi.ListenerEvent) (cbl *confirmedBlockListener, err error) {
	if bcm.chainTrackingMode == ffcapi.ChainTrackingModeLight {
		return nil, i18n.NewError(fgCtx, tmmsgs.MsgConfirmedBlockListenerUnsupportedMode, bcm.chainTrackingMode)
	}
	cbl = &confirmedBlockListener{
		bcm: bcm,
		// We need our own listener for each confirmed block stream, and the bcm has to fan out
		newBlockHashes:        make(chan *ffcapi.BlockHashEvent, config.GetInt(tmconfig.ConfirmationsBlockQueueLength)),
		dispatcherTap:         make(chan struct{}, 1),
		id:                    id,
		eventStream:           eventStream,
		requiredConfirmations: bcm.requiredConfirmations,
		connector:             bcm.connector,
		retry:                 bcm.retry,
		rollingCheckpoint:     checkpoint,
		processorDone:         make(chan struct{}),
		dispatcherDone:        make(chan struct{}),
	}
	cbl.ctx, cbl.cancelFunc = context.WithCancel(bcm.ctx)
	// add a log context for this specific confirmation manager (as there are many within the )
	cbl.ctx = log.WithLogField(cbl.ctx, "role", fmt.Sprintf("confirmed_block_stream_%s", id))

	switch fromBlock {
	case "", ffcapi.FromBlockLatest:
		if checkpoint != nil {
			cbl.fromBlock = checkpoint.Block
		} else {
			cbl.waitingForFromBlock = true
		}
	case ffcapi.FromBlockEarliest:
		fromBlock = "0"
		fallthrough
	default:
		if cbl.fromBlock, err = strconv.ParseUint(fromBlock, 10, 64); err != nil {
			return nil, i18n.NewError(fgCtx, tmmsgs.MsgFromBlockInvalid, fromBlock)
		}
	}

	bcm.cblLock.Lock()
	defer bcm.cblLock.Unlock()
	if _, alreadyStarted := bcm.cbls[*id]; alreadyStarted {
		return nil, i18n.NewError(fgCtx, tmmsgs.MsgBlockListenerAlreadyStarted, id)
	}
	bcm.cbls[*id] = cbl

	go cbl.notificationProcessor()
	go cbl.dispatcher()
	return cbl, nil
}

func (bcm *blockConfirmationManager) StopConfirmedBlockListener(fgCtx context.Context, id *fftypes.UUID) error {
	bcm.cblLock.Lock()
	defer bcm.cblLock.Unlock()

	cbl := bcm.cbls[*id]
	if cbl == nil {
		return i18n.NewError(fgCtx, tmmsgs.MsgBlockListenerNotStarted, id)
	}

	// Don't hold lock while waiting, but do re-lock before deleting from the map
	// (means multiple callers could enter this block in the middle, but that's re-entrant)
	bcm.cblLock.Unlock()
	cbl.cancelFunc()
	<-cbl.processorDone
	<-cbl.dispatcherDone
	bcm.cblLock.Lock()

	delete(bcm.cbls, *id)
	return nil
}

// The notificationProcessor processes all notification immediately from the head of the chain
// regardless of how far back in the chain the dispatcher is.
func (cbl *confirmedBlockListener) notificationProcessor() {
	defer close(cbl.processorDone)
	for {
		select {
		case bhe := <-cbl.newBlockHashes:
			cbl.processBlockHashes(bhe.BlockHashes)
		case <-cbl.ctx.Done():
			log.L(cbl.ctx).Debugf("Confirmed block listener stopping")
			return
		}
	}
}

func (cbl *confirmedBlockListener) processBlockHashes(blockHashes []string) {
	for _, blockHash := range blockHashes {
		block, err := cbl.bcm.getBlockByHash(blockHash)
		if err != nil || block == nil {
			// regardless of the failure, as long as we get notified of subsequent
			// blocks that work this will work itself out.
			log.L(cbl.ctx).Errorf("Failed to retrieve block %s: %v", blockHash, err)
			continue
		}
		cbl.processBlockNotification(block)
	}
}

// Whenever we get a new block we try and reconcile it into our current view of the
// canonical chain ahead of the last checkpoint.
// Then we update the state the dispatcher uses to walk forwards from and see what
// is confirmed and ready to dispatch
func (cbl *confirmedBlockListener) processBlockNotification(block *apitypes.BlockInfo) {

	cbl.stateLock.Lock()
	defer cbl.stateLock.Unlock()

	if cbl.waitingForFromBlock {
		// by definition we won't find anything in cbl.blocksSinceCheckpoint below
		cbl.fromBlock = block.BlockNumber.Uint64()
		cbl.waitingForFromBlock = false
	} else if block.BlockNumber.Uint64() < cbl.fromBlock {
		log.L(cbl.ctx).Debugf("Notification of block %d/%s < fromBlock %d", block.BlockNumber, block.BlockHash, cbl.fromBlock)
		return
	}

	// If the block is before our checkpoint, we ignore it completely
	if cbl.rollingCheckpoint != nil && block.BlockNumber.Uint64() <= cbl.rollingCheckpoint.Block {
		log.L(cbl.ctx).Debugf("Notification of block %d/%s <= checkpoint %d", block.BlockNumber, block.BlockHash, cbl.rollingCheckpoint.Block)
		return
	}

	// If the block immediate adds onto the set of blocks being processed, then we just attach it there
	// and notify the dispatcher to process it directly. No need for the other routine to query again.
	// When we're in steady state listening to the stable head of the chain, this should be the most common case.
	var dispatchHead *apitypes.BlockInfo
	if len(cbl.newHeadToAdd) > 0 {
		// we've snuck in multiple notifications while the dispatcher is busy... don't add indefinitely to this list
		if len(cbl.newHeadToAdd) >= 10 /* not considered worth adding/explaining a tuning property for this */ {
			log.L(cbl.ctx).Infof("Block listener fell behind head of chain")
			// Nothing more we can do in this function until it catches up - we just have to discard the notification
			return
		}
		dispatchHead = cbl.newHeadToAdd[len(cbl.newHeadToAdd)-1]
	}
	if dispatchHead == nil && len(cbl.blocksSinceCheckpoint) > 0 {
		dispatchHead = cbl.blocksSinceCheckpoint[len(cbl.blocksSinceCheckpoint)-1]
	}
	switch {
	case dispatchHead != nil && block.BlockNumber == dispatchHead.BlockNumber+1 && block.ParentHash == dispatchHead.BlockHash:
		// Ok - we just need to pop it onto the list, and ensure we wake the dispatcher routine
		log.L(cbl.ctx).Debugf("Directly passing block %d/%s to dispatcher after block %d/%s", block.BlockNumber, block.BlockHash, dispatchHead.BlockNumber, dispatchHead.BlockHash)
		cbl.newHeadToAdd = append(cbl.newHeadToAdd, block)
	case dispatchHead == nil && (cbl.rollingCheckpoint == nil || block.BlockNumber.Uint64() == (cbl.rollingCheckpoint.Block+1)):
		// This is the next block the dispatcher needs, to wake it up with this.
		log.L(cbl.ctx).Debugf("Directly passing block %d/%s to dispatcher as no blocks pending", block.BlockNumber, block.BlockHash)
		cbl.newHeadToAdd = append(cbl.newHeadToAdd, block)
	default:
		// Otherwise see if it's a conflicting fork to any of our existing blocks
		for idx, existingBlock := range cbl.blocksSinceCheckpoint {
			if existingBlock.BlockNumber == block.BlockNumber {
				// Must discard up to this point
				cbl.blocksSinceCheckpoint = cbl.blocksSinceCheckpoint[0:idx]
				cbl.newHeadToAdd = nil
				// This block fits, slot it into this point in the chain
				if idx == 0 || block.ParentHash == cbl.blocksSinceCheckpoint[idx-1].BlockHash {
					log.L(cbl.ctx).Debugf("Notification of re-org %d/%s replacing block %d/%s", block.BlockNumber, block.BlockHash, existingBlock.BlockNumber, existingBlock.BlockHash)
					cbl.blocksSinceCheckpoint = append(cbl.blocksSinceCheckpoint[0:idx], block)
				} else {
					log.L(cbl.ctx).Debugf("Notification of block %d/%s conflicting with previous block %d/%s", block.BlockNumber, block.BlockHash, existingBlock.BlockNumber, existingBlock.BlockHash)
				}
				break
			}
		}
	}

	// There's something for the dispatcher to process
	cbl.tapDispatcher()

}

func (cbl *confirmedBlockListener) tapDispatcher() {
	select {
	case cbl.dispatcherTap <- struct{}{}:
	default:
	}
}

func (cbl *confirmedBlockListener) dispatcher() {
	defer close(cbl.dispatcherDone)

	for {
		if !cbl.waitingForFromBlock {
			// spin getting blocks until we it looks like we need to wait for a notification
			lastFromNotification := false
			for cbl.readNextBlock(&lastFromNotification) {
				cbl.dispatchAllConfirmed()
			}
		}

		select {
		case <-cbl.dispatcherTap:
		case <-cbl.ctx.Done():
			log.L(cbl.ctx).Debugf("Confirmed block dispatcher stopping")
			return
		}

	}
}

// MUST be called under lock
func (cbl *confirmedBlockListener) popDispatchedIfAvailable(lastFromNotification *bool) (blockNumberToFetch uint64, found bool) {

	if len(cbl.newHeadToAdd) > 0 {
		// If we find one in the lock, it must be ready for us to append
		nextBlock := cbl.newHeadToAdd[0]
		cbl.newHeadToAdd = append([]*apitypes.BlockInfo{}, cbl.newHeadToAdd[1:]...)
		cbl.blocksSinceCheckpoint = append(cbl.blocksSinceCheckpoint, nextBlock)

		// We track that we've done this, so we know if we run out going round the loop later,
		// there's no point in doing a get-by-number
		*lastFromNotification = true
		return 0, true
	}

	blockNumberToFetch = cbl.fromBlock
	if cbl.rollingCheckpoint != nil && cbl.rollingCheckpoint.Block >= cbl.fromBlock {
		blockNumberToFetch = cbl.rollingCheckpoint.Block + 1
	}
	if len(cbl.blocksSinceCheckpoint) > 0 {
		blockNumberToFetch = cbl.blocksSinceCheckpoint[len(cbl.blocksSinceCheckpoint)-1].BlockNumber.Uint64() + 1
	}
	return blockNumberToFetch, false
}

func (cbl *confirmedBlockListener) readNextBlock(lastFromNotification *bool) (found bool) {

	var nextBlock *apitypes.BlockInfo
	var blockNumberToFetch uint64
	var dispatchedPopped bool
	err := cbl.retry.Do(cbl.ctx, "next block", func(_ int) (retry bool, err error) {
		// If the notifier has lined up a block for us grab it before
		cbl.stateLock.Lock()
		blockNumberToFetch, dispatchedPopped = cbl.popDispatchedIfAvailable(lastFromNotification)
		cbl.stateLock.Unlock()
		if dispatchedPopped || *lastFromNotification {
			// We processed a dispatch this time, or last time.
			// Either way we're tracking at the head and there's no point doing a query
			// we expect to return nothing - as we should get another notification.
			return false, nil
		}

		// Get the next block
		nextBlock, err = cbl.bcm.getBlockByNumber(blockNumberToFetch, false, "")
		return true, err
	})
	if nextBlock == nil || err != nil {
		// We either got a block dispatched, or did not find a block ourselves.
		return dispatchedPopped
	}

	// In the lock append it to our list, checking it's valid to append to what we have
	cbl.stateLock.Lock()
	defer cbl.stateLock.Unlock()

	// We have to check because we unlocked, that we weren't beaten to the punch while we queried
	// by the dispatcher.
	if _, dispatchedPopped = cbl.popDispatchedIfAvailable(lastFromNotification); !dispatchedPopped {

		// It's possible that while we were off at the node querying this, a notification came in
		// that affected our state. We need to check this still matches, or go round again
		if len(cbl.blocksSinceCheckpoint) > 0 {
			if cbl.blocksSinceCheckpoint[len(cbl.blocksSinceCheckpoint)-1].BlockHash != nextBlock.ParentHash {
				// This doesn't attach to the end of our list. Trim it off and try again.
				cbl.blocksSinceCheckpoint = cbl.blocksSinceCheckpoint[0 : len(cbl.blocksSinceCheckpoint)-1]
				return true
			}
		}

		// We successfully attached it
		cbl.blocksSinceCheckpoint = append(cbl.blocksSinceCheckpoint, nextBlock)
	}
	return true

}

func (cbl *confirmedBlockListener) dispatchAllConfirmed() {
	for {
		var toDispatch *ffcapi.ListenerEvent
		cbl.stateLock.Lock()
		if uint64(len(cbl.blocksSinceCheckpoint)) > cbl.requiredConfirmations {
			block := cbl.blocksSinceCheckpoint[0]
			// don't want memory to grow indefinitely by shifting right, so we create a new slice here
			cbl.blocksSinceCheckpoint = append([]*apitypes.BlockInfo{}, cbl.blocksSinceCheckpoint[1:]...)
			cbl.rollingCheckpoint = &ffcapi.BlockListenerCheckpoint{
				Block: block.BlockNumber.Uint64(),
			}
			toDispatch = &ffcapi.ListenerEvent{
				BlockEvent: &ffcapi.BlockEvent{
					ListenerID: cbl.id,
					BlockInfo: ffcapi.BlockInfo{
						//nolint:gosec
						BlockNumber:       fftypes.NewFFBigInt(int64(block.BlockNumber)),
						BlockHash:         block.BlockHash,
						ParentHash:        block.ParentHash,
						TransactionHashes: block.TransactionHashes,
					},
				},
				Checkpoint: cbl.rollingCheckpoint,
			}
		}
		cbl.stateLock.Unlock()
		if toDispatch == nil {
			return
		}
		log.L(cbl.ctx).Infof("Dispatching block %d/%s", toDispatch.BlockEvent.BlockNumber.Uint64(), toDispatch.BlockEvent.BlockHash)
		select {
		case cbl.eventStream <- toDispatch:
		case <-cbl.ctx.Done():
		}
	}
}
