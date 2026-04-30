// Copyright 2019 Kaleido

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package confirmations

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-transaction-manager/pkg/apitypes"
	"github.com/hyperledger/firefly-transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCBLCatchUpToHeadFromZeroNoConfirmations(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()

	blocks := testBlockArray(10)

	mbiNum := mca.On("BlockInfoByNumber", mock.Anything, mock.Anything)
	mbiNum.Run(func(args mock.Arguments) { mockBlockNumberReturn(mbiNum, args, blocks) })

	bcm.requiredConfirmations = 0
	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, ffcapi.FromBlockEarliest, nil, esDispatch)
	assert.NoError(t, err)

	for i := 0; i < len(blocks)-int(bcm.requiredConfirmations); i++ {
		b := <-esDispatch
		assert.Equal(t, b.BlockEvent.BlockInfo, blocks[i].BlockInfo)
	}

	time.Sleep(1 * time.Millisecond)
	assert.Empty(t, cbl.blocksSinceCheckpoint)

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLCatchUpToHeadFromZeroWithConfirmations(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()

	blocks := testBlockArray(15)

	mbiNum := mca.On("BlockInfoByNumber", mock.Anything, mock.Anything)
	mbiNum.Run(func(args mock.Arguments) { mockBlockNumberReturn(mbiNum, args, blocks) })

	bcm.requiredConfirmations = 5
	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, ffcapi.FromBlockEarliest, nil, esDispatch)
	assert.NoError(t, err)

	for i := 0; i < len(blocks)-int(bcm.requiredConfirmations); i++ {
		b := <-esDispatch
		assert.Equal(t, b.BlockEvent.BlockInfo, blocks[i].BlockInfo)
	}

	time.Sleep(1 * time.Millisecond)
	assert.Len(t, cbl.blocksSinceCheckpoint, int(bcm.requiredConfirmations))
	select {
	case <-esDispatch:
		assert.Fail(t, "should not have received block in confirmation window")
	default: // good - we should have the confirmations sat there, but no dispatch
	}

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLListenFromCurrentBlock(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()

	blocks := testBlockArray(15)

	mbiHash := mca.On("BlockInfoByHash", mock.Anything, mock.Anything)
	mbiHash.Run(func(args mock.Arguments) { mockBlockHashReturn(mbiHash, args, blocks) })

	mca.On("BlockInfoByNumber", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReasonNotFound, fmt.Errorf("not found")).Maybe()

	bcm.requiredConfirmations = 5
	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, "", nil, esDispatch)
	assert.NoError(t, err)

	// Notify starting at block 5
	for i := 5; i < len(blocks); i++ {
		bcm.propagateBlockHashToCBLs(&ffcapi.BlockHashEvent{
			BlockHashes: []string{blocks[i].BlockHash},
		})
	}

	// Randomly notify below that too, which will be ignored
	bcm.propagateBlockHashToCBLs(&ffcapi.BlockHashEvent{
		BlockHashes: []string{blocks[1].BlockHash},
	})

	for i := 5; i < len(blocks)-int(bcm.requiredConfirmations); i++ {
		b := <-esDispatch
		assert.Equal(t, b.BlockEvent.BlockNumber, blocks[i].BlockNumber)
		assert.Equal(t, b.BlockEvent.BlockInfo, blocks[i].BlockInfo)
	}

	time.Sleep(1 * time.Millisecond)
	assert.Len(t, cbl.blocksSinceCheckpoint, int(bcm.requiredConfirmations))
	select {
	case <-esDispatch:
		assert.Fail(t, "should not have received block in confirmation window")
	default: // good - we should have the confirmations sat there, but no dispatch
	}

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLListenFromCurrentUsingCheckpointBlock(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()

	blocks := testBlockArray(0)

	mbiNum := mca.On("BlockInfoByNumber", mock.Anything, mock.Anything)
	mbiNum.Run(func(args mock.Arguments) { mockBlockNumberReturn(mbiNum, args, blocks) })

	bcm.requiredConfirmations = 5
	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, "", &ffcapi.BlockListenerCheckpoint{
		Block: 12345,
	}, esDispatch)
	assert.NoError(t, err)

	assert.False(t, cbl.waitingForFromBlock)
	assert.Equal(t, uint64(12345), cbl.fromBlock)

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLHandleReorgInConfirmationWindow1(t *testing.T) {
	// test where the reorg happens at the edge of the confirmation window
	testCBLHandleReorgInConfirmationWindow(t,
		10, // blocks in chain before re-org
		5,  // blocks that remain from original chain after re-org
		5,  // required confirmations
	)
}

func TestCBLHandleReorgInConfirmationWindow2(t *testing.T) {
	// test where the reorg happens replacing some blocks
	// WE ALREADY CONFIRMED - meaning we dispatched them incorrectly
	// because the confirmations were not tuned correctly
	testCBLHandleReorgInConfirmationWindow(t,
		10, // blocks in chain before re-org
		0,  // blocks that remain from original chain after re-org
		5,  // required confirmations
	)
}

func TestCBLHandleReorgInConfirmationWindow3(t *testing.T) {
	// test without confirmations, so everything is a problem
	testCBLHandleReorgInConfirmationWindow(t,
		10, // blocks in chain before re-org
		0,  // blocks that remain from original chain after re-org
		0,  // required confirmations
	)
}

func TestCBLHandleReorgInConfirmationWindow4(t *testing.T) {
	// test of a re-org of one
	testCBLHandleReorgInConfirmationWindow(t,
		5, // blocks in chain before re-org
		4, // blocks that remain from original chain after re-org
		4, // required confirmations
	)
}

func testCBLHandleReorgInConfirmationWindow(t *testing.T, blockLenBeforeReorg, overlap, reqConf int) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()
	blocksBeforeReorg := testBlockArray(blockLenBeforeReorg)
	blocksAfterReorg := testBlockArray(blockLenBeforeReorg + overlap)
	if overlap > 0 {
		copy(blocksAfterReorg, blocksBeforeReorg[0:overlap])
		blocksAfterReorg[overlap] = &ffcapi.BlockInfoByNumberResponse{
			BlockInfo: blocksAfterReorg[overlap].BlockInfo,
		}
		blocksAfterReorg[overlap].ParentHash = blocksAfterReorg[overlap-1].BlockHash
	}
	checkBlocksSequential(t, "before", blocksBeforeReorg)
	checkBlocksSequential(t, "after ", blocksAfterReorg)

	var cbl *confirmedBlockListener
	var isAfterReorg atomic.Bool
	notificationsDone := make(chan struct{})
	mbiNum := mca.On("BlockInfoByNumber", mock.Anything, mock.Anything)
	mbiNum.Run(func(args mock.Arguments) {
		if isAfterReorg.Load() {
			mockBlockNumberReturn(mbiNum, args, blocksAfterReorg)
		} else {
			mockBlockNumberReturn(mbiNum, args, blocksBeforeReorg)
			// we instigate the re-org when we've returned all the blocks
			if int(args[1].(*ffcapi.BlockInfoByNumberRequest).BlockNumber.Int64()) >= len(blocksBeforeReorg) {
				isAfterReorg.Store(true)
				go func() {
					defer close(notificationsDone)
					// Simulate the modified blocks only coming in with delays
					for i := overlap; i < len(blocksAfterReorg); i++ {
						time.Sleep(100 * time.Microsecond)
						bcm.propagateBlockHashToCBLs(&ffcapi.BlockHashEvent{
							BlockHashes: []string{blocksAfterReorg[i].BlockHash},
						})
					}
				}()
			}
		}
	})
	// We query by hash only for the notifications, which are only on the after-reorg blocks
	mbiHash := mca.On("BlockInfoByHash", mock.Anything, mock.Anything)
	mbiHash.Run(func(args mock.Arguments) { mockBlockHashReturn(mbiHash, args, blocksAfterReorg) })

	bcm.requiredConfirmations = uint64(reqConf)
	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, ffcapi.FromBlockEarliest, nil, esDispatch)
	assert.NoError(t, err)

	for i := 0; i < len(blocksAfterReorg)-int(bcm.requiredConfirmations); i++ {
		b := <-esDispatch
		dangerArea := len(blocksAfterReorg) - overlap
		if i >= overlap && i < (dangerArea-reqConf) {
			// This would be a bad situation in reality, where a reorg crossed the confirmations
			// boundary. An indication someone incorrectly configured their confirmations
			assert.Equal(t, b.BlockEvent.BlockInfo, blocksBeforeReorg[i].BlockInfo)
		} else {
			assert.Equal(t, b.BlockEvent.BlockInfo, blocksAfterReorg[i].BlockInfo)
		}
	}

	time.Sleep(1 * time.Millisecond)
	assert.LessOrEqual(t, len(cbl.blocksSinceCheckpoint), int(bcm.requiredConfirmations))
	select {
	case b := <-esDispatch:
		assert.Fail(t, fmt.Sprintf("should not have received block in confirmation window: %d/%s", b.BlockEvent.BlockNumber.Int64(), b.BlockEvent.BlockHash))
	default: // good - we should have the confirmations sat there, but no dispatch
	}

	// Wait for the notifications to go through
	<-notificationsDone

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLHandleRandomConflictingBlockNotification(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()
	blocks := testBlockArray(50)

	randBlock := &ffcapi.BlockInfoByHashResponse{
		BlockInfo: ffcapi.BlockInfo{
			BlockNumber: fftypes.NewFFBigInt(3),
			BlockHash:   fftypes.NewRandB32().String(),
			ParentHash:  fftypes.NewRandB32().String(),
		},
	}

	var cbl *confirmedBlockListener
	sentRandom := false
	mbiNum := mca.On("BlockInfoByNumber", mock.Anything, mock.Anything)
	mbiNum.Run(func(args mock.Arguments) {
		mockBlockNumberReturn(mbiNum, args, blocks)
		if !sentRandom && args[1].(*ffcapi.BlockInfoByNumberRequest).BlockNumber.Int64() == 4 {
			sentRandom = true
			bcm.propagateBlockHashToCBLs(&ffcapi.BlockHashEvent{
				BlockHashes: []string{randBlock.BlockHash},
			})
			// Give notification handler likelihood to run before we continue the by-number getting
			time.Sleep(1 * time.Millisecond)
		}
	})
	// We query by hash only for the notifications, which are only on the after-reorg blocks
	mca.On("BlockInfoByHash", mock.Anything, mock.MatchedBy(func(req *ffcapi.BlockInfoByHashRequest) bool {
		return req.BlockHash == randBlock.BlockHash
	})).Return(randBlock, ffcapi.ErrorReason(""), nil)

	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, ffcapi.FromBlockEarliest, nil, esDispatch)
	assert.NoError(t, err)
	cbl.requiredConfirmations = 5

	for i := 0; i < len(blocks)-int(cbl.requiredConfirmations); i++ {
		b := <-esDispatch
		assert.Equal(t, b.BlockEvent.BlockInfo, blocks[i].BlockInfo)
	}

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLDispatcherFallsBehindHead(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()

	blocks := testBlockArray(30)

	mbiHash := mca.On("BlockInfoByHash", mock.Anything, mock.Anything)
	mbiHash.Run(func(args mock.Arguments) { mockBlockHashReturn(mbiHash, args, blocks) })

	bcm.requiredConfirmations = 5

	// Start a CBL, but then cancel the dispatcher
	cbl, err := bcm.startConfirmedBlockListener(bcm.ctx, id, "", nil, esDispatch)
	assert.NoError(t, err)
	cbl.cancelFunc()
	<-cbl.dispatcherDone
	<-cbl.processorDone

	// Restart just the processor
	cbl.ctx, cbl.cancelFunc = context.WithCancel(bcm.ctx)
	cbl.processorDone = make(chan struct{})
	go cbl.notificationProcessor()

	// Notify all the blocks
	for i := 0; i < len(blocks); i++ {
		bcm.propagateBlockHashToCBLs(&ffcapi.BlockHashEvent{
			BlockHashes: []string{blocks[i].BlockHash},
		})
	}

	// Wait until there's a first tap to the dispatcher, meaning
	// dispatches should have been added to the newHeadToAdd list
	<-cbl.dispatcherTap

	// ... until it got too far ahead and then set to nil.
	checkHeadLen := func() int {
		cbl.stateLock.Lock()
		defer cbl.stateLock.Unlock()
		return len(cbl.newHeadToAdd)
	}
	for checkHeadLen() < 10 {
		time.Sleep(1 * time.Millisecond)
	}

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestCBLStartBadFromBlock(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()

	esDispatch := make(chan *ffcapi.ListenerEvent)

	id := fftypes.NewUUID()

	_, err := bcm.startConfirmedBlockListener(bcm.ctx, id, "wrong", nil, esDispatch)
	assert.Regexp(t, "FF21090", err)

	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestProcessBlockHashesSwallowsFailure(t *testing.T) {
	bcm, mca := newTestBlockConfirmationManager()
	cbl := &confirmedBlockListener{
		ctx: bcm.ctx,
		bcm: bcm,
	}
	blocks := testBlockArray(1)
	mbiHash := mca.On("BlockInfoByHash", mock.Anything, mock.Anything)
	mbiHash.Return(nil, ffcapi.ErrorReasonDownstreamDown, fmt.Errorf("nope"))
	cbl.processBlockHashes([]string{blocks[0].BlockHash})
	bcm.Stop()
	mca.AssertExpectations(t)
}

func TestDispatchAllConfirmedNonBlocking(t *testing.T) {
	bcm, _ := newTestBlockConfirmationManager()
	cbl := &confirmedBlockListener{
		id:            fftypes.NewUUID(),
		ctx:           bcm.ctx,
		bcm:           bcm,
		processorDone: make(chan struct{}),
		eventStream:   make(chan<- *ffcapi.ListenerEvent), // blocks indefinitely
	}

	cbl.requiredConfirmations = 0
	cbl.blocksSinceCheckpoint = []*apitypes.BlockInfo{
		{BlockNumber: fftypes.FFuint64(12345), BlockHash: fftypes.NewRandB32().String()},
	}
	waitForDispatchReturn := make(chan struct{})
	go func() {
		defer close(waitForDispatchReturn)
		cbl.dispatchAllConfirmed()
	}()

	bcm.cancelFunc()
	bcm.Stop()
	<-waitForDispatchReturn

	// Ensure if there were a pending dispatch it wouldn't block
	close(cbl.processorDone)
	bcm.cbls = map[fftypes.UUID]*confirmedBlockListener{*cbl.id: cbl}
	bcm.propagateBlockHashToCBLs(&ffcapi.BlockHashEvent{})
}

func TestCBLDoubleStart(t *testing.T) {
	id := fftypes.NewUUID()

	bcm, mca := newTestBlockConfirmationManager()
	mca.On("BlockInfoByNumber", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReasonNotFound, fmt.Errorf("not found"))

	err := bcm.StartConfirmedBlockListener(bcm.ctx, id, ffcapi.FromBlockEarliest, nil, make(chan<- *ffcapi.ListenerEvent))
	assert.NoError(t, err)

	err = bcm.StartConfirmedBlockListener(bcm.ctx, id, ffcapi.FromBlockEarliest, nil, make(chan<- *ffcapi.ListenerEvent))
	assert.Regexp(t, "FF21087", err)

	bcm.Stop()
}

func TestCBLStopNotStarted(t *testing.T) {
	id := fftypes.NewUUID()

	bcm, _ := newTestBlockConfirmationManager()

	err := bcm.StopConfirmedBlockListener(bcm.ctx, id)
	assert.Regexp(t, "FF21088", err)

	bcm.Stop()
}

func testBlockArray(l int) []*ffcapi.BlockInfoByNumberResponse {
	blocks := make([]*ffcapi.BlockInfoByNumberResponse, l)
	for i := 0; i < l; i++ {
		blocks[i] = &ffcapi.BlockInfoByNumberResponse{
			BlockInfo: ffcapi.BlockInfo{
				BlockNumber: fftypes.NewFFBigInt(int64(i)),
				BlockHash:   fftypes.NewRandB32().String(),
			},
		}
		if i == 0 {
			blocks[i].ParentHash = fftypes.NewRandB32().String()
		} else {
			blocks[i].ParentHash = blocks[i-1].BlockHash
		}
	}
	return blocks
}

func mockBlockNumberReturn(mbiBlockInfoByNumber *mock.Call, args mock.Arguments, blocks []*ffcapi.BlockInfoByNumberResponse) {
	req := args[1].(*ffcapi.BlockInfoByNumberRequest)
	blockNo := int(req.BlockNumber.Uint64())
	if blockNo >= len(blocks) {
		mbiBlockInfoByNumber.Return(nil, ffcapi.ErrorReasonNotFound, fmt.Errorf("not found"))
	} else {
		mbiBlockInfoByNumber.Return(blocks[blockNo], ffcapi.ErrorReason(""), nil)
	}
}

func mockBlockHashReturn(mbiBlockInfoByHash *mock.Call, args mock.Arguments, blocks []*ffcapi.BlockInfoByNumberResponse) {
	req := args[1].(*ffcapi.BlockInfoByHashRequest)
	for _, b := range blocks {
		if b.BlockHash == req.BlockHash {
			mbiBlockInfoByHash.Return(&ffcapi.BlockInfoByHashResponse{
				BlockInfo: b.BlockInfo,
			}, ffcapi.ErrorReason(""), nil)
			return
		}
	}
	mbiBlockInfoByHash.Return(nil, ffcapi.ErrorReasonNotFound, fmt.Errorf("not found"))
}

func checkBlocksSequential(t *testing.T, desc string, blocks []*ffcapi.BlockInfoByNumberResponse) {
	blockHashes := make([]string, len(blocks))
	var lastBlock *ffcapi.BlockInfoByNumberResponse
	invalid := false
	for i, b := range blocks {
		assert.NotEmpty(t, b.BlockHash)
		blockHashes[i] = b.BlockHash
		if i == 0 {
			assert.NotEmpty(t, b.ParentHash)
		} else if lastBlock.BlockHash != b.ParentHash {
			invalid = true
		}
		lastBlock = b
	}
	fmt.Printf("%s: %s\n", desc, strings.Join(blockHashes, ","))
	if invalid {
		panic("wrong sequence") // aid to writing tests that build sequences
	}
}
