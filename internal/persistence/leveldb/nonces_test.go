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

package leveldb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
)

func TestNonceStaleStateContention(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	// Write a stale record to persistence
	oldTime := fftypes.FFTime(time.Now().Add(-100 * time.Hour))
	err := p.writeTransaction(ctx, &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{
			ID:      "stale1",
			Created: &oldTime,
			Status:  apitypes.TxStatusSucceeded,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  "0x12345",
				Nonce: fftypes.NewFFBigInt(1000),
			},
		},
	}, true)
	assert.NoError(t, err)

	locked1 := make(chan struct{})
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	go func() {
		defer close(done1)

		ln, err := p.assignAndLockNonce(context.Background(), "ns1:"+fftypes.NewUUID().String(), "0x12345", func(ctx context.Context, signer string) (uint64, error) {
			assert.Equal(t, "0x12345", signer)
			return 1111, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, uint64(1111), ln.nonce)
		defer ln.complete(ctx)
		close(locked1)

		time.Sleep(1 * time.Millisecond)
		err = p.writeTransaction(ctx, &apitypes.TXWithStatus{
			ManagedTX: &apitypes.ManagedTX{
				ID:      "ns1:" + fftypes.NewUUID().String(),
				Created: fftypes.Now(),
				Status:  apitypes.TxStatusPending,
				TransactionHeaders: ffcapi.TransactionHeaders{
					From:  "0x12345",
					Nonce: fftypes.NewFFBigInt(int64(ln.nonce)),
				},
			},
		}, true)
		assert.NoError(t, err)
	}()

	go func() {
		defer close(done2)

		<-locked1
		ln, err := p.assignAndLockNonce(context.Background(), "ns2:"+fftypes.NewUUID().String(), "0x12345", func(ctx context.Context, signer string) (uint64, error) {
			panic("should not be called")
		})
		assert.NoError(t, err)

		assert.Equal(t, uint64(1112), ln.nonce)

		ln.complete(context.Background())

	}()

	<-done1
	<-done2

}

func TestNonceListError(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	done()

	err := p.InsertTransactionWithNextNonce(ctx, &apitypes.ManagedTX{
		ID:     "ns1:" + fftypes.NewUUID().String(),
		Status: apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x12345",
		},
	}, func(ctx context.Context, signer string) (uint64, error) {
		panic("should not be called")
	})
	assert.Regexp(t, "leveldb: closed", err)

}

func TestNonceListStaleThenQueryFail(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()

	err := p.InsertTransactionWithNextNonce(ctx, &apitypes.ManagedTX{
		ID:     "ns1:" + fftypes.NewUUID().String(),
		Status: apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x12345",
		},
	}, func(ctx context.Context, signer string) (uint64, error) {
		return 0, fmt.Errorf("pop")
	})
	assert.Regexp(t, "pop", err)

}

func TestNonceListNotStale(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	p.nonceStateTimeout = 1 * time.Hour

	tx1 := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{
			ID:      "stale1",
			Created: fftypes.Now(),
			Status:  apitypes.TxStatusSucceeded,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  "0x12345",
				Nonce: fftypes.NewFFBigInt(1000),
			},
		},
	}
	err := p.writeTransaction(ctx, tx1, true)
	assert.NoError(t, err)

	tx2 := &apitypes.ManagedTX{
		ID:      "ns1:" + fftypes.NewUUID().String(),
		Created: fftypes.Now(),
		Status:  apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x12345",
		},
	}
	err = p.InsertTransactionWithNextNonce(ctx, tx2, func(ctx context.Context, signer string) (uint64, error) {
		panic("should not be called")
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1001), tx2.Nonce.Int64())

}

func TestNonceListStaleBehind(t *testing.T) {

	ctx, p, done := newTestLevelDBPersistence(t)
	defer done()
	p.nonceStateTimeout = 1 * time.Hour

	oldTime := fftypes.FFTime(time.Now().Add(-100 * time.Hour))
	tx1 := &apitypes.TXWithStatus{
		ManagedTX: &apitypes.ManagedTX{
			ID:      "stale1",
			Created: &oldTime,
			Status:  apitypes.TxStatusSucceeded,
			TransactionHeaders: ffcapi.TransactionHeaders{
				From:  "0x12345",
				Nonce: fftypes.NewFFBigInt(1001),
			},
		},
	}
	err := p.writeTransaction(ctx, tx1, true)
	assert.NoError(t, err)

	tx2 := &apitypes.ManagedTX{
		ID:      "ns1:" + fftypes.NewUUID().String(),
		Created: fftypes.Now(),
		Status:  apitypes.TxStatusPending,
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x12345",
		},
	}
	err = p.InsertTransactionWithNextNonce(ctx, tx2, func(ctx context.Context, signer string) (uint64, error) {
		return 1000, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1002), tx2.Nonce.Int64())

}
