// Copyright © 2026 Kaleido, Inc.
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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/transaction-manager/mocks/txhandlermocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPostSubmissions(t *testing.T) {
	url, m, done := newTestManager(t)
	defer done()

	err := m.Start()
	assert.NoError(t, err)

	// Mock the transaction handler directly
	mth := txhandlermocks.TransactionHandler{}
	mtx1 := &apitypes.ManagedTX{
		ID: "tx1",
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x111111",
			To:   "0x222222",
		},
	}
	mtx2 := &apitypes.ManagedTX{
		ID: "tx2",
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x111111",
			To:   "0x222222",
		},
	}
	mth.On("HandleSubmissions", mock.Anything, mock.MatchedBy(func(submissions []*apitypes.SubmissionRequest) bool {
		return len(submissions) == 2 && submissions[0].ID == "tx1" && submissions[1].ID == "tx2"
	})).Return([]*apitypes.SubmissionResponse{
		{ID: "tx1", Success: true, Output: mtx1},
		{ID: "tx2", Success: true, Output: mtx2},
	})
	m.txHandler = &mth

	// Create a batch request with multiple SendTransaction requests as JSON
	batchReqJSON := `{
		"requests": [
			{
				"id": "tx1",
				"from": "0x111111",
				"to": "0x222222",
				"method": {"type":"function","name":"test"},
				"params": ["value1"]
			},
			{
				"id": "tx2",
				"from": "0x111111",
				"to": "0x222222",
				"method": {"type":"function","name":"test"},
				"params": ["value2"]
			}
		]
	}`

	var batchResp apitypes.BatchResponse
	res, err := resty.New().
		SetTimeout(10*time.Second).
		R().
		SetHeader("Content-Type", "application/json").
		SetBody(batchReqJSON).
		SetResult(&batchResp).
		Post(url + "/submit")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Len(t, batchResp.Responses, 2)

	// Both should succeed
	for i, resp := range batchResp.Responses {
		assert.True(t, resp.Success, "Request %d should succeed", i)
		assert.Nil(t, resp.Error, "Request %d should have no error", i)
		assert.NotNil(t, resp.Output, "Request %d should have output", i)
		assert.Equal(t, fmt.Sprintf("tx%d", i+1), resp.ID)
	}
}

func TestPostSubmissionsMixedDeploymentsAndTransactions(t *testing.T) {
	url, m, done := newTestManager(t)
	defer done()

	err := m.Start()
	assert.NoError(t, err)

	// Mock the transaction handler directly
	mth := txhandlermocks.TransactionHandler{}
	mtx1 := &apitypes.ManagedTX{
		ID: "tx1",
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x111111",
			To:   "0x222222",
		},
	}
	mtx2 := &apitypes.ManagedTX{
		ID: "tx2",
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x111111",
			To:   "0x222222",
		},
	}
	deploy1 := &apitypes.ManagedTX{
		ID: "deploy1",
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x111111",
		},
	}
	mth.On("HandleSubmissions", mock.Anything, mock.MatchedBy(func(submissions []*apitypes.SubmissionRequest) bool {
		return len(submissions) == 3 && submissions[0].ID == "tx1" && submissions[1].ID == "deploy1" && submissions[2].ID == "tx2"
	})).Return([]*apitypes.SubmissionResponse{
		{ID: "tx1", Success: true, Output: mtx1},
		{ID: "deploy1", Success: true, Output: deploy1},
		{ID: "tx2", Success: true, Output: mtx2},
	})
	m.txHandler = &mth

	// Create a batch request with both SendTransaction and Deploy requests as JSON
	batchReqJSON := `{
		"requests": [
			{
				"id": "tx1",
				"from": "0x111111",
				"to": "0x222222",
				"method": {"type":"function","name":"test"},
				"params": ["value1"]
			},
			{
				"id": "deploy1",
				"from": "0x111111",
				"definition": {"abi":[]},
				"contract": "0xbytecode"
			},
			{
				"id": "tx2",
				"from": "0x111111",
				"to": "0x222222",
				"method": {"type":"function","name":"test"},
				"params": ["value2"]
			}
		]
	}`

	var batchResp apitypes.BatchResponse
	res, err := resty.New().
		SetTimeout(10*time.Second).
		R().
		SetHeader("Content-Type", "application/json").
		SetBody(batchReqJSON).
		SetResult(&batchResp).
		Post(url + "/submit")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Len(t, batchResp.Responses, 3)

	// All should succeed
	for i, resp := range batchResp.Responses {
		assert.True(t, resp.Success, "Request %d should succeed", i)
		assert.Nil(t, resp.Error, "Request %d should have no error", i)
		assert.NotNil(t, resp.Output, "Request %d should have output", i)
	}
}

func TestPostSubmissionsInvalidRequest(t *testing.T) {
	url, m, done := newTestManager(t)
	defer done()

	err := m.Start()
	assert.NoError(t, err)

	// Mock the transaction handler directly
	mth := txhandlermocks.TransactionHandler{}
	mtx1 := &apitypes.ManagedTX{
		ID: "tx1",
		TransactionHeaders: ffcapi.TransactionHeaders{
			From: "0x111111",
			To:   "0x222222",
		},
	}
	// First transaction succeeds, second fails (empty From address)
	mth.On("HandleSubmissions", mock.Anything, mock.MatchedBy(func(submissions []*apitypes.SubmissionRequest) bool {
		return len(submissions) == 2 && submissions[0].ID == "tx1" && submissions[1].ID == "tx2"
	})).Return([]*apitypes.SubmissionResponse{
		{ID: "tx1", Success: true, Output: mtx1},
		{ID: "tx2", Success: false, Error: &ffcapi.SubmissionError{
			Error:              "No From address provided",
			SubmissionRejected: false,
		}},
	})
	m.txHandler = &mth

	// Create a batch request with missing required fields in one request
	batchReqJSON := `{
		"requests": [
			{
				"id": "tx1",
				"from": "0x111111",
				"to": "0x222222",
				"method": {"type":"function","name":"test"},
				"params": ["value1"]
			},
			{
				"id": "tx2",
				"from": ""
			}
		]
	}`

	var batchResp apitypes.BatchResponse
	res, err := resty.New().
		SetTimeout(10*time.Second).
		R().
		SetHeader("Content-Type", "application/json").
		SetBody(batchReqJSON).
		SetResult(&batchResp).
		Post(url + "/submit")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Len(t, batchResp.Responses, 2)

	// First should succeed, second should fail
	assert.True(t, batchResp.Responses[0].Success)
	assert.False(t, batchResp.Responses[1].Success)
	assert.NotNil(t, batchResp.Responses[1].Error)

	// Error should be a SubmissionError object
	var submissionErr ffcapi.SubmissionError
	errBytes, _ := json.Marshal(batchResp.Responses[1].Error)
	err = json.Unmarshal(errBytes, &submissionErr)
	assert.NoError(t, err)
	assert.NotEmpty(t, submissionErr.Error)
}

func TestPostSubmissionsEmpty(t *testing.T) {
	url, m, done := newTestManager(t)
	defer done()
	err := m.Start()
	assert.NoError(t, err)

	// Mock the transaction handler directly
	mth := txhandlermocks.TransactionHandler{}
	mth.On("HandleSubmissions", mock.Anything, mock.MatchedBy(func(submissions []*apitypes.SubmissionRequest) bool {
		return len(submissions) == 0
	})).Return([]*apitypes.SubmissionResponse{})
	m.txHandler = &mth

	// Create an empty batch request as JSON
	batchReqJSON := `{
		"requests": []
	}`

	var batchResp apitypes.BatchResponse
	res, err := resty.New().
		SetTimeout(10*time.Second).
		R().
		SetHeader("Content-Type", "application/json").
		SetBody(batchReqJSON).
		SetResult(&batchResp).
		Post(url + "/submit")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
	assert.Len(t, batchResp.Responses, 0)
}
