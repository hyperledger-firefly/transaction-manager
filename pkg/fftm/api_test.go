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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/internal/confirmations"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/mocks/confirmationsmocks"
	"github.com/hyperledger-firefly/transaction-manager/mocks/ffcapimocks"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sampleSendTX = `{
	"headers": {
		"id": "ns1:904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "SendTransaction"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"to": "0xe1a078b9e2b145d0a7387f09277c6ae1d9470771",
	"gas": 1000000,
	"method": {
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			}
		],
		"name":"set",
		"outputs":[],
		"stateMutability":"nonpayable",
		"type":"function"
	},
	"params": [
		{
			"value": 4276993775,
			"type": "uint256"
		}
	]
}`

const sampleDeployTX = `{
	"headers": {
		"id": "ns1:904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "DeployContract"
	},
	"from": "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8",
	"gas": 1000000,
	"contract": "0xfeedbeef",
	"definition": [{
		"inputs": [
			{
				"internalType":" uint256",
				"name": "x",
				"type": "uint256"
			}
		],
		"type":"constructor"
	}],
	"params": [
		{
			"value": 4276993775,
			"type": "uint256"
		}
	]
}`

func TestSendTransactionE2E(t *testing.T) {

	txSent := make(chan struct{})

	url, m, cancel := newTestManager(t)
	defer cancel()

	mFFC := m.connector.(*ffcapimocks.API)

	mFFC.On("NextNonceForSigner", mock.Anything, mock.MatchedBy(func(nonceReq *ffcapi.NextNonceForSignerRequest) bool {
		return "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8" == nonceReq.Signer
	})).Return(&ffcapi.NextNonceForSignerResponse{
		Nonce: fftypes.NewFFBigInt(12345),
	}, ffcapi.ErrorReason(""), nil)

	mFFC.On("TransactionPrepare", mock.Anything, mock.MatchedBy(func(prepTX *ffcapi.TransactionPrepareRequest) bool {
		return "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8" == prepTX.From &&
			"0xe1a078b9e2b145d0a7387f09277c6ae1d9470771" == prepTX.To &&
			uint64(1000000) == prepTX.Gas.Uint64() &&
			"set" == prepTX.Method.JSONObject().GetString("name") &&
			1 == len(prepTX.Params) &&
			"4276993775" == prepTX.Params[0].JSONObject().GetString("value") &&
			"4276993775" == prepTX.Params[0].JSONObject().GetString("value")
	})).Return(&ffcapi.TransactionPrepareResponse{
		TransactionData: "RAW_UNSIGNED_BYTES",
		Gas:             fftypes.NewFFBigInt(2000000), // gas estimate simulation
	}, ffcapi.ErrorReason(""), nil)

	mFFC.On("TransactionSend", mock.Anything, mock.MatchedBy(func(sendTX *ffcapi.TransactionSendRequest) bool {
		matches := "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8" == sendTX.From &&
			"0xe1a078b9e2b145d0a7387f09277c6ae1d9470771" == sendTX.To &&
			uint64(2000000) == sendTX.Gas.Uint64() &&
			`223344556677` == sendTX.GasPrice.String() &&
			"RAW_UNSIGNED_BYTES" == sendTX.TransactionData
		if matches {
			// We're at end of job for this test
			close(txSent)
		}
		return matches
	})).Return(&ffcapi.TransactionSendResponse{
		TransactionHash: "0x106215b9c0c9372e3f541beff0cdc3cd061a26f69f3808e28fd139a1abc9d345",
	}, ffcapi.ErrorReason(""), nil)

	mc := m.confirmations.(*confirmationsmocks.Manager)
	mc.On("Notify", mock.MatchedBy(func(n *confirmations.Notification) bool {
		return n.NotificationType == confirmations.NewTransaction
	})).Return(nil)

	m.Start()

	req := strings.NewReader(sampleSendTX)
	res, err := resty.New().R().
		SetBody(req).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 202, res.StatusCode())

	<-txSent

}

func TestSendInvalidRequestBadTXType(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	req := strings.NewReader(`{
		"headers": {
			"type": "SendTransaction"
		},
		"from": {
			"Not": "a string"
		}
	}`)
	var errRes ffcapi.SubmissionError
	res, err := resty.New().R().
		SetBody(req).
		SetError(&errRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())
	assert.True(t, errRes.SubmissionRejected)
	assert.Regexp(t, "FF21022", errRes.Error)
}

func TestSendInvalidDeployBadTXType(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	req := strings.NewReader(`{
		"headers": {
			"type": "DeployContract"
		},
		"from": {
			"Not": "a string"
		}
	}`)
	var errRes ffcapi.SubmissionError
	res, err := resty.New().R().
		SetBody(req).
		SetError(&errRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())
	assert.True(t, errRes.SubmissionRejected)
	assert.Regexp(t, "FF21022", errRes.Error)
}

func TestSwaggerEndpoints(t *testing.T) {

	// TODO: Add field descriptions
	// testDescriptions = true
	// defer func() {
	// 	testDescriptions = false
	// }()

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	res, err := resty.New().R().SetDoNotParseResponse(true).Get(url + "/api/spec.json")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())

	res, err = resty.New().R().SetDoNotParseResponse(true).Get(url + "/api/spec.yaml")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())

	res, err = resty.New().R().SetDoNotParseResponse(true).Get(url + "/api")
	assert.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode())
}

func TestSendInvalidRequestWrongType(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	req := strings.NewReader(`{
		"headers": {
			"id": "ns1:` + fftypes.NewUUID().String() + `",
			"type": "wrong"
		}
	}`)
	var errRes fftypes.RESTError
	res, err := resty.New().R().
		SetBody(req).
		SetError(&errRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())
	assert.Regexp(t, "FF21023", errRes.Error)
}

func TestSendTransactionPrepareFail(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()

	mFFC := m.connector.(*ffcapimocks.API)

	mFFC.On("NextNonceForSigner", mock.Anything, mock.MatchedBy(func(nonceReq *ffcapi.NextNonceForSignerRequest) bool {
		return "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8" == nonceReq.Signer
	})).Return(&ffcapi.NextNonceForSignerResponse{
		Nonce: fftypes.NewFFBigInt(12345),
	}, ffcapi.ErrorReason(""), nil)

	mFFC.On("TransactionPrepare", mock.Anything, mock.Anything).
		// Reverted reason will give us a submission error
		Return(nil, ffcapi.ErrorReasonTransactionReverted, i18n.NewError(m.ctx, tmmsgs.MsgTransactionFailed))

	m.Start()

	req := strings.NewReader(sampleSendTX)
	var errRes ffcapi.SubmissionError
	res, err := resty.New().R().
		SetBody(req).
		SetError(&errRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())
	assert.True(t, errRes.SubmissionRejected)

}

func TestDeployContractPrepareFail(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()

	mFFC := m.connector.(*ffcapimocks.API)

	mFFC.On("NextNonceForSigner", mock.Anything, mock.MatchedBy(func(nonceReq *ffcapi.NextNonceForSignerRequest) bool {
		return "0xb480F96c0a3d6E9e9a263e4665a39bFa6c4d01E8" == nonceReq.Signer
	})).Return(&ffcapi.NextNonceForSignerResponse{
		Nonce: fftypes.NewFFBigInt(12345),
	}, ffcapi.ErrorReason(""), nil)

	mFFC.On("DeployContractPrepare", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	m.Start()

	req := strings.NewReader(sampleDeployTX)
	res, err := resty.New().R().
		SetBody(req).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())

}

func TestQueryOK(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	mca := m.connector.(*ffcapimocks.API)
	mca.On("QueryInvoke", mock.Anything, mock.MatchedBy(func(req *ffcapi.QueryInvokeRequest) bool {
		return req.Method.String() == `"some method details"`
	})).Return(&ffcapi.QueryInvokeResponse{
		Outputs: fftypes.JSONAnyPtr(`"some output data"`),
	}, ffcapi.ErrorReason(""), nil)

	var queryRes string
	res, err := resty.New().R().
		SetBody(&apitypes.QueryRequest{
			Headers: apitypes.RequestHeaders{
				ID:   fftypes.NewUUID().String(),
				Type: apitypes.RequestTypeQuery,
			},
			TransactionInput: ffcapi.TransactionInput{
				Method: fftypes.JSONAnyPtr(`"some method details"`),
			},
		}).
		SetResult(&queryRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 202, res.StatusCode())

	assert.Equal(t, `some output data`, queryRes)

	mca.AssertExpectations(t)

}

func TestQueryFail(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	mca := m.connector.(*ffcapimocks.API)
	mca.On("QueryInvoke", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	res, err := resty.New().R().
		SetBody(&apitypes.QueryRequest{
			Headers: apitypes.RequestHeaders{
				ID:   fftypes.NewUUID().String(),
				Type: apitypes.RequestTypeQuery,
			},
			TransactionInput: ffcapi.TransactionInput{
				Method: fftypes.JSONAnyPtr(`"some method details"`),
			},
		}).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())

	mca.AssertExpectations(t)

}

func TestQueryBadRequest(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	var errRes fftypes.RESTError
	res, err := resty.New().R().
		SetBody(`{
				"headers": {
					"id": "`+fftypes.NewUUID().String()+`",
					"type": "Query"
				},
				"params": "not an array"
			}`,
		).
		SetHeader("content-type", "application/json").
		SetError(&errRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())
	assert.Regexp(t, "FF21022", errRes.Error)

}

func TestNotFound(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	var errRes fftypes.RESTError
	res, err := resty.New().R().
		SetError(&errRes).
		Post(url + "/not found")
	assert.NoError(t, err)
	assert.Equal(t, 404, res.StatusCode())
	assert.Regexp(t, "FF00167", errRes.Error)
}

func TestTransactionReceiptOK(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	mca := m.connector.(*ffcapimocks.API)
	mca.On("TransactionReceipt", mock.Anything, mock.MatchedBy(func(req *ffcapi.TransactionReceiptRequest) bool {
		return req.TransactionHash == `0x12345`
	})).Return(&ffcapi.TransactionReceiptResponse{
		TransactionReceiptResponseBase: ffcapi.TransactionReceiptResponseBase{
			BlockHash:        "0x111111",
			BlockNumber:      fftypes.NewFFBigInt(10000),
			TransactionIndex: fftypes.NewFFBigInt(10),
			ProtocolID:       "111/222/333",
			Success:          true,
		},
		Events: []*ffcapi.Event{
			{
				ID: ffcapi.EventID{Signature: "MyEvent()"},
			},
		},
	}, ffcapi.ErrorReason(""), nil)

	var queryRes map[string]interface{}
	res, err := resty.New().R().
		SetBody(&apitypes.TransactionReceiptRequest{
			Headers: apitypes.RequestHeaders{
				ID:   fftypes.NewUUID().String(),
				Type: apitypes.RequestTypeTransactionReceipt,
			},
			TransactionReceiptRequest: ffcapi.TransactionReceiptRequest{
				TransactionHash: "0x12345",
			},
		}).
		SetResult(&queryRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 202, res.StatusCode())

	d, _ := json.Marshal(queryRes)
	assert.JSONEq(t, `{
		"blockHash": "0x111111",
		"blockNumber": "10000",
		"protocolId": "111/222/333",
		"success": true,
		"transactionIndex": "10",
		"events": [
			{
				"blockHash": "",
				"blockNumber": "0",
				"data": null,
				"logIndex": "0",
				"signature": "MyEvent()",
				"transactionHash": "",
				"transactionIndex": "0"
			}
		]
	}`, string(d))

	mca.AssertExpectations(t)

}

func TestTransactionReceiptFail(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	mca := m.connector.(*ffcapimocks.API)
	mca.On("TransactionReceipt", mock.Anything, mock.Anything).Return(nil, ffcapi.ErrorReason(""), fmt.Errorf("pop"))

	res, err := resty.New().R().
		SetBody(&apitypes.TransactionReceiptRequest{
			Headers: apitypes.RequestHeaders{
				ID:   fftypes.NewUUID().String(),
				Type: apitypes.RequestTypeTransactionReceipt,
			},
			TransactionReceiptRequest: ffcapi.TransactionReceiptRequest{
				TransactionHash: "0x12345",
			},
		}).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 500, res.StatusCode())

	mca.AssertExpectations(t)

}

func TestTransactionReceiptBadRequest(t *testing.T) {

	url, m, cancel := newTestManager(t)
	defer cancel()
	m.Start()

	var errRes fftypes.RESTError
	res, err := resty.New().R().
		SetBody(`{
				"headers": {
					"id": "`+fftypes.NewUUID().String()+`",
					"type": "TransactionReceipt"
				},
				"eventFilters": "not an array"
			}`,
		).
		SetHeader("content-type", "application/json").
		SetError(&errRes).
		Post(url)
	assert.NoError(t, err)
	assert.Equal(t, 400, res.StatusCode())
	assert.Regexp(t, "FF21022", errRes.Error)

}
