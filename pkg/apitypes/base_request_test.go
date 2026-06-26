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

package apitypes

import (
	"encoding/json"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
)

func TestBaseRequestDecoding(t *testing.T) {

	sampleRequest := &TransactionRequest{
		Headers: RequestHeaders{
			Type: RequestTypeSendTransaction,
			ID:   fftypes.NewUUID().String(),
		},
		TransactionInput: ffcapi.TransactionInput{
			TransactionHeaders: ffcapi.TransactionHeaders{
				From: "0x12345",
			},
		},
	}

	j, err := json.Marshal(&sampleRequest)
	assert.NoError(t, err)

	var br BaseRequest
	err = json.Unmarshal(j, &br)
	assert.NoError(t, err)

	assert.Equal(t, RequestTypeSendTransaction, br.Headers.Type)
	assert.Equal(t, sampleRequest.Headers.ID, br.Headers.ID)

	var receivedRequest TransactionRequest
	err = br.UnmarshalTo(&receivedRequest)
	assert.NoError(t, err)

	assert.Equal(t, "0x12345", receivedRequest.TransactionInput.From)

}
