// Copyright © 2022 - 2026 Kaleido, Inc.
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
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// TransactionRequest is the payload sent to initiate a new transaction
type TransactionRequest struct {
	Headers RequestHeaders `json:"headers"`
	ffcapi.TransactionInput
}

// ContractDeployRequest is the payload sent to initiate a new transaction
type ContractDeployRequest struct {
	Headers RequestHeaders `json:"headers"`
	ffcapi.ContractDeployPrepareRequest
}

// SubmissionRequest combines all fields from TransactionInput and ContractDeployPrepareRequest
// The request type is determined by which fields are present:
// - If Definition and/or Contract are present, it's treated as a DeployContract request
// - Otherwise, it's treated as a SendTransaction request
type SubmissionRequest struct {
	// Common fields for all requests
	ID string `ffstruct:"fftmrequest" json:"id"`
	ffcapi.TransactionHeaders
	Params []*fftypes.JSONAny `json:"params,omitempty"`
	Errors []*fftypes.JSONAny `json:"errors,omitempty"`

	// TransactionInput fields
	Method *fftypes.JSONAny `json:"method,omitempty"`

	// ContractDeployPrepareRequest fields
	Definition *fftypes.JSONAny `json:"definition,omitempty"` // such as an ABI for EVM
	Contract   *fftypes.JSONAny `json:"contract,omitempty"`   // such as the Bytecode for EVM
}

// SubmissionResponse is the response to a single submission request
type SubmissionResponse struct {
	ID      string      `json:"id,omitempty"`     // ID from the request headers, if present
	Success bool        `json:"success"`          // Whether this request succeeded
	Output  interface{} `json:"output,omitempty"` // The successful response (ManagedTX for SendTransaction/Deploy)
	Error   interface{} `json:"error,omitempty"`  // Error (SubmissionError for SendTransaction/Deploy, string for others) if success is false
}
