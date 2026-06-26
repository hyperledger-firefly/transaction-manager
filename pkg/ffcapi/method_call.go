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

package ffcapi

import (
	"github.com/hyperledger-firefly/common/pkg/fftypes"
)

// QueryInvokeRequest requests execution of a smart contract method in order to either:
// 1) Query state
// 2) Attempt to extract the revert reason from an on-chain failure to execute a transaction
//
// See the list of standard error reasons that should be returned for situations that can be
// detected by the back-end connector.
type QueryInvokeRequest struct {
	TransactionInput
	BlockNumber *string `json:"blockNumber,omitempty"`
}

type QueryInvokeResponse struct {
	Outputs *fftypes.JSONAny `json:"outputs"` // The data output from the method call - can be array or object structure
}
