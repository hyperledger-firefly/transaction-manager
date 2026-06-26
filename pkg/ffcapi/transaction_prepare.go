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

package ffcapi

import (
	"github.com/hyperledger-firefly/common/pkg/fftypes"
)

// TransactionPrepareRequest is used to prepare a set of JSON formatted developer friendly
// inputs, into a raw transaction ready for submission to the blockchain.
//
// The connector is responsible for encoding the transaction ready for submission,
// and returning the hash for the transaction as well as a string serialization of
// the pre-signed raw transaction in a format of its own choosing (hex etc.).
// The hash is expected to be a function of:
// - the method signature
// - the signing identity
// - the nonce
// - the particular blockchain the transaction is submitted to
// - the input parameters
//
// If "gas" is not supplied, the connector is expected to perform gas estimation
// prior to generating the payload.
//
// See the list of standard error reasons that should be returned for situations that can be
// detected by the back-end connector.
type TransactionPrepareRequest struct {
	TransactionInput
}

type TransactionPrepareResponse struct {
	Gas             *fftypes.FFBigInt `json:"gas"`
	TransactionData string            `json:"transactionData"`
}
