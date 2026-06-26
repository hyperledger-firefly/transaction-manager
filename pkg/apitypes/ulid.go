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
	"crypto/rand"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	ulid "github.com/oklog/ulid/v2"
)

var ulidReader = &ulid.LockedMonotonicReader{
	MonotonicReader: &ulid.MonotonicEntropy{
		Reader: rand.Reader,
	},
}

// NewULID returns a Universally Unique Lexicographically Sortable Identifier (ULID).
// For consistency we impersonate the formatting of a UUID, so they can be used
// interchangeably.
// This can be used in database tables to ensure monotonic increasing identifiers.
func NewULID() *fftypes.UUID {
	u := ulid.MustNew(ulid.Timestamp(time.Now()), ulidReader)
	return (*fftypes.UUID)(&u)
}
