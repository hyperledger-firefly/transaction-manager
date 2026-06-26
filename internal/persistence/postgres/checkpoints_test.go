// Copyright © 2023 Kaleido, Inc.
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

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

func TestCheckpointsPSQ(t *testing.T) {

	ctx, p, _, done := initTestPSQL(t)
	defer done()

	cp1 := &apitypes.EventStreamCheckpoint{
		StreamID: fftypes.NewUUID(),
		Listeners: apitypes.CheckpointListeners{
			*fftypes.NewUUID(): json.RawMessage(`{"some":"data"}`),
		},
	}
	err := p.WriteCheckpoint(ctx, cp1)
	assert.NoError(t, err)

	cp2, err := p.GetCheckpoint(ctx, cp1.StreamID)
	assert.NoError(t, err)
	assert.NotNil(t, cp2.Time)
	assert.NotNil(t, cp2.FirstCheckpoint)
	cp1.Time = cp2.Time
	cp1.FirstCheckpoint = cp2.FirstCheckpoint
	assert.Equal(t, cp1, cp2)

	err = p.DeleteCheckpoint(ctx, cp1.StreamID)
	assert.NoError(t, err)

	cp3, err := p.GetCheckpoint(ctx, cp1.StreamID)
	assert.Nil(t, cp3)

}
