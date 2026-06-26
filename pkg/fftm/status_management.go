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
	"context"

	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func (m *manager) getLiveStatus(ctx context.Context) (resp *apitypes.LiveStatus, err error) {
	resp = &apitypes.LiveStatus{}
	status, _, err := m.connector.IsLive(ctx)
	if err == nil {
		resp.LiveResponse = *status
	} else {
		log.L(ctx).Warnf("Failed to fetch live status: %s", err)
		return nil, err
	}
	return resp, nil
}

func (m *manager) getReadyStatus(ctx context.Context) (resp *apitypes.ReadyStatus, err error) {
	resp = &apitypes.ReadyStatus{}
	status, _, err := m.connector.IsReady(ctx)
	if err == nil {
		resp.ReadyResponse = *status
	} else {
		log.L(ctx).Warnf("Failed to fetch ready status: %s", err)
		return nil, err
	}
	return resp, nil
}
