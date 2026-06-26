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
	"net/http"

	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"

	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

// deprecatedGetStatus deprecated, is present for backwards compatibility with the previous generation of connectors i.e. Ethconnect
var deprecatedGetStatus = func(m *manager) *ffapi.Route {
	return &ffapi.Route{
		Name:            "getStatus",
		Path:            "/status",
		Method:          http.MethodGet,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     tmmsgs.APIEndpointGetStatus,
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return &apitypes.LiveStatus{} },
		JSONOutputCodes: []int{http.StatusOK},
		JSONHandler: func(r *ffapi.APIRequest) (output interface{}, err error) {
			return m.getLiveStatus(r.Req.Context())
		},
	}
}

var deprecatedGetLiveStatus = func(m *manager) *ffapi.Route {
	return &ffapi.Route{
		Name:            "getLiveStatus",
		Path:            "/status/live",
		Method:          http.MethodGet,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     tmmsgs.APIEndpointGetStatusLive,
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return &apitypes.LiveStatus{} },
		JSONOutputCodes: []int{http.StatusOK},
		JSONHandler: func(r *ffapi.APIRequest) (output interface{}, err error) {
			return m.getLiveStatus(r.Req.Context())
		},
	}
}

var getLiveness = func(m *manager) *ffapi.Route {
	return &ffapi.Route{
		Name:            "getLiveness",
		Path:            "/livez",
		Method:          http.MethodGet,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     tmmsgs.APIEndpointGetLiveness,
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return &apitypes.LiveStatus{} },
		JSONOutputCodes: []int{http.StatusOK},
		JSONHandler: func(r *ffapi.APIRequest) (output interface{}, err error) {
			return m.getLiveStatus(r.Req.Context())
		},
	}
}
