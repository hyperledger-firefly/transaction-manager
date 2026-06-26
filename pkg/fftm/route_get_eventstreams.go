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

package fftm

import (
	"net/http"

	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/transaction-manager/internal/persistence"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

var getEventStreams = func(m *manager) *ffapi.Route {
	route := &ffapi.Route{
		Name:            "getEventStreams",
		Path:            "/eventstreams",
		Method:          http.MethodGet,
		PathParams:      nil,
		Description:     tmmsgs.APIEndpointGetEventStreams,
		JSONInputValue:  nil,
		JSONOutputValue: func() interface{} { return []*apitypes.EventStream{} },
		JSONOutputCodes: []int{http.StatusOK},
	}
	if m.richQueryEnabled {
		route.FilterFactory = persistence.EventStreamFilters
		route.JSONHandler = func(r *ffapi.APIRequest) (output interface{}, err error) {
			return r.FilterResult(m.persistence.RichQuery().ListStreams(r.Req.Context(), r.Filter))
		}
	} else {
		// Very limited query support
		route.QueryParams = []*ffapi.QueryParam{
			{Name: "limit", Description: tmmsgs.APIParamLimit},
			{Name: "after", Description: tmmsgs.APIParamAfter},
		}
		route.JSONHandler = func(r *ffapi.APIRequest) (output interface{}, err error) {
			return m.GetStreamsByCreateTime(r.Req.Context(), r.QP["after"], r.QP["limit"])
		}
	}
	return route
}
