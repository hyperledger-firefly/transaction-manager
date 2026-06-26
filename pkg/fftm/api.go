// Copyright © 2025 Kaleido, Inc.
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

	"github.com/gorilla/mux"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
)

var testDescriptions = false

func (m *manager) router(metricsEnabled bool) *mux.Router {
	mux := mux.NewRouter()

	if metricsEnabled {
		mux.Use(m.metricsManager.GetAPIServerRESTHTTPMiddleware())
	}
	hf := ffapi.HandlerFactory{
		DefaultRequestTimeout: config.GetDuration(tmconfig.APIDefaultRequestTimeout),
		MaxTimeout:            config.GetDuration(tmconfig.APIMaxRequestTimeout),
		PassthroughHeaders:    config.GetStringSlice(tmconfig.APIPassthroughHeaders),
	}
	oah := &ffapi.OpenAPIHandlerFactory{
		BaseSwaggerGenOptions: ffapi.SwaggerGenOptions{
			Title:                     "FireFly Transaction Manager API",
			Version:                   "1.0",
			PanicOnMissingDescription: testDescriptions,
			DefaultRequestTimeout:     config.GetDuration(tmconfig.APIDefaultRequestTimeout),
			SupportFieldRedaction:     true,
		},
	}

	routes := m.routes()
	for _, r := range routes {
		mux.Path(r.Path).Methods(r.Method).Handler(hf.RouteHandler(r))
	}
	mux.Path("/api").Methods(http.MethodGet).Handler(hf.APIWrapper(oah.SwaggerUIHandler("")))
	mux.Path("/api/spec.yaml").Methods(http.MethodGet).Handler(hf.APIWrapper(oah.OpenAPIHandler("", ffapi.OpenAPIFormatYAML, routes)))
	mux.Path("/api/spec.json").Methods(http.MethodGet).Handler(hf.APIWrapper(oah.OpenAPIHandler("", ffapi.OpenAPIFormatJSON, routes)))

	mux.HandleFunc("/ws", m.wsServer.Handler)

	mux.NotFoundHandler = hf.APIWrapper(func(_ http.ResponseWriter, req *http.Request) (status int, err error) {
		return 404, i18n.NewError(req.Context(), i18n.Msg404NotFound)
	})
	return mux
}

func (m *manager) createMonitoringMuxRouter() *mux.Router {
	r := mux.NewRouter()

	if config.GetBool(tmconfig.DeprecatedMetricsEnabled) {
		r.Path(config.GetString(tmconfig.DeprecatedMetricsPath)).Handler(m.metricsManager.HTTPHandler())
	} else {
		r.Path(config.GetString(tmconfig.MonitoringMetricsPath)).Handler(m.metricsManager.HTTPHandler())
	}
	hf := ffapi.HandlerFactory{
		DefaultRequestTimeout: config.GetDuration(tmconfig.APIDefaultRequestTimeout),
		MaxTimeout:            config.GetDuration(tmconfig.APIMaxRequestTimeout),
	}
	for _, route := range m.monitoringRoutes() {
		r.Path(route.Path).Methods(route.Method).Handler(hf.RouteHandler(route))
	}
	r.NotFoundHandler = hf.APIWrapper(func(_ http.ResponseWriter, req *http.Request) (status int, err error) {
		return 404, i18n.NewError(req.Context(), i18n.Msg404NotFound)
	})
	return r
}

func (m *manager) runAPIServer() {
	m.apiServer.ServeHTTP(m.ctx)
}

func (m *manager) runMonitoringServer() {
	m.monitoringServer.ServeHTTP(m.ctx)
}
