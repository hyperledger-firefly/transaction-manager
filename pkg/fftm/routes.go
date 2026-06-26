// Copyright © 2025 - 2026 Kaleido, Inc.
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

import "github.com/hyperledger-firefly/common/pkg/ffapi"

func (m *manager) routes() []*ffapi.Route {
	return []*ffapi.Route{
		deleteEventStream(m),
		deleteEventStreamListener(m),
		deleteSubscription(m),
		deleteTransaction(m),
		getEventStream(m),
		getEventStreamListener(m),
		getEventStreamListeners(m),
		getEventStreams(m),
		deprecatedGetStatus(m),
		getSubscription(m),
		getSubscriptions(m),
		deprecatedGetLiveStatus(m),  // TODO: remove this route from the API routes, they are already in the monitoring routes
		deprecatedGetReadyStatus(m), // TODO: remove this route from the API routes, they are already in the monitoring routes
		getTransaction(m),
		getTransactionConfirmations(m),
		getTransactionHistory(m),
		getTransactionReceipt(m),
		getTransactions(m),
		patchEventStream(m),
		patchEventStreamListener(m),
		patchSubscription(m),
		postEventStream(m),
		postEventStreamListenerReset(m),
		postEventStreamListeners(m),
		postEventStreamResume(m),
		postEventStreamSuspend(m),
		postSubmit(m),
		postRootCommand(m),
		postSubscriptionReset(m),
		postSubscriptions(m),
		getAddressBalance(m),
		getGasPrice(m),
		postTransactionSuspend(m),
		postTransactionResume(m),
		patchTransaction(m),
	}
}

func (m *manager) monitoringRoutes() []*ffapi.Route {
	return []*ffapi.Route{
		getReadiness(m),
		getLiveness(m),
	}
}
