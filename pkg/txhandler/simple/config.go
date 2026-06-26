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

package simple

import (
	"net/http"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
)

const (
	MaxInFlight = "maxInFlight"

	Interval       = "interval"
	RetryInitDelay = "retry.initialDelay"
	RetryMaxDelay  = "retry.maxDelay"
	RetryFactor    = "retry.factor"

	FixedGasPrice          = "fixedGasPrice"    // when not using a gas station - will be treated as a raw JSON string, so can be numeric 123, or string "123", or object {"maxPriorityFeePerGas":123})
	ResubmitInterval       = "resubmitInterval" // warnings will be written to the log at this interval if mining has not occurred, and the TX will be resubmitted
	GasOracleConfig        = "gasOracle"
	GasOracleMode          = "mode"
	GasOracleMethod        = "method"
	GasOracleTemplate      = "template"
	GasOracleQueryInterval = "queryInterval"
)

const (
	GasOracleModeDisabled  = "disabled"
	GasOracleModeRESTAPI   = "restapi"
	GasOracleModeConnector = "connector"

	defaultMaxInFlight    = 100
	defaultInterval       = "10s"
	defaultRetryInitDelay = "250ms"
	defaultRetryMaxDelay  = "30s"
	defaultRetryFactor    = 2.0
)

const (
	defaultResubmitInterval       = "5m"
	defaultGasOracleQueryInterval = "5m"
	defaultGasOracleMethod        = http.MethodGet
	defaultGasOracleMode          = GasOracleModeConnector
)

func (f *TransactionHandlerFactory) InitConfig(conf config.Section) {
	conf.AddKnownKey(FixedGasPrice)
	conf.AddKnownKey(ResubmitInterval, defaultResubmitInterval)

	conf.AddKnownKey(MaxInFlight, defaultMaxInFlight)
	conf.AddKnownKey(Interval, defaultInterval)
	conf.AddKnownKey(RetryInitDelay, defaultRetryInitDelay)
	conf.AddKnownKey(RetryMaxDelay, defaultRetryMaxDelay)
	conf.AddKnownKey(RetryFactor, defaultRetryFactor)

	gasOracleConfig := conf.SubSection(GasOracleConfig)
	ffresty.InitConfig(gasOracleConfig)
	gasOracleConfig.AddKnownKey(GasOracleMethod, defaultGasOracleMethod)
	gasOracleConfig.AddKnownKey(GasOracleMode, defaultGasOracleMode)
	gasOracleConfig.AddKnownKey(GasOracleQueryInterval, defaultGasOracleQueryInterval)
	gasOracleConfig.AddKnownKey(GasOracleTemplate)

	// Init the deprecated policy engine config in case people are still using them
	legacyConfig := tmconfig.DeprecatedPolicyEngineBaseConfig.SubSection(f.Name())
	legacyConfig.AddKnownKey(FixedGasPrice)
	legacyConfig.AddKnownKey(ResubmitInterval, defaultResubmitInterval)

	legacyGasOracleConfig := legacyConfig.SubSection(GasOracleConfig)
	ffresty.InitConfig(legacyGasOracleConfig)
	legacyGasOracleConfig.AddKnownKey(GasOracleMethod, defaultGasOracleMethod)
	legacyGasOracleConfig.AddKnownKey(GasOracleMode, defaultGasOracleMode)
	legacyGasOracleConfig.AddKnownKey(GasOracleQueryInterval, defaultGasOracleQueryInterval)
	legacyGasOracleConfig.AddKnownKey(GasOracleTemplate)
}
