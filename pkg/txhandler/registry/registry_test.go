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

package txhandlerfactory

import (
	"context"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler/simple"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestRegistryWithDeprecatedConfig(t *testing.T) {
	tmconfig.Reset()
	viper.SetDefault(string(tmconfig.DeprecatedPolicyEngineName), "simple")
	RegisterHandler(&simple.TransactionHandlerFactory{})
	tmconfig.DeprecatedPolicyEngineBaseConfig.SubSection("simple").Set(simple.FixedGasPrice, "12345")
	p, err := NewTransactionHandler(context.Background(), tmconfig.DeprecatedPolicyEngineBaseConfig, "simple")
	assert.NotNil(t, p)
	assert.NoError(t, err)

	p, err = NewTransactionHandler(context.Background(), tmconfig.DeprecatedPolicyEngineBaseConfig, "bob")
	assert.Nil(t, p)
	assert.Regexp(t, "FF21070", err)

}

func TestRegistry(t *testing.T) {
	tmconfig.Reset()
	RegisterHandler(&simple.TransactionHandlerFactory{})

	tmconfig.TransactionHandlerBaseConfig.SubSection("simple").Set(simple.FixedGasPrice, "12345")
	p, err := NewTransactionHandler(context.Background(), tmconfig.TransactionHandlerBaseConfig, "simple")
	assert.NotNil(t, p)
	assert.NoError(t, err)

	p, err = NewTransactionHandler(context.Background(), tmconfig.TransactionHandlerBaseConfig, "bob")
	assert.Nil(t, p)
	assert.Regexp(t, "FF21070", err)

}
