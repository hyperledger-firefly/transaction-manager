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

package txhandlerfactory

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/txhandler"
)

var txHandlers = make(map[string]Factory)

func NewTransactionHandler(ctx context.Context, baseConfig config.Section, name string) (txhandler.TransactionHandler, error) {
	factory, ok := txHandlers[name]
	if !ok {
		return nil, i18n.NewError(ctx, tmmsgs.MsgTransactionHandlerNotRegistered, name)
	}
	return factory.NewTransactionHandler(ctx, baseConfig.SubSection(name))
}

type Factory interface {
	Name() string
	InitConfig(conf config.Section)
	NewTransactionHandler(ctx context.Context, conf config.Section) (txhandler.TransactionHandler, error)
}

func RegisterHandler(factory Factory) string {
	name := factory.Name()
	txHandlers[name] = factory
	// init the new transaction handler configurations
	factory.InitConfig(tmconfig.TransactionHandlerBaseConfig.SubSection(name))
	return name
}
