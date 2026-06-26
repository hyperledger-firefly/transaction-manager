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

package cmd

import (
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/stretchr/testify/assert"
)

func TestMigrateCommandFailInit(t *testing.T) {
	cmd := MigrateCommand(func() error {
		return fmt.Errorf("pop")
	})
	cmd.SetArgs([]string{"leveldb2postgres"})
	err := cmd.Execute()
	assert.Regexp(t, "pop", err)
}

func TestMigrateCommandFailRun(t *testing.T) {
	cmd := MigrateCommand(func() error {
		tmconfig.Reset()
		return nil
	})
	cmd.SetArgs([]string{"leveldb2postgres"})
	err := cmd.Execute()
	assert.Regexp(t, "FF21050", err)
}
