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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hyperledger-firefly/transaction-manager/internal/apiclient"
	"github.com/spf13/cobra"
)

func clientListenersListCommand(clientFactory func() (apiclient.FFTMClient, error)) *cobra.Command {
	clientListenersListCmd := &cobra.Command{
		Use:   "list",
		Short: "List listeners",
		Long:  "",
		RunE: func(_ *cobra.Command, _ []string) error {
			client, err := clientFactory()
			if err != nil {
				return err
			}
			if eventStreamID == "" {
				return fmt.Errorf("eventstream flag not set")
			}
			listeners, err := client.GetListeners(context.Background(), eventStreamID)
			if err != nil {
				return err
			}
			json, _ := json.MarshalIndent(listeners, "", "  ")
			fmt.Println(string(json))
			return nil
		},
	}
	return clientListenersListCmd
}
