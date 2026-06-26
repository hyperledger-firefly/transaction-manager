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
	"fmt"
	"strings"

	"github.com/hyperledger-firefly/transaction-manager/internal/apiclient"
	"github.com/spf13/cobra"
)

func clientListenersDeleteCommand(clientFactory func() (apiclient.FFTMClient, error)) *cobra.Command {
	clientListenersDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete event streams",
		Long:  "",
		RunE: func(_ *cobra.Command, _ []string) error {
			client, err := clientFactory()
			if err != nil {
				return err
			}
			if eventStreamID == "" {
				return fmt.Errorf("eventstream flag not set")
			}
			if listenerID == "" && nameRegex == "" {
				return fmt.Errorf("listener or name flag must be set")
			}
			if listenerID != "" && nameRegex != "" {
				return fmt.Errorf("listener and name flags cannot be combined")
			}
			if listenerID != "" {
				err := client.DeleteListener(context.Background(), eventStreamID, listenerID)
				if err != nil {
					if !(strings.Contains(err.Error(), "FF21046") && ignoreNotFound) {
						return err
					}
				}
			}
			if nameRegex != "" {
				err := client.DeleteListenersByName(context.Background(), eventStreamID, nameRegex)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	clientListenersDeleteCmd.Flags().StringVarP(&listenerID, "listener", "", "", "The ID of the listener")
	clientListenersDeleteCmd.Flags().StringVarP(&nameRegex, "name", "", "", "A regular expression for matching the listener name")
	return clientListenersDeleteCmd
}
