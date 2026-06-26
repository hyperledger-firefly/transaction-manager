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

func clientEventStreamsDeleteCommand(clientFactory func() (apiclient.FFTMClient, error)) *cobra.Command {
	clientEventStreamsDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete event streams",
		Long:  "",
		RunE: func(_ *cobra.Command, _ []string) error {
			client, err := clientFactory()
			if err != nil {
				return err
			}
			if eventStreamID == "" && nameRegex == "" {
				return fmt.Errorf("eventstream or name flag must be set")
			}
			if eventStreamID != "" && nameRegex != "" {
				return fmt.Errorf("eventstream and name flags cannot be combined")
			}
			if eventStreamID != "" {
				err := client.DeleteEventStream(context.Background(), eventStreamID)
				if err != nil {
					if !(strings.Contains(err.Error(), "FF21046") && ignoreNotFound) {
						return err
					}
				}
			}
			if nameRegex != "" {
				err := client.DeleteEventStreamsByName(context.Background(), nameRegex)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	clientEventStreamsDeleteCmd.Flags().StringVarP(&eventStreamID, "eventstream", "", "", "The ID of the event stream")
	clientEventStreamsDeleteCmd.Flags().StringVarP(&nameRegex, "name", "", "", "A regular expression for matching the event stream name")
	return clientEventStreamsDeleteCmd
}
