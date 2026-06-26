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
	"github.com/hyperledger-firefly/transaction-manager/internal/apiclient"
	"github.com/spf13/cobra"
)

var eventStreamID string

func clientEventStreamsCommand(clientFactory func() (apiclient.FFTMClient, error)) *cobra.Command {
	clientEventStreamsCmd := &cobra.Command{
		Use:   "eventstreams <subcommand>",
		Short: "Make API requests to an blockchain connector instance",
	}
	clientEventStreamsCmd.AddCommand(clientEventStreamsListCommand(clientFactory))
	clientEventStreamsCmd.AddCommand(clientEventStreamsDeleteCommand(clientFactory))
	return clientEventStreamsCmd
}
