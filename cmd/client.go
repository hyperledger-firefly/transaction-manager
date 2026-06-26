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
	"context"
	"fmt"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/httpserver"
	"github.com/hyperledger-firefly/transaction-manager/internal/apiclient"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/spf13/cobra"
)

var url string
var nameRegex string
var ignoreNotFound bool

var tlsEnabled bool
var caFile string
var certFile string
var keyFile string

func ClientCommand() *cobra.Command {
	return buildClientCommand(createClient)
}

func buildClientCommand(clientFactory func() (apiclient.FFTMClient, error)) *cobra.Command {
	clientCmd := &cobra.Command{
		Use:   "client <subcommand>",
		Short: "Make API requests to a blockchain connector instance",
	}
	defaultURL := fmt.Sprintf("http://%s:%s", tmconfig.APIConfig.GetString(httpserver.HTTPConfAddress), tmconfig.APIConfig.GetString(httpserver.HTTPConfPort))

	clientCmd.PersistentFlags().BoolVarP(&ignoreNotFound, "ignore-not-found", "", false, "Does not return an error if the resource is not found. Useful for idempotent delete functions.")
	clientCmd.PersistentFlags().StringVarP(&url, "url", "", defaultURL, "The URL of the blockchain connector")

	clientCmd.PersistentFlags().BoolVarP(&tlsEnabled, "tls", "", false, "Enable TLS on client")
	clientCmd.PersistentFlags().StringVarP(&caFile, "cacert", "", "", "The tls CA cert file")
	clientCmd.PersistentFlags().StringVarP(&certFile, "cert", "", "", "The tls cert file")
	clientCmd.PersistentFlags().StringVarP(&keyFile, "key", "", "", "The tls key file")

	clientCmd.AddCommand(clientEventStreamsCommand(clientFactory))
	clientCmd.AddCommand(clientListenersCommand(clientFactory))

	return clientCmd
}

func createClient() (apiclient.FFTMClient, error) {
	cfg := config.RootSection("fftm_client")
	apiclient.InitConfig(cfg)
	if url != "" {
		cfg.Set("url", url)
	}
	if tlsEnabled {
		tlsConf := cfg.SubSection("tls")
		tlsConf.Set(fftls.HTTPConfTLSEnabled, true)
		if caFile != "" {
			tlsConf.Set(fftls.HTTPConfTLSCAFile, caFile)
		}
		if certFile != "" {
			tlsConf.Set(fftls.HTTPConfTLSCertFile, certFile)
		}
		if keyFile != "" {
			tlsConf.Set(fftls.HTTPConfTLSKeyFile, keyFile)
		}
	}
	return apiclient.NewFFTMClient(context.Background(), cfg)
}

func init() {
	tmconfig.Reset()
}
