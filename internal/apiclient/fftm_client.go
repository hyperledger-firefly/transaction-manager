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

package apiclient

import (
	"context"

	resty "github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

type FFTMClient interface {
	GetEventStreams(ctx context.Context) ([]apitypes.EventStream, error)
	GetListeners(ctx context.Context, eventStreamID string) ([]apitypes.Listener, error)
	DeleteEventStream(ctx context.Context, eventStreamID string) error
	DeleteEventStreamsByName(ctx context.Context, nameRegex string) error
	DeleteListener(ctx context.Context, eventStreamID, listenerID string) error
	DeleteListenersByName(ctx context.Context, eventStreamID, nameRegex string) error
}

type fftmClient struct {
	client *resty.Client
}

func InitConfig(conf config.Section) {
	ffresty.InitConfig(conf)
}

func NewFFTMClient(ctx context.Context, staticConfig config.Section) (FFTMClient, error) {
	client, err := ffresty.New(ctx, staticConfig)
	if err != nil {
		return nil, err
	}
	return &fftmClient{
		client: client,
	}, nil
}
