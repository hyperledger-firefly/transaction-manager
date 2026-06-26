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

package apiclient

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func (c *fftmClient) GetEventStreams(ctx context.Context) ([]apitypes.EventStream, error) {
	eventStreams := []apitypes.EventStream{}
	resp, err := c.client.R().
		SetContext(ctx).
		SetResult(&eventStreams).
		Get("eventstreams")
	if !resp.IsSuccess() {
		return nil, errors.New(string(resp.Body()))
	}
	return eventStreams, err
}

func (c *fftmClient) GetListeners(ctx context.Context, eventStreamID string) ([]apitypes.Listener, error) {
	listeners := []apitypes.Listener{}
	resp, err := c.client.R().
		SetContext(ctx).
		SetResult(&listeners).
		Get(fmt.Sprintf("eventstreams/%s/listeners", eventStreamID))
	if !resp.IsSuccess() {
		return nil, errors.New(string(resp.Body()))
	}
	return listeners, err
}

func (c *fftmClient) DeleteEventStream(ctx context.Context, eventStreamID string) error {
	resp, err := c.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("eventstreams/%s", eventStreamID))
	if err != nil {
		return err
	}
	if !resp.IsSuccess() {
		return errors.New(string(resp.Body()))
	}
	return nil
}

func (c *fftmClient) DeleteEventStreamsByName(ctx context.Context, nameRegex string) error {
	regex, err := regexp.Compile(nameRegex)
	if err != nil {
		return err
	}

	eventStreams, err := c.GetEventStreams(ctx)
	if err != nil {
		return err
	}

	for _, eventStream := range eventStreams {
		if regex.MatchString(*eventStream.Name) {
			err := c.DeleteEventStream(ctx, eventStream.ID.String())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *fftmClient) DeleteListener(ctx context.Context, eventStreamID, listenerID string) error {
	resp, err := c.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("eventstreams/%s/listeners/%s", eventStreamID, listenerID))
	if err != nil {
		return err
	}
	if !resp.IsSuccess() {
		return errors.New(string(resp.Body()))
	}
	return nil
}

func (c *fftmClient) DeleteListenersByName(ctx context.Context, eventStreamID, nameRegex string) error {
	regex, err := regexp.Compile(nameRegex)
	if err != nil {
		return err
	}

	listeners, err := c.GetListeners(ctx, eventStreamID)
	if err != nil {
		return err
	}

	for _, listener := range listeners {
		if regex.MatchString(*listener.Name) {
			err := c.DeleteListener(ctx, eventStreamID, listener.ID.String())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
