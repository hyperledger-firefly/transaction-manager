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

package events

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/url"
	"time"

	resty "github.com/go-resty/resty/v2"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmconfig"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
)

func mergeValidateWhConfig(ctx context.Context, changed bool, base *apitypes.WebhookConfig, updates *apitypes.WebhookConfig) (*apitypes.WebhookConfig, bool, error) {

	if base == nil {
		base = &apitypes.WebhookConfig{}
	}
	if updates == nil {
		updates = &apitypes.WebhookConfig{}
	}
	merged := &apitypes.WebhookConfig{}

	// URL (no default - must be set)
	changed = apitypes.CheckUpdateString(changed, &merged.URL, base.URL, updates.URL, "")
	if *merged.URL == "" {
		return nil, false, i18n.NewError(ctx, tmmsgs.MsgMissingWebhookURL)
	}

	// Headers
	changed = apitypes.CheckUpdateStringMap(changed, &merged.Headers, base.Headers, updates.Headers)

	// Skip host verify (disable TLS checking)
	changed = apitypes.CheckUpdateBool(changed, &merged.TLSkipHostVerify, base.TLSkipHostVerify, updates.TLSkipHostVerify, false)

	// Request timeout
	if updates.EthCompatRequestTimeoutSec != nil {
		dv := fftypes.FFDuration(*updates.EthCompatRequestTimeoutSec) * fftypes.FFDuration(time.Second)
		changed = apitypes.CheckUpdateDuration(changed, &merged.RequestTimeout, base.RequestTimeout, &dv, esDefaults.webhookRequestTimeout)
	} else {
		changed = apitypes.CheckUpdateDuration(changed, &merged.RequestTimeout, base.RequestTimeout, updates.RequestTimeout, esDefaults.webhookRequestTimeout)
	}

	return merged, changed, nil
}

type webhookAction struct {
	allowPrivateIPs bool
	spec            *apitypes.WebhookConfig
	client          *resty.Client
}

func newWebhookAction(bgCtx context.Context, spec *apitypes.WebhookConfig) (*webhookAction, error) {
	client, err := ffresty.New(bgCtx, tmconfig.WebhookPrefix) // majority of settings come from config
	if err != nil {
		return nil, err
	}
	client.SetTimeout(time.Duration(*spec.RequestTimeout)) // request timeout set per stream
	if *spec.TLSkipHostVerify {
		client.SetTLSClientConfig(&tls.Config{
			InsecureSkipVerify: true,
		})
	}

	return &webhookAction{
		spec:            spec,
		allowPrivateIPs: config.GetBool(tmconfig.WebhooksAllowPrivateIPs),
		client:          client,
	}, nil
}

// attemptWebhookAction performs a single attempt of a webhook action
func (w *webhookAction) attemptBatch(ctx context.Context, batchNumber int64, attempt int, events []*apitypes.EventWithContext) error {
	// We perform DNS resolution before each attempt, to exclude private IP address ranges from the target
	u, _ := url.Parse(*w.spec.URL)
	addr, err := net.ResolveIPAddr("ip4", u.Hostname())
	if err != nil {
		return i18n.NewError(ctx, tmmsgs.MsgInvalidHost, u.Hostname())
	}
	if w.isAddressBlocked(addr) {
		return i18n.NewError(ctx, tmmsgs.MsgBlockWebhookAddress, addr, u.Hostname())
	}
	req := w.client.R().
		SetContext(ctx).
		SetBody(events).
		SetDoNotParseResponse(true)
	req.Header.Set("Content-Type", "application/json")
	for h, v := range w.spec.Headers {
		req.Header.Set(h, v)
	}
	res, err := req.Post(u.String())
	if err != nil {
		log.L(ctx).Errorf("Webhook %s (%s) batch=%d attempt=%d: %s", *w.spec.URL, u, batchNumber, attempt, err)
		return i18n.NewError(ctx, tmmsgs.MsgWebhookErr, err)
	}
	defer res.RawBody().Close()
	if res.IsError() {
		resBody, _ := io.ReadAll(res.RawBody())
		log.L(ctx).Errorf("Webhook %s (%s) [%d] batch=%d attempt=%d: %s", *w.spec.URL, u, res.StatusCode(), batchNumber, attempt, resBody)
		err = i18n.NewError(ctx, tmmsgs.MsgWebhookFailedStatus, res.StatusCode())
	}
	return err
}

// isAddressBlocked allows blocking of all of the "private" address blocks defined by IPv4
func (w *webhookAction) isAddressBlocked(ip *net.IPAddr) bool {
	ip4 := ip.IP.To4()
	return !w.allowPrivateIPs &&
		(ip4[0] == 0 ||
			ip4[0] >= 224 ||
			ip4[0] == 127 ||
			ip4[0] == 10 ||
			(ip4[0] == 172 && ip4[1] >= 16 && ip4[1] < 32) ||
			(ip4[0] == 192 && ip4[1] == 168))
}
