// Copyright © 2024 Kaleido, Inc.
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

package fftm

import (
	"context"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/internal/tmmsgs"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/sirupsen/logrus"
)

var postRootCommand = func(m *manager) *ffapi.Route {
	return &ffapi.Route{
		Name:           "postRootCommand",
		Path:           "/",
		Method:         http.MethodPost,
		PathParams:     nil,
		QueryParams:    nil,
		Description:    tmmsgs.APIEndpointPostRoot,
		JSONInputValue: func() interface{} { return &apitypes.BaseRequest{} },
		JSONInputSchema: func(_ context.Context, schemaGen ffapi.SchemaGenerator) (*openapi3.SchemaRef, error) {
			schemas := []*openapi3.SchemaRef{}
			txRequest, err := schemaGen(&apitypes.TransactionRequest{})
			if err == nil {
				schemas = append(schemas, txRequest)
			}
			deployRequest, err := schemaGen(&apitypes.ContractDeployRequest{})
			if err == nil {
				schemas = append(schemas, deployRequest)
			}
			queryRequest, err := schemaGen(&apitypes.QueryRequest{})
			if err == nil {
				schemas = append(schemas, queryRequest)
			}
			return &openapi3.SchemaRef{
				Value: &openapi3.Schema{
					AnyOf: schemas,
				},
			}, err
		},
		JSONOutputSchema: func(ctx context.Context, schemaGen ffapi.SchemaGenerator) (*openapi3.SchemaRef, error) {
			managedTX, _ := schemaGen(&apitypes.QueryRequest{})
			return &openapi3.SchemaRef{
				Value: &openapi3.Schema{
					AnyOf: openapi3.SchemaRefs{
						&openapi3.SchemaRef{
							Value: &openapi3.Schema{
								Description: i18n.Expand(ctx, tmmsgs.APIEndpointDeleteEventStream),
							},
						},
						managedTX,
					},
				},
			}, nil
		},
		JSONOutputCodes: []int{http.StatusAccepted},
		JSONHandler: func(r *ffapi.APIRequest) (output interface{}, err error) {
			baseReq := r.Input.(*apitypes.BaseRequest)
			switch baseReq.Headers.Type {
			case apitypes.RequestTypeSendTransaction:
				// We have to supply an extra submissionRejected boolean on submission errors
				return mapSubmissionErrors(r, func() (output interface{}, submissionRejected bool, err error) {
					var tReq apitypes.TransactionRequest
					if err = baseReq.UnmarshalTo(&tReq); err != nil {
						return nil, true /* reject */, i18n.NewError(r.Req.Context(), tmmsgs.MsgInvalidRequestErr, baseReq.Headers.Type, err)
					}
					return m.txHandler.HandleNewTransaction(r.Req.Context(), &tReq)
				}), nil
			case apitypes.RequestTypeDeploy:
				// We have to supply an extra submissionRejected boolean on submission errors
				return mapSubmissionErrors(r, func() (output interface{}, submissionRejected bool, err error) {
					var tReq apitypes.ContractDeployRequest
					if err = baseReq.UnmarshalTo(&tReq); err != nil {
						return nil, true /* reject */, i18n.NewError(r.Req.Context(), tmmsgs.MsgInvalidRequestErr, baseReq.Headers.Type, err)
					}
					return m.txHandler.HandleNewContractDeployment(r.Req.Context(), &tReq)
				}), nil
			case apitypes.RequestTypeQuery:
				var tReq apitypes.QueryRequest
				if err = baseReq.UnmarshalTo(&tReq); err != nil {
					return nil, i18n.NewError(r.Req.Context(), tmmsgs.MsgInvalidRequestErr, baseReq.Headers.Type, err)
				}
				res, _, err := m.connector.QueryInvoke(r.Req.Context(), &ffcapi.QueryInvokeRequest{
					TransactionInput: tReq.TransactionInput,
					BlockNumber:      tReq.BlockNumber,
				})
				if err != nil {
					return nil, err
				}
				return res.Outputs, nil
			case apitypes.RequestTypeTransactionReceipt:
				var tReq apitypes.TransactionReceiptRequest
				if err = baseReq.UnmarshalTo(&tReq); err != nil {
					return nil, i18n.NewError(r.Req.Context(), tmmsgs.MsgInvalidRequestErr, baseReq.Headers.Type, err)
				}
				res, _, err := m.connector.TransactionReceipt(r.Req.Context(), &tReq.TransactionReceiptRequest)
				if err != nil {
					return nil, err
				}
				apiRes := &apitypes.TransactionReceiptResponse{
					TransactionReceiptResponseBase: res.TransactionReceiptResponseBase,
				}
				// Ugly necessity to work around the complex serialization interface in EventWithContext without
				// moving or duplicating the code
				for _, e := range res.Events {
					apiRes.Events = append(apiRes.Events, &apitypes.EventWithContext{
						Event: e,
					})
				}
				return apiRes, nil
			default:
				return nil, i18n.NewError(r.Req.Context(), tmmsgs.MsgUnsupportedRequestType, baseReq.Headers.Type)
			}
		},
	}
}

// Common function to help us supply an extra submissionRejected boolean on submission errors.
func mapSubmissionErrors(r *ffapi.APIRequest, handler func() (output interface{}, submissionRejected bool, err error)) interface{} {
	output, submissionRejected, err := handler()
	if err == nil {
		return output
	}
	l := log.L(r.Req.Context())
	status := 500
	if ffe, ok := (interface{}(err)).(i18n.FFError); ok {
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			l.Debugf("%s:\n%s", ffe.Error(), ffe.StackTrace())
		}
		status = ffe.HTTPStatus()
	}
	r.SuccessStatus = status
	l.Errorf("Submission failed (submissionRejected=%t) [%d]: %s", submissionRejected, status, err)
	return &ffcapi.SubmissionError{
		Error:              err.Error(),
		SubmissionRejected: submissionRejected,
	}
}
