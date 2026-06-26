// Copyright © 2026 Kaleido, Inc.
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

package simple

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/transaction-manager/pkg/apitypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// HandleSubmissions handles a batch of new submission requests (transactions and contract deployments).
// Submissions are prepared first, then inserted in batch using InsertTransactionsWithNextNonce for efficient nonce allocation.
// Each submission in the batch is processed independently, and results are returned in arrays matching the input order.
// If a submission fails during preparation, it does not affect the processing of other submissions in the batch.
// The request type (transaction vs deployment) is determined by the presence of Definition or Contract fields.
func (sth *simpleTransactionHandler) HandleSubmissions(ctx context.Context, submissions []*apitypes.SubmissionRequest) []*apitypes.SubmissionResponse {
	log.L(ctx).Debugf("HandleSubmissions processing batch of %d submission requests", len(submissions))

	// Process batch using the generic handleBatch function
	mtxs, submissionRejected, errs := sth.handleBatch(ctx, submissions)

	// Convert results to SubmissionResponse format
	responses := make([]*apitypes.SubmissionResponse, len(submissions))
	for i := range submissions {
		responses[i] = &apitypes.SubmissionResponse{
			ID:      submissions[i].ID,
			Success: mtxs[i] != nil && errs[i] == nil,
		}

		if mtxs[i] != nil {
			responses[i].Output = mtxs[i]
		}

		if errs[i] != nil {
			responses[i].Error = &ffcapi.SubmissionError{
				Error:              errs[i].Error(),
				SubmissionRejected: submissionRejected[i],
			}
		}
	}

	return responses
}

// handleBatch is a batch processing function that handles the common logic for submissions.
func (sth *simpleTransactionHandler) handleBatch(
	ctx context.Context,
	submissions []*apitypes.SubmissionRequest,
) (mtxs []*apitypes.ManagedTX, submissionRejected []bool, errs []error) {

	totalCount := len(submissions)

	// Initialize result arrays with the same length as input
	mtxs = make([]*apitypes.ManagedTX, totalCount)
	submissionRejected = make([]bool, totalCount)
	errs = make([]error, totalCount)

	// Prepare all items first
	preparedTxs := make([]*apitypes.ManagedTX, 0, totalCount)
	preparedIndices := make([]int, 0, totalCount)

	for i := 0; i < totalCount; i++ {
		submission := submissions[i]
		if submission == nil {
			continue
		}

		itemID := submission.ID
		if itemID == "" {
			itemID = fftypes.NewUUID().String()
			submission.ID = itemID
		}

		log.L(ctx).Tracef("HandleSubmissions preparing submission %d/%d with ID %s", i+1, totalCount, itemID)

		// Prepare the item - determine if this is a deployment request based on presence of Definition or Contract
		var preparedMtx *apitypes.ManagedTX
		var rejected bool
		var err error

		isDeploy := submission.Definition != nil || submission.Contract != nil
		if isDeploy {
			// Convert to ContractDeployRequest and prepare
			deployReq := &apitypes.ContractDeployRequest{
				Headers: apitypes.RequestHeaders{
					ID:   submission.ID,
					Type: apitypes.RequestTypeDeploy,
				},
				ContractDeployPrepareRequest: ffcapi.ContractDeployPrepareRequest{
					TransactionHeaders: submission.TransactionHeaders,
					Definition:         submission.Definition,
					Contract:           submission.Contract,
					Params:             submission.Params,
					Errors:             submission.Errors,
				},
			}
			preparedMtx, rejected, err = sth.prepareContractDeployment(ctx, deployReq)
		} else {
			// Convert to TransactionRequest and prepare
			txReq := &apitypes.TransactionRequest{
				Headers: apitypes.RequestHeaders{
					ID:   submission.ID,
					Type: apitypes.RequestTypeSendTransaction,
				},
				TransactionInput: ffcapi.TransactionInput{
					TransactionHeaders: submission.TransactionHeaders,
					Method:             submission.Method,
					Params:             submission.Params,
					Errors:             submission.Errors,
				},
			}
			preparedMtx, rejected, err = sth.prepareTransaction(ctx, txReq)
		}

		if rejected || err != nil {
			log.L(ctx).Errorf("HandleSubmissions failed to prepare submission %d/%d with ID %s: %+v", i+1, totalCount, itemID, err)
			errs[i] = err
			submissionRejected[i] = rejected
			continue
		}

		// Store prepared transaction for batch insertion
		preparedTxs = append(preparedTxs, preparedMtx)
		preparedIndices = append(preparedIndices, i)
	}

	// Batch insert all prepared transactions using InsertTransactionsWithNextNonce
	// This optimizes nonce allocation for transactions from the same signer and batches database operations
	if len(preparedTxs) > 0 {
		log.L(ctx).Tracef("HandleSubmissions batch inserting %d prepared submissions", len(preparedTxs))
		insertErrs := sth.toolkit.TXPersistence.InsertTransactionsWithNextNonce(ctx, preparedTxs, func(ctx context.Context, signer string) (uint64, error) {
			nextNonceRes, _, err := sth.toolkit.Connector.NextNonceForSigner(ctx, &ffcapi.NextNonceForSignerRequest{
				Signer: signer,
			})
			if err != nil {
				return 0, err
			}
			return nextNonceRes.Nonce.Uint64(), nil
		})

		// Process insertion results and add history entries for successful transactions
		sth.processBatchInsertionResults(ctx, preparedTxs, preparedIndices, insertErrs, totalCount, mtxs, errs)

		// Mark inflight stale once after batch insertion
		sth.markInflightStale()
	}

	// Log summary of batch processing results
	sth.logBatchProcessingSummary(ctx, totalCount, errs, submissionRejected)

	return mtxs, submissionRejected, errs
}

// processBatchInsertionResults processes the results of a batch insertion operation.
// It adds history entries for successful transactions and updates the result arrays.
func (sth *simpleTransactionHandler) processBatchInsertionResults(ctx context.Context, preparedTxs []*apitypes.ManagedTX, preparedIndices []int, insertErrs []error, totalCount int, mtxs []*apitypes.ManagedTX, errs []error) {

	for j, preparedIdx := range preparedIndices {
		if insertErrs[j] != nil {
			log.L(ctx).Errorf("HandleSubmissions failed to insert transaction %d/%d with ID %s: %+v", preparedIdx+1, totalCount, preparedTxs[j].ID, insertErrs[j])
			errs[preparedIdx] = insertErrs[j]
			continue
		}

		// Transaction successfully inserted, add history entry
		log.L(ctx).Tracef("HandleSubmissions persisted transaction with ID: %s, using nonce %s", preparedTxs[j].ID, preparedTxs[j].Nonce.String())
		err := sth.toolkit.TXHistory.AddSubStatusAction(ctx, preparedTxs[j].ID, apitypes.TxSubStatusReceived, apitypes.TxActionAssignNonce, fftypes.JSONAnyPtr(`{"nonce":"`+preparedTxs[j].Nonce.String()+`"}`), nil, fftypes.Now())
		if err != nil {
			log.L(ctx).Errorf("HandleSubmissions failed to add history entry for transaction %s: %+v", preparedTxs[j].ID, err)
			errs[preparedIdx] = err
			continue
		}

		mtxs[preparedIdx] = preparedTxs[j]
		log.L(ctx).Tracef("HandleSubmissions successfully processed transaction request %d/%d with ID %s", preparedIdx+1, totalCount, preparedTxs[j].ID)
	}
}

// logBatchProcessingSummary logs a summary of batch processing results.
func (sth *simpleTransactionHandler) logBatchProcessingSummary(ctx context.Context, totalCount int, errs []error, submissionRejected []bool) {

	successCount := 0
	rejectedCount := 0
	errorCount := 0
	for i := range errs {
		switch {
		case errs[i] != nil:
			errorCount++
		case submissionRejected[i]:
			rejectedCount++
		default:
			successCount++
		}
	}
	log.L(ctx).Debugf("HandleSubmissions batch processing complete: %d succeeded, %d rejected, %d errors out of %d total requests", successCount, rejectedCount, errorCount, totalCount)
}
