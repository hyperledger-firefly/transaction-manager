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

package tmmsgs

import (
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"golang.org/x/text/language"
)

var ffc = func(key, translation, fieldType string) i18n.ConfigMessageKey {
	return i18n.FFC(language.AmericanEnglish, key, translation, fieldType)
}

//revive:disable
var (
	ConfigAPIDefaultRequestTimeout = ffc("config.api.defaultRequestTimeout", "Default server-side request timeout for API calls", i18n.TimeDurationType)
	ConfigAPIMaxRequestTimeout     = ffc("config.api.maxRequestTimeout", "Maximum server-side request timeout a caller can request with a Request-Timeout header", i18n.TimeDurationType)
	ConfigAPIAddress               = ffc("config.api.address", "Listener address for API", i18n.StringType)
	ConfigAPIPort                  = ffc("config.api.port", "Listener port for API", i18n.IntType)
	ConfigAPIPublicURL             = ffc("config.api.publicURL", "External address callers should access API over", i18n.StringType)
	ConfigAPIReadTimeout           = ffc("config.api.readTimeout", "The maximum time to wait when reading from an HTTP connection", i18n.TimeDurationType)
	ConfigAPIWriteTimeout          = ffc("config.api.writeTimeout", "The maximum time to wait when writing to a HTTP connection", i18n.TimeDurationType)
	ConfigAPIShutdownTimeout       = ffc("config.api.shutdownTimeout", "The maximum amount of time to wait for any open HTTP requests to finish before shutting down the HTTP server", i18n.TimeDurationType)
	ConfigAPIPassthroughHeaders    = ffc("config.api.passthroughHeaders", "A list of HTTP request headers to pass through to dependency microservices", i18n.ArrayStringType)
	ConfigAPISimpleQuery           = ffc("config.api.simpleQuery", "Force use of original limited API query syntax, even if rich query is supported in the database", i18n.BooleanType)

	ConfigDebugPort = ffc("config.debug.port", "An HTTP port on which to enable the go debugger", i18n.IntType)

	ConfigConfirmationsBlockCacheSize           = ffc("config.confirmations.blockCacheSize", "The maximum number of block headers to keep in the cache", i18n.IntType)
	ConfigConfirmationsBlockQueueLength         = ffc("config.confirmations.blockQueueLength", "Internal queue length for notifying the confirmations manager of new blocks", i18n.IntType)
	ConfigConfirmationsNotificationsQueueLength = ffc("config.confirmations.notificationQueueLength", "Internal queue length for notifying the confirmations manager of new transactions/events", i18n.IntType)
	ConfigConfirmationsRequired                 = ffc("config.confirmations.required", "Number of confirmations required to consider a transaction/event final", i18n.IntType)
	ConfigConfirmationsStaleReceiptTimeout      = ffc("config.confirmations.staleReceiptTimeout", "Duration after which to force a receipt check for a pending transaction", i18n.TimeDurationType)
	ConfigConfirmationsFetchReceiptUponEntry    = ffc("config.confirmations.fetchReceiptUponEntry", "Fetch receipt of new transactions immediately when they are added to the internal queue. When set to false, fetch will only happen when a new block is received or the transaction has been queue for more than the stale receipt timeout", i18n.BooleanType)
	ConfigConfirmationsReceiptWorkers           = ffc("config.confirmations.receiptWorkers", "Number of workers to use to query in parallel for receipts", i18n.IntType)

	ConfigTransactionsNonceStateTimeout = ffc("config.transactions.nonceStateTimeout", "How old the most recently submitted transaction record in our local state needs to be, before we make a request to the node to query the next nonce for a signing address", i18n.TimeDurationType)
	ConfigTransactionsMaxHistoryCount   = ffc("config.transactions.maxHistoryCount", "The number of historical status updates to retain in the operation", i18n.IntType)

	DeprecatedConfigTransactionsMaxInflight                  = ffc("config.transactions.maxInFlight", "Deprecated: Please use 'transactions.handler.simple.maxInFlight' instead", i18n.IntType)
	DeprecatedConfigPolicyEngineName                         = ffc("config.policyengine.name", "Deprecated: Please use 'transactions.handler.name' instead", i18n.StringType)
	DeprecatedConfigLoopInterval                             = ffc("config.policyloop.interval", "Deprecated: Please use 'transactions.handler.simple.interval' instead", i18n.TimeDurationType)
	DeprecatedConfigPolicyEngineSimpleFixedGasPrice          = ffc("config.policyengine.simple.fixedGasPrice", "Deprecated: Please use 'transactions.handler.simple.fixedGasPrice' instead", "Raw JSON")
	DeprecatedConfigPolicyEngineSimpleResubmitInterval       = ffc("config.policyengine.simple.resubmitInterval", "Deprecated: Please use 'transactions.handler.simple.resubmitInterval' instead", i18n.TimeDurationType)
	DeprecatedConfigPolicyEngineSimpleGasOracleEnabled       = ffc("config.policyengine.simple.gasOracle.mode", "Deprecated: Please use 'transactions.handler.simple.gasOracle.mode' instead", "'connector', 'restapi', 'fixed', or 'disabled'")
	DeprecatedConfigPolicyEngineSimpleGasOracleGoTemplate    = ffc("config.policyengine.simple.gasOracle.template", "Deprecated: Please use 'transactions.handler.simple.gasOracle.template' instead", i18n.GoTemplateType)
	DeprecatedConfigPolicyEngineSimpleGasOracleURL           = ffc("config.policyengine.simple.gasOracle.url", "Deprecated: Please use 'transactions.handler.simple.gasOracle.url' instead", i18n.StringType)
	DeprecatedConfigPolicyEngineSimpleGasOracleProxyURL      = ffc("config.policyengine.simple.gasOracle.proxy.url", "Deprecated: Please use 'transactions.handler.simple.gasOracle.proxy.url' instead", i18n.StringType)
	DeprecatedConfigPolicyEngineSimpleGasOracleMethod        = ffc("config.policyengine.simple.gasOracle.method", "Deprecated: Please use 'transactions.handler.simple.gasOracle.method' instead", i18n.StringType)
	DeprecatedConfigPolicyEngineSimpleGasOracleQueryInterval = ffc("config.policyengine.simple.gasOracle.queryInterval", "Deprecated: Please use 'transactions.handler.simple.gasOracle.queryInterval' instead", i18n.TimeDurationType)
	DeprecatedConfigLoopRetryInitDelay                       = ffc("config.policyloop.retry.initialDelay", "Deprecated: Please use 'transactions.handler.simple.interval' instead", i18n.TimeDurationType)
	DeprecatedConfigLoopRetryMaxDelay                        = ffc("config.policyloop.retry.maxDelay", "Deprecated: Please use 'transactions.handler.simple.interval' instead", i18n.TimeDurationType)
	DeprecatedConfigLoopRetryFactor                          = ffc("config.policyloop.retry.factor", "Deprecated: Please use 'transactions.handler.simple.interval' instead", i18n.TimeDurationType)
	DeprecatedConfigTXHandlerNonceStateTimeout               = ffc("config.transactions.handler.simple.nonceStateTimeout", "Deprecated: Please use 'transactions.handler.simple.nonceStateTimeout' instead", i18n.TimeDurationType)

	ConfigTXHandlerName        = ffc("config.transactions.handler.name", "The name of the transaction handler to use", i18n.StringType)
	ConfigTXHandlerMaxInflight = ffc("config.transactions.handler.simple.maxInFlight", "The maximum number of transactions to have in-flight with the transaction handler / blockchain transaction pool", i18n.IntType)

	ConfigTXHandlerSimpleInterval               = ffc("config.transactions.handler.simple.interval", "Interval at which to invoke the transaction handler loop to evaluate outstanding transactions", i18n.TimeDurationType)
	ConfigTXHandlerSimpleFixedGasPrice          = ffc("config.transactions.handler.simple.fixedGasPrice", "A fixed gasPrice value/structure to pass to the connector", "Raw JSON")
	ConfigTXHandlerSimpleResubmitInterval       = ffc("config.transactions.handler.simple.resubmitInterval", "The time between warning and re-sending a transaction (same nonce) when a blockchain transaction has not been allocated a receipt", i18n.TimeDurationType)
	ConfigTXHandlerSimpleRetryInitDelay         = ffc("config.transactions.handler.simple.retry.initialDelay", "Initial retry delay for retrieving transactions from the persistence", i18n.TimeDurationType)
	ConfigTXHandlerSimpleRetryMaxDelay          = ffc("config.transactions.handler.simple.retry.maxDelay", "Maximum delay between retries for retrieving transactions from the persistence", i18n.TimeDurationType)
	ConfigTXHandlerSimpleRetryFactor            = ffc("config.transactions.handler.simple.retry.factor", "Factor to increase the delay by, between each retry for retrieving transactions from the persistence", i18n.FloatType)
	ConfigTXHandlerSimpleGasOracleEnabled       = ffc("config.transactions.handler.simple.gasOracle.mode", "The gas oracle mode", "'connector', 'restapi', 'fixed', or 'disabled'")
	ConfigTXHandlerSimpleGasOracleGoTemplate    = ffc("config.transactions.handler.simple.gasOracle.template", "REST API Gas Oracle: A go template to execute against the result from the Gas Oracle, to create a JSON block that will be passed as the gas price to the connector", i18n.GoTemplateType)
	ConfigTXHandlerSimpleGasOracleURL           = ffc("config.transactions.handler.simple.gasOracle.url", "REST API Gas Oracle: The URL of a Gas Oracle REST API to call", i18n.StringType)
	ConfigTXHandlerSimpleGasOracleProxyURL      = ffc("config.transactions.handler.simple.gasOracle.proxy.url", "Optional HTTP proxy URL to use for the Gas Oracle REST API", i18n.StringType)
	ConfigPTXHandlerSimpleGasOracleMethod       = ffc("config.transactions.handler.simple.gasOracle.method", "The HTTP Method to use when invoking the Gas Oracle REST API", i18n.StringType)
	ConfigTXHandlerSimpleGasOracleQueryInterval = ffc("config.transactions.handler.simple.gasOracle.queryInterval", "The minimum interval between queries to the Gas Oracle", i18n.TimeDurationType)

	ConfigEventStreamsDefaultsBatchSize                 = ffc("config.eventstreams.defaults.batchSize", "Default batch size for newly created event streams", i18n.IntType)
	ConfigEventStreamsDefaultsBatchTimeout              = ffc("config.eventstreams.defaults.batchTimeout", "Default batch timeout for newly created event streams", i18n.TimeDurationType)
	ConfigEventStreamsDefaultsErrorHandling             = ffc("config.eventstreams.defaults.errorHandling", "Default error handling for newly created event streams", "'skip' or 'block'")
	ConfigEventStreamsDefaultsRetryTimeout              = ffc("config.eventstreams.defaults.retryTimeout", "Default retry timeout for newly created event streams", i18n.TimeDurationType)
	ConfigEventStreamsDefaultsBlockedRetryDelay         = ffc("config.eventstreams.defaults.blockedRetryDelay", "Default blocked retry delay for newly created event streams", i18n.TimeDurationType)
	ConfigEventStreamsDefaultsWebhookRequestTimeout     = ffc("config.eventstreams.defaults.webhookRequestTimeout", "Default WebHook request timeout for newly created event streams", i18n.TimeDurationType)
	ConfigEventStreamsDefaultsWebsocketDistributionMode = ffc("config.eventstreams.defaults.websocketDistributionMode", "Default WebSocket distribution mode for newly created event streams", "'load_balance' or 'broadcast'")
	ConfigEventStreamsCheckpointInterval                = ffc("config.eventstreams.checkpointInterval", "Regular interval to write checkpoints for an event stream listener that is not actively detecting/delivering events", i18n.TimeDurationType)
	ConfigEventStreamsRetryInitDelay                    = ffc("config.eventstreams.retry.initialDelay", "Initial retry delay", i18n.TimeDurationType)
	ConfigEventStreamsRetryMaxDelay                     = ffc("config.eventstreams.retry.maxDelay", "Maximum delay between retries", i18n.TimeDurationType)
	ConfigEventStreamsRetryFactor                       = ffc("config.eventstreams.retry.factor", "Factor to increase the delay by, between each retry", i18n.FloatType)

	ConfigPersistenceType              = ffc("config.persistence.type", "The type of persistence to use", "`leveldb`, `postgres`(supports rich query)")
	ConfigPersistenceLevelDBPath       = ffc("config.persistence.leveldb.path", "The path for the LevelDB persistence directory", i18n.StringType)
	ConfigPersistenceLevelDBMaxHandles = ffc("config.persistence.leveldb.maxHandles", "The maximum number of cached file handles LevelDB should keep open", i18n.IntType)
	ConfigPersistenceLevelDBSyncWrites = ffc("config.persistence.leveldb.syncWrites", "Whether to synchronously perform writes to the storage", i18n.BooleanType)

	ConfigWebhooksAllowPrivateIPs = ffc("config.webhooks.allowPrivateIPs", "Whether to allow WebHook URLs that resolve to Private IP address ranges (vs. internet addresses)", i18n.BooleanType)
	ConfigWebhooksURL             = ffc("config.webhooks.url", "Unused (overridden by the WebHook configuration of an individual event stream)", i18n.IgnoredType)
	ConfigWebhooksProxyURL        = ffc("config.webhooks.proxy.url", "Optional HTTP proxy to use when invoking WebHooks", i18n.StringType)

	ConfigMetricsAddress           = ffc("config.metrics.address", "The IP address on which the metrics HTTP API should listen", i18n.IntType)
	DeprecatedConfigMetricsEnabled = ffc("config.metrics.enabled", "Deprecated: Please use 'monitoring.enabled' instead", i18n.BooleanType)
	DeprecatedConfigMetricsPath    = ffc("config.metrics.path", "Deprecated: Please use 'monitoring.metricsPath' instead", i18n.StringType)
	ConfigMonitoringEnabled        = ffc("config.monitoring.enabled", "Enables the monitoring APIs", i18n.BooleanType)
	ConfigMonitoringMetricsPath    = ffc("config.monitoring.metricsPath", "The path from which to serve the Prometheus metrics", i18n.StringType)
	ConfigMetricsPort              = ffc("config.metrics.port", "The port on which the metrics HTTP API should listen", i18n.IntType)
	ConfigMetricsPublicURL         = ffc("config.metrics.publicURL", "The fully qualified public URL for the metrics API. This is used for building URLs in HTTP responses and in OpenAPI Spec generation", "URL "+i18n.StringType)
	ConfigMetricsReadTimeout       = ffc("config.metrics.readTimeout", "The maximum time to wait when reading from an HTTP connection", i18n.TimeDurationType)
	ConfigMetricsWriteTimeout      = ffc("config.metrics.writeTimeout", "The maximum time to wait when writing to an HTTP connection", i18n.TimeDurationType)
	ConfigMetricsShutdownTimeout   = ffc("config.metrics.shutdownTimeout", "The maximum amount of time to wait for any open HTTP requests to finish before shutting down the HTTP server", i18n.TimeDurationType)

	ConfigDatabasePostgresMaxConnIdleTime   = ffc("config.persistence.postgres.maxConnIdleTime", "The maximum amount of time a database connection can be idle", i18n.TimeDurationType)
	ConfigDatabasePostgresMaxConnLifetime   = ffc("config.persistence.postgres.maxConnLifetime", "The maximum amount of time to keep a database connection open", i18n.TimeDurationType)
	ConfigDatabasePostgresMaxConns          = ffc("config.persistence.postgres.maxConns", "Maximum connections to the database", i18n.IntType)
	ConfigDatabasePostgresMaxIdleConns      = ffc("config.persistence.postgres.maxIdleConns", "The maximum number of idle connections to the database", i18n.IntType)
	ConfigDatabasePostgresURL               = ffc("config.persistence.postgres.url", "The PostgreSQL connection string for the database", i18n.StringType)
	ConfigGlobalMigrationsAuto              = ffc("config.global.migrations.auto", "Enables automatic database migrations", i18n.BooleanType)
	ConfigGlobalMigrationsDirectory         = ffc("config.global.migrations.directory", "The directory containing the numerically ordered migration DDL files to apply to the database", i18n.StringType)
	ConfigTXWriterBatchSize                 = ffc("config.global.txwriter.batchSize", "Number of persistence operations on transactions to attempt to group into a DB transaction", i18n.IntType)
	ConfigTXWriterBatchTimeout              = ffc("config.global.txwriter.batchTimeout", "Duration to hold batch open for new transaction operations before flushing to the DB", i18n.TimeDurationType)
	ConfigTXWriterCacheSlots                = ffc("config.global.txwriter.cacheSlots", "Number of transactions to hold cached metadata for to avoid DB read operations to calculate history", i18n.IntType)
	ConfigTXWriterCount                     = ffc("config.global.txwriter.count", "Number of transactions writing routines to start", i18n.IntType)
	ConfigTXWriterHistoryCompactionInterval = ffc("config.global.txwriter.historyCompactionInterval", "Duration between cleanup activities on the DB for a transaction with a large history", i18n.TimeDurationType)
	ConfigTXWriterHistorySummaryLimit       = ffc("config.global.txwriter.historySummaryLimit", "Maximum number of action entries to return embedded in the JSON response object when querying a transaction summary", i18n.IntType)
)
