// ============================================================================
// File: bigquery-client.js
// BigQuery API client with partition-aware queries

import { BigQuery } from '@google-cloud/bigquery';
import { QueryBuilder } from './query-builder.js';

/**
 * BigQuery client for fetching data with partition-aware queries
 * Supports column selection and distributed workload partitioning
 */
export class BigQueryClient {
	/**
	 * Creates a new BigQueryClient instance
	 * @param {Object} config - Configuration object
	 * @param {Object} config.bigquery - BigQuery configuration
	 * @param {string} config.bigquery.projectId - GCP project ID
	 * @param {string} config.bigquery.dataset - BigQuery dataset name
	 * @param {string} config.bigquery.table - BigQuery table name
	 * @param {string} config.bigquery.timestampColumn - Timestamp column name
	 * @param {string} config.bigquery.credentials - Path to credentials file
	 * @param {string} config.bigquery.location - BigQuery location (e.g., 'US', 'EU')
	 * @param {Array<string>} config.bigquery.columns - Columns to select (defaults to ['*'])
	 */
	constructor(config) {
		logger.info('[BigQueryClient] Constructor called - initializing BigQuery client');
		logger.debug(
			`[BigQueryClient] Config - projectId: ${config.bigquery.projectId}, dataset: ${config.bigquery.dataset}, table: ${config.bigquery.table}, location: ${config.bigquery.location}`
		);

		this.config = config;
		this.client = new BigQuery({
			projectId: config.bigquery.projectId,
			keyFilename: config.bigquery.credentials,
			location: config.bigquery.location,
		});

		this.dataset = config.bigquery.dataset;
		this.table = config.bigquery.table;
		this.timestampColumn = config.bigquery.timestampColumn;
		this.columns = config.bigquery.columns || ['*'];

		// Retry configuration with exponential backoff
		this.maxRetries = config.bigquery.maxRetries || 5;
		this.initialRetryDelay = config.bigquery.initialRetryDelay || 1000; // 1 second

		// Initialize query builder with column selection
		this.queryBuilder = new QueryBuilder({
			dataset: this.dataset,
			table: this.table,
			timestampColumn: this.timestampColumn,
			columns: this.columns,
		});

		logger.info(`[BigQueryClient] Client initialized successfully with columns: ${this.queryBuilder.getColumnList()}`);
		logger.info(
			`[BigQueryClient] Retry configuration - maxRetries: ${this.maxRetries}, initialDelay: ${this.initialRetryDelay}ms`
		);
	}

	/**
	 * Resolves all parameters that might be promises
	 * @param {Object} params - Parameter object
	 * @returns {Promise<Object>} Resolved parameters
	 * @private
	 */
	async resolveParams(params) {
		const entries = Object.entries(params);
		const resolvedEntries = await Promise.all(entries.map(async ([key, value]) => [key, await value]));
		return Object.fromEntries(resolvedEntries);
	}

	/**
	 * Determines if a BigQuery error is transient and should be retried
	 * @param {Error} error - The error to check
	 * @returns {boolean} True if the error is retryable
	 * @private
	 */
	isRetryableError(error) {
		if (!error) return false;

		// Check error code for retryable conditions
		const retryableCodes = [
			'rateLimitExceeded',
			'quotaExceeded',
			'internalError',
			'backendError',
			'serviceUnavailable',
			'timeout',
			503, // Service Unavailable
			429, // Too Many Requests
		];

		// Check error.code
		if (error.code && retryableCodes.includes(error.code)) {
			return true;
		}

		// Check nested errors array (BigQuery specific)
		if (error.errors && Array.isArray(error.errors)) {
			return error.errors.some((e) => e.reason && retryableCodes.includes(e.reason));
		}

		// Check HTTP status code
		if (error.response && retryableCodes.includes(error.response.status)) {
			return true;
		}

		return false;
	}

	/**
	 * Executes a query with exponential backoff retry logic
	 * @param {Function} queryFn - Async function that executes the query
	 * @param {string} operation - Name of the operation (for logging)
	 * @returns {Promise<*>} Query result
	 * @private
	 */
	async executeWithRetry(queryFn, operation) {
		let lastError;

		for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
			try {
				return await queryFn();
			} catch (error) {
				lastError = error;

				// Check if we should retry
				const isRetryable = this.isRetryableError(error);
				const attemptsRemaining = this.maxRetries - attempt;

				if (!isRetryable || attemptsRemaining === 0) {
					// Log detailed error information
					logger.error(
						`[BigQueryClient.${operation}] Query failed (attempt ${attempt + 1}/${this.maxRetries + 1}): ${error.message}`
					);
					if (error.errors) {
						error.errors.forEach((e) => logger.error(`  ${e.reason} at ${e.location}: ${e.message}`));
					}
					throw error;
				}

				// Calculate backoff delay with jitter
				// Exponential backoff: initialDelay * 2^attempt
				// Jitter: random value between 0 and calculated delay
				const exponentialDelay = this.initialRetryDelay * Math.pow(2, attempt);
				const jitter = Math.random() * exponentialDelay;
				const delay = Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds

				logger.warn(
					`[BigQueryClient.${operation}] Transient error (attempt ${attempt + 1}/${this.maxRetries + 1}): ${error.message}. Retrying in ${Math.round(delay)}ms...`
				);
				if (error.errors) {
					error.errors.forEach((e) => logger.warn(`  ${e.reason}: ${e.message}`));
				}

				// Wait before retrying
				await new Promise((resolve) => setTimeout(resolve, delay));
			}
		}

		// Should never reach here, but just in case
		throw lastError;
	}

	/**
	 * Pulls a partition of data from BigQuery
	 * Uses modulo-based partitioning for distributed workload
	 * @param {Object} options - Query options
	 * @param {number} options.nodeId - Current node ID (0-based)
	 * @param {number} options.clusterSize - Total number of nodes
	 * @param {string|Date} options.lastTimestamp - Last synced timestamp
	 * @param {number} options.batchSize - Number of records to fetch
	 * @returns {Promise<Array>} Array of records from BigQuery
	 */
	async pullPartition({ nodeId, clusterSize, lastTimestamp, batchSize }) {
		logger.info(
			`[BigQueryClient.pullPartition] Pulling partition - nodeId: ${nodeId}, clusterSize: ${clusterSize}, batchSize: ${batchSize}`
		);
		logger.debug(
			`[BigQueryClient.pullPartition] Query parameters - lastTimestamp: ${lastTimestamp}, timestampColumn: ${this.timestampColumn}`
		);

		// Build query using QueryBuilder
		const query = this.queryBuilder.buildPullPartitionQuery();

		// lastTimestamp is already an ISO string from checkpoint (String! type in schema)
		// Just pass it directly to BigQuery's TIMESTAMP() parameter
		const normalizedTimestamp = await this.normalizeToIso(lastTimestamp);
		logger.debug(`[BigQueryClient.pullPartition] Normalized timestamp: ${normalizedTimestamp}`);

		// Resolve any promise parameters
		const params = await this.resolveParams({
			nodeId,
			clusterSize,
			lastTimestamp: normalizedTimestamp,
			batchSize,
		});

		const options = {
			query,
			params: params,
		};

		logger.trace(`[BigQueryClient.pullPartition] Generated SQL query: ${query}`);

		return await this.executeWithRetry(async () => {
			logger.debug('[BigQueryClient.pullPartition] Executing BigQuery query...');
			const startTime = Date.now();
			const [rows] = await this.client.query(options);
			const duration = Date.now() - startTime;
			logger.info(`[BigQueryClient.pullPartition] Query complete - returned ${rows.length} rows in ${duration}ms`);
			logger.debug(
				`[BigQueryClient.pullPartition] First row timestamp: ${rows.length > 0 ? Date(rows[0][this.timestampColumn]) : 'N/A'}`
			);
			return rows;
		}, 'pullPartition');
	}

	/**
	 * Normalizes a timestamp to ISO 8601 format
	 * @param {Date|number|string|Object} ts - Timestamp to normalize
	 * @returns {Promise<string|null>} ISO 8601 formatted timestamp
	 * @throws {Error} If timestamp cannot be parsed
	 */
	async normalizeToIso(ts) {
		if (ts === null || ts === undefined) return null;

		if (ts instanceof Date) {
			// Check if the Date is valid before calling toISOString()
			if (Number.isNaN(ts.getTime())) {
				throw new Error(`Invalid Date object: ${ts}`);
			}
			return ts.toISOString();
		}

		if (typeof ts === 'number') return new Date(ts).toISOString();

		if (typeof ts === 'string') {
			// If someone passed "Wed Nov 05 2025 16:11:45 GMT-0700 (Mountain ...)"
			// normalize it to ISO; reject if not parseable.
			const d = new Date(ts);
			if (!Number.isNaN(d.getTime())) return d.toISOString();
			throw new Error(`Unparseable timestamp string: ${ts}`);
		}

		if (typeof ts.toISOString === 'function') return ts.toISOString();

		throw new Error(`Unsupported lastTimestamp type: ${typeof ts}`);
	}

	/**
	 * Counts records in a partition
	 * @param {Object} options - Query options
	 * @param {number} options.nodeId - Current node ID (0-based)
	 * @param {number} options.clusterSize - Total number of nodes
	 * @returns {Promise<number>} Count of records in partition
	 */
	async countPartition({ nodeId, clusterSize }) {
		logger.info(
			`[BigQueryClient.countPartition] Counting partition records - nodeId: ${nodeId}, clusterSize: ${clusterSize}`
		);

		// Build query using QueryBuilder
		const query = this.queryBuilder.buildCountPartitionQuery();

		logger.trace(`[BigQueryClient.countPartition] Count query: ${query}`);

		const options = {
			query,
			params: { clusterSize, nodeId },
		};

		return await this.executeWithRetry(async () => {
			logger.debug('[BigQueryClient.countPartition] Executing count query...');
			const startTime = Date.now();
			const [rows] = await this.client.query(options);
			const duration = Date.now() - startTime;
			const count = rows[0].count;
			logger.info(
				`[BigQueryClient.countPartition] Count complete - ${count} records in partition (took ${duration}ms)`
			);
			return count;
		}, 'countPartition');
	}

	/**
	 * Verifies that a specific record exists in BigQuery
	 * @param {Object} record - Record to verify
	 * @param {string} record.timestamp - Record timestamp
	 * @param {string} record.id - Record ID
	 * @returns {Promise<boolean>} True if record exists, false otherwise
	 */
	async verifyRecord(record) {
		logger.debug(`[BigQueryClient.verifyRecord] Verifying record - timestamp: ${record.timestamp}`);

		// Build query using QueryBuilder
		const query = this.queryBuilder.buildVerifyRecordQuery();

		logger.trace(`[BigQueryClient.verifyRecord] Verification query: ${query}`);

		// Normalize timestamp to ISO string for BigQuery
		// Records from Harper may have Date objects
		const normalizedTimestamp = await this.normalizeToIso(record.timestamp);

		const options = {
			query,
			params: {
				timestamp: normalizedTimestamp,
				recordId: record.id,
			},
		};

		try {
			return await this.executeWithRetry(async () => {
				logger.debug('[BigQueryClient.verifyRecord] Executing verification query...');
				const [rows] = await this.client.query(options);
				const exists = rows.length > 0;
				logger.debug(`[BigQueryClient.verifyRecord] Record ${exists ? 'EXISTS' : 'NOT FOUND'} in BigQuery`);
				return exists;
			}, 'verifyRecord');
		} catch (error) {
			logger.error(`[BigQueryClient.verifyRecord] Verification error after retries: ${error.message}`, error);
			return false;
		}
	}
}
