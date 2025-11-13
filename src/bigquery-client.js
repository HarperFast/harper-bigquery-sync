// ============================================================================
// File: bigquery-client.js
// BigQuery API client with partition-aware queries

import { BigQuery } from '@google-cloud/bigquery';

export class BigQueryClient {
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
		logger.info('[BigQueryClient] Client initialized successfully');
	}

	async resolveParams(params) {
		const entries = Object.entries(params);
		const resolvedEntries = await Promise.all(entries.map(async ([key, value]) => [key, await value]));
		return Object.fromEntries(resolvedEntries);
	}

	async pullPartition({ nodeId, clusterSize, lastTimestamp, batchSize }) {
		logger.info(
			`[BigQueryClient.pullPartition] Pulling partition - nodeId: ${nodeId}, clusterSize: ${clusterSize}, batchSize: ${batchSize}`
		);
		logger.debug(
			`[BigQueryClient.pullPartition] Query parameters - lastTimestamp: ${lastTimestamp} type: ${typeof lastTimestamp}, timestampColumn: ${this.timestampColumn}`
		);

		const query = `
    SELECT *
    FROM \`${this.dataset}.${this.table}\`
    WHERE
      -- guard + normalize types
      CAST(@clusterSize AS INT64) > 0
      AND CAST(@nodeId AS INT64) BETWEEN 0 AND CAST(@clusterSize AS INT64) - 1
      -- sharding
      AND MOD(UNIX_MICROS(${this.timestampColumn}), CAST(@clusterSize AS INT64)) = CAST(@nodeId AS INT64)
      -- time filter
      AND ${this.timestampColumn} > TIMESTAMP(@lastTimestamp)
    ORDER BY ${this.timestampColumn} ASC
    LIMIT CAST(@batchSize AS INT64)
    `;

		// Assume these might return Promises:
		const params = await this.resolveParams({
			nodeId,
			clusterSize,
			lastTimestamp,
			batchSize,
		});

		const options = {
			query,
			params: params,
		};

		logger.trace(`[BigQueryClient.pullPartition] Generated SQL query: ${query}`);

		try {
			logger.debug('[BigQueryClient.pullPartition] Executing BigQuery query...');
			const startTime = Date.now();
			const [rows] = await this.client.query(options);
			const duration = Date.now() - startTime;
			logger.info(`[BigQueryClient.pullPartition] Query complete - returned ${rows.length} rows in ${duration}ms`);
			logger.debug(
				`[BigQueryClient.pullPartition] First row timestamp: ${rows.length > 0 ? Date(rows[0][this.timestampColumn]) : 'N/A'}`
			);
			return rows;
		} catch (error) {
			// Always log full error detail
			logger.error('[BigQueryClient.pullPartition] BigQuery query failed');
			logger.error(`Error name: ${error.name}`);
			logger.error(`Error message: ${error.message}`);
			logger.error(`Error stack: ${error.stack}`);

			// BigQuery often includes structured info
			if (error.errors) {
				for (const e of error.errors) {
					logger.error(`BigQuery error reason: ${e.reason}`);
					logger.error(`BigQuery error location: ${e.location}`);
					logger.error(`BigQuery error message: ${e.message}`);
				}
			}
		}
	}

	async normalizeToIso(ts) {
		if (ts === null || ts === undefined) return null;

		if (ts instanceof Date) return ts.toISOString();

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

	async countPartition({ nodeId, clusterSize }) {
		logger.info(
			`[BigQueryClient.countPartition] Counting partition records - nodeId: ${nodeId}, clusterSize: ${clusterSize}`
		);

		const query = `
      SELECT COUNT(*) as count
      FROM \`${this.dataset}.${this.table}\`
      WHERE MOD(
        ABS(FARM_FINGERPRINT(CAST(${this.timestampColumn} AS STRING))),
        @clusterSize
      ) = @nodeId
    `;

		logger.trace(`[BigQueryClient.countPartition] Count query: ${query}`);

		const options = {
			query,
			params: { clusterSize, nodeId },
		};

		try {
			logger.debug('[BigQueryClient.countPartition] Executing count query...');
			const startTime = Date.now();
			const [rows] = await this.client.query(options);
			const duration = Date.now() - startTime;
			const count = rows[0].count;
			logger.info(
				`[BigQueryClient.countPartition] Count complete - ${count} records in partition (took ${duration}ms)`
			);
			return count;
		} catch (error) {
			logger.error(`[BigQueryClient.countPartition] Count query error: ${error.message}`, error);
			throw error;
		}
	}

	async verifyRecord(record) {
		logger.debug(`[BigQueryClient.verifyRecord] Verifying record - timestamp: ${record.timestamp}`);
		// Verify a specific record exists in BigQuery by timestamp and unique identifier
		// Note: This assumes a unique identifier field exists - adapt to your schema
		const query = `
      SELECT 1
      FROM \`${this.dataset}.${this.table}\`
      WHERE ${this.timestampColumn} = @timestamp
        AND id = @recordId
      LIMIT 1
    `;

		logger.trace(`[BigQueryClient.verifyRecord] Verification query: ${query}`);

		const options = {
			query,
			params: {
				timestamp: record.timestamp,
				recordId: record.id,
			},
		};

		try {
			logger.debug('[BigQueryClient.verifyRecord] Executing verification query...');
			const [rows] = await this.client.query(options);
			const exists = rows.length > 0;
			logger.debug(`[BigQueryClient.verifyRecord] Record ${exists ? 'EXISTS' : 'NOT FOUND'} in BigQuery`);
			return exists;
		} catch (error) {
			logger.error(`[BigQueryClient.verifyRecord] Verification error: ${error.message}`, error);
			return false;
		}
	}
}
