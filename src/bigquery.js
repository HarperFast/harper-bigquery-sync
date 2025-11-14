/**
 * BigQuery Client Wrapper for Maritime Vessel Data
 * Handles schema creation, data insertion, and retention management
 */

import { BigQuery } from '@google-cloud/bigquery';
import fs from 'fs';
import os from 'os';
import path from 'path';

class MaritimeBigQueryClient {
	constructor(config = {}) {
		this.projectId = config.projectId || process.env.GCP_PROJECT_ID;
		this.datasetId = config.datasetId || process.env.BIGQUERY_DATASET || 'maritime_tracking';
		this.tableId = config.tableId || process.env.BIGQUERY_TABLE || 'vessel_positions';
		this.retentionDays = config.retentionDays || parseInt(process.env.RETENTION_DAYS || '30', 10);
		this.location = config.location || process.env.BIGQUERY_LOCATION || 'US';

		// Streaming insert API option (off by default for backward compatibility)
		this.useStreamingAPIs = config.useStreamingAPIs || false;

		if (!this.projectId) {
			throw new Error('projectId must be set in config or GCP_PROJECT_ID environment variable');
		}

		// Configure BigQuery client
		const bqConfig = {
			projectId: this.projectId,
		};

		// Add credentials if provided (path to service account key file)
		if (config.credentials) {
			bqConfig.keyFilename = config.credentials;
		}

		this.bigquery = new BigQuery(bqConfig);

		this.dataset = this.bigquery.dataset(this.datasetId);
		this.table = this.dataset.table(this.tableId);
	}

	/**
	 * Define the BigQuery schema for vessel position data
	 */
	getSchema() {
		return [
			{ name: 'mmsi', type: 'STRING', mode: 'REQUIRED' },
			{ name: 'imo', type: 'STRING', mode: 'REQUIRED' },
			{ name: 'vessel_name', type: 'STRING', mode: 'REQUIRED' },
			{ name: 'vessel_type', type: 'STRING', mode: 'REQUIRED' },
			{ name: 'flag', type: 'STRING', mode: 'REQUIRED' },
			{ name: 'length', type: 'INTEGER', mode: 'REQUIRED' },
			{ name: 'beam', type: 'INTEGER', mode: 'REQUIRED' },
			{ name: 'draft', type: 'FLOAT', mode: 'REQUIRED' },
			{ name: 'latitude', type: 'FLOAT', mode: 'REQUIRED' },
			{ name: 'longitude', type: 'FLOAT', mode: 'REQUIRED' },
			{ name: 'speed_knots', type: 'FLOAT', mode: 'REQUIRED' },
			{ name: 'course', type: 'INTEGER', mode: 'REQUIRED' },
			{ name: 'heading', type: 'INTEGER', mode: 'REQUIRED' },
			{ name: 'status', type: 'STRING', mode: 'REQUIRED' },
			{ name: 'destination', type: 'STRING', mode: 'NULLABLE' },
			{ name: 'eta', type: 'TIMESTAMP', mode: 'NULLABLE' },
			{ name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
			{ name: 'report_date', type: 'STRING', mode: 'REQUIRED' },
		];
	}

	/**
	 * Initialize BigQuery resources (dataset and table)
	 */
	async initialize() {
		try {
			// Create dataset if it doesn't exist
			const [datasetExists] = await this.dataset.exists();
			if (!datasetExists) {
				console.log(`Creating dataset: ${this.datasetId}`);
				await this.bigquery.createDataset(this.datasetId, {
					location: this.location,
				});
				console.log(`Dataset ${this.datasetId} created`);
			} else {
				console.log(`Dataset ${this.datasetId} already exists`);
			}

			// Create table if it doesn't exist
			const [tableExists] = await this.table.exists();
			if (!tableExists) {
				console.log(`Creating table: ${this.tableId}`);
				const options = {
					schema: this.getSchema(),
					location: this.location,
					timePartitioning: {
						type: 'DAY',
						field: 'timestamp',
					},
					clustering: {
						fields: ['vessel_type', 'mmsi', 'report_date'],
					},
				};

				await this.dataset.createTable(this.tableId, options);
				console.log(`Table ${this.tableId} created with schema and partitioning`);
			} else {
				console.log(`Table ${this.tableId} already exists`);
			}

			return true;
		} catch (error) {
			console.error('Error initializing BigQuery resources:', error);
			throw error;
		}
	}

	/**
	 * Insert batch of records into BigQuery
	 * Dispatches to streaming or load job API based on configuration
	 * @param {Array} records - Records to insert
	 * @param {number} maxRetries - Maximum retry attempts (default: 5)
	 * @returns {Promise<Object>} - Result with success flag, recordCount, and method
	 */
	async insertBatch(records, maxRetries = 5) {
		if (this.useStreamingAPIs) {
			return await this._insertStreaming(records, maxRetries);
		} else {
			return await this._insertLoadJob(records, maxRetries);
		}
	}

	/**
	 * Insert batch using Streaming Insert API
	 * Lower latency but has cost implications
	 * @param {Array} records - Records to insert
	 * @param {number} maxRetries - Maximum retry attempts (default: 3)
	 * @returns {Promise<Object>} - Result with success flag, recordCount, and method
	 */
	async _insertStreaming(records, maxRetries = 3) {
		if (!records || records.length === 0) {
			throw new Error('No records to insert');
		}

		let lastError;

		for (let attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				// BigQuery streaming insert API
				await this.table.insert(records, {
					skipInvalidRows: false,
					ignoreUnknownValues: false,
				});

				// Success
				return {
					success: true,
					recordCount: records.length,
					method: 'streaming',
				};
			} catch (error) {
				lastError = error;

				// Handle partial failures
				if (error.name === 'PartialFailureError') {
					console.error('Partial failure - some rows failed to insert:', error.errors);

					// Log failed rows for debugging
					error.errors.forEach((err, index) => {
						console.error(`Row ${index} failed:`, err);
					});

					throw new Error(`Partial failure: ${error.errors.length} rows failed`);
				}

				// Check if this is a retryable error
				const isRetryable =
					error.code === 429 || // Quota exceeded
					error.code === 503 || // Service unavailable
					(error.code >= 500 && error.code < 600); // Server errors

				if (!isRetryable || attempt === maxRetries) {
					throw error;
				}

				// Exponential backoff: 1s, 2s, 4s
				const backoffMs = Math.pow(2, attempt - 1) * 1000;
				console.log(`Streaming insert failed (attempt ${attempt}/${maxRetries}): ${error.message}`);
				console.log(`Retrying in ${backoffMs / 1000}s...`);

				await new Promise((resolve) => setTimeout(resolve, backoffMs));
			}
		}

		// Should never reach here, but just in case
		throw lastError;
	}

	/**
	 * Insert batch using Load Job API (free tier compatible)
	 * Includes retry logic for transient network errors
	 * @param {Array} records - Records to insert
	 * @param {number} maxRetries - Maximum retry attempts (default: 5)
	 * @returns {Promise<Object>} - Result with success flag, recordCount, and method
	 */
	async _insertLoadJob(records, maxRetries = 5) {
		if (!records || records.length === 0) {
			throw new Error('No records to insert');
		}

		const tmpFile = path.join(os.tmpdir(), `maritime-vessels-${Date.now()}.ndjson`);

		try {
			// Write records to temporary NDJSON file
			const ndjson = records.map((record) => JSON.stringify(record)).join('\n');
			fs.writeFileSync(tmpFile, ndjson, 'utf8');

			// Retry loop for transient network errors
			let lastError;
			for (let attempt = 1; attempt <= maxRetries; attempt++) {
				try {
					// Load file into BigQuery - this waits for job completion
					await this.table.load(tmpFile, {
						sourceFormat: 'NEWLINE_DELIMITED_JSON',
						writeDisposition: 'WRITE_APPEND',
						autodetect: false,
						schema: { fields: this.getSchema() },
					});

					// Success - clean up and return
					fs.unlinkSync(tmpFile);

					if (attempt > 1) {
						console.log(`Batch inserted successfully after ${attempt} attempts`);
					}

					return {
						success: true,
						recordCount: records.length,
						method: 'load_job',
					};
				} catch (loadError) {
					lastError = loadError;

					// Check if this is a retryable error (network timeout, rate limit, etc.)
					const isRetryable =
						loadError.code === 'ETIMEDOUT' ||
						loadError.code === 'ECONNRESET' ||
						loadError.code === 'ENOTFOUND' ||
						loadError.code === 429 || // Rate limit
						(loadError.code >= 500 && loadError.code < 600); // Server errors

					if (!isRetryable || attempt === maxRetries) {
						// Non-retryable error or final attempt - clean up and throw
						if (fs.existsSync(tmpFile)) {
							fs.unlinkSync(tmpFile);
						}
						throw loadError;
					}

					// Exponential backoff: 2^attempt seconds (2s, 4s, 8s, 16s, 32s)
					const backoffMs = Math.pow(2, attempt) * 1000;
					console.log(`Upload failed (attempt ${attempt}/${maxRetries}): ${loadError.message}`);
					console.log(`Retrying in ${backoffMs / 1000}s...`);

					await new Promise((resolve) => setTimeout(resolve, backoffMs));
				}
			}

			// Should never reach here, but just in case
			throw lastError;
		} catch (error) {
			// Clean up temp file on any error
			if (fs.existsSync(tmpFile)) {
				fs.unlinkSync(tmpFile);
			}
			console.error('Error inserting batch:', error.message);
			throw error;
		}
	}

	/**
	 * Get table statistics
	 */
	async getStats() {
		try {
			const [metadata] = await this.table.getMetadata();

			// Query for additional statistics
			const query = `
        SELECT
          COUNT(*) as total_records,
          COUNT(DISTINCT mmsi) as unique_vessels,
          COUNT(DISTINCT vessel_type) as vessel_types,
          MIN(timestamp) as oldest_record,
          MAX(timestamp) as newest_record,
          APPROX_COUNT_DISTINCT(CONCAT(CAST(latitude AS STRING), ',', CAST(longitude AS STRING))) as unique_positions
        FROM \`${this.projectId}.${this.datasetId}.${this.tableId}\`
      `;

			const [rows] = await this.bigquery.query({ query });

			return {
				tableMetadata: {
					numBytes: metadata.numBytes,
					numRows: metadata.numRows,
					creationTime: metadata.creationTime,
					lastModifiedTime: metadata.lastModifiedTime,
				},
				statistics: rows[0],
			};
		} catch (error) {
			console.error('Error getting statistics:', error);
			throw error;
		}
	}

	/**
	 * Clean up old data based on retention policy
	 */
	async cleanupOldData() {
		try {
			const cutoffDate = new Date();
			cutoffDate.setDate(cutoffDate.getDate() - this.retentionDays);

			const query = `
        DELETE FROM \`${this.projectId}.${this.datasetId}.${this.tableId}\`
        WHERE TIMESTAMP(timestamp) < TIMESTAMP('${cutoffDate.toISOString()}')
      `;

			console.log(`Cleaning up data older than ${cutoffDate.toISOString()}`);

			const [job] = await this.bigquery.createQueryJob({
				query,
				location: this.location,
			});

			const [response] = await job.getQueryResults();

			console.log(`Cleanup completed. Deleted rows: ${response.length}`);

			return {
				success: true,
				cutoffDate: cutoffDate.toISOString(),
				deletedRows: response.length,
			};
		} catch (error) {
			console.error('Error cleaning up old data:', error);
			throw error;
		}
	}

	/**
	 * Clear all data from table (truncate) without deleting the table
	 */
	async clearData() {
		try {
			const query = `
        DELETE FROM \`${this.projectId}.${this.datasetId}.${this.tableId}\`
        WHERE TRUE
      `;

			console.log(`Clearing all data from ${this.tableId}...`);

			const [job] = await this.bigquery.createQueryJob({
				query,
				location: this.location,
			});

			await job.getQueryResults();

			console.log('All data cleared from table (schema preserved)');

			return { success: true };
		} catch (error) {
			console.error('Error clearing data:', error);
			throw error;
		}
	}

	/**
	 * Delete all data and table
	 */
	async deleteTable() {
		try {
			const [exists] = await this.table.exists();
			if (exists) {
				await this.table.delete();
				console.log(`Table ${this.tableId} deleted`);
				return { success: true };
			} else {
				console.log(`Table ${this.tableId} does not exist`);
				return { success: true, message: 'Table does not exist' };
			}
		} catch (error) {
			console.error('Error deleting table:', error);
			throw error;
		}
	}

	/**
	 * Query vessels by type
	 */
	async getVesselsByType(vesselType, limit = 100) {
		try {
			const query = `
        SELECT *
        FROM \`${this.projectId}.${this.datasetId}.${this.tableId}\`
        WHERE vessel_type = @vesselType
        ORDER BY timestamp DESC
        LIMIT @limit
      `;

			const options = {
				query,
				params: { vesselType, limit },
			};

			const [rows] = await this.bigquery.query(options);
			return rows;
		} catch (error) {
			console.error('Error querying vessels by type:', error);
			throw error;
		}
	}

	/**
	 * Query vessels in a geographic bounding box
	 */
	async getVesselsInBoundingBox(minLat, maxLat, minLon, maxLon, limit = 1000) {
		try {
			const query = `
        SELECT *
        FROM \`${this.projectId}.${this.datasetId}.${this.tableId}\`
        WHERE latitude BETWEEN @minLat AND @maxLat
          AND longitude BETWEEN @minLon AND @maxLon
          AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        ORDER BY timestamp DESC
        LIMIT @limit
      `;

			const options = {
				query,
				params: { minLat, maxLat, minLon, maxLon, limit },
			};

			const [rows] = await this.bigquery.query(options);
			return rows;
		} catch (error) {
			console.error('Error querying vessels in bounding box:', error);
			throw error;
		}
	}
}

export default MaritimeBigQueryClient;
