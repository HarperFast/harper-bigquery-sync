/**
 * Maritime Vessel Data Synthesizer Service
 * Orchestrates data generation and insertion into BigQuery
 */

import { EventEmitter } from 'events';
import MaritimeVesselGenerator from './generator.js';
import MaritimeBigQueryClient from './bigquery.js';

class MaritimeDataSynthesizer extends EventEmitter {
	constructor(config = {}) {
		super();

		this.config = {
			totalVessels: parseInt(config.totalVessels || process.env.TOTAL_VESSELS || '100000', 10),
			batchSize: parseInt(config.batchSize || process.env.BATCH_SIZE || '100', 10),
			generationIntervalMs: parseInt(config.generationIntervalMs || process.env.GENERATION_INTERVAL_MS || '60000', 10),
			retentionDays: parseInt(config.retentionDays || process.env.RETENTION_DAYS || '30', 10),
			cleanupIntervalHours: parseInt(config.cleanupIntervalHours || process.env.CLEANUP_INTERVAL_HOURS || '24', 10),
			...config,
		};

		this.generator = new MaritimeVesselGenerator({
			totalVessels: this.config.totalVessels,
			vesselsPerBatch: this.config.batchSize,
		});

		this.bigquery = new MaritimeBigQueryClient({
			projectId: config.projectId,
			datasetId: config.datasetId,
			tableId: config.tableId,
			credentials: config.credentials,
			location: config.location,
			retentionDays: this.config.retentionDays,
		});

		this.isRunning = false;
		this.generationTimer = null;
		this.cleanupTimer = null;
		this.stats = {
			totalBatchesGenerated: 0,
			totalRecordsInserted: 0,
			errors: 0,
			startTime: null,
		};
	}

	/**
	 * Initialize BigQuery resources and optionally load historical data
	 */
	async initialize(daysOfHistoricalData = 0) {
		try {
			this.emit('init:starting', { days: daysOfHistoricalData });

			// Initialize BigQuery schema
			await this.bigquery.initialize();
			this.emit('init:bigquery-ready');

			if (daysOfHistoricalData > 0) {
				await this.loadHistoricalData(daysOfHistoricalData);
			}

			this.emit('init:completed', { days: daysOfHistoricalData });
			return true;
		} catch (error) {
			this.emit('init:error', { error });
			throw error;
		}
	}

	/**
	 * Load historical data for specified number of days
	 */
	async loadHistoricalData(days) {
		try {
			this.emit('init:data-generation-starting', { days });

			const recordsPerDay =
				Math.floor((24 * 60 * 60 * 1000) / this.config.generationIntervalMs) * this.config.batchSize;
			const totalRecords = recordsPerDay * days;
			const totalBatches = Math.ceil(totalRecords / this.config.batchSize);

			console.log(`Loading ${days} days of historical data...`);
			console.log(`  Records per day: ${recordsPerDay.toLocaleString()}`);
			console.log(`  Total records: ${totalRecords.toLocaleString()}`);
			console.log(`  Total batches: ${totalBatches.toLocaleString()}`);
			console.log(`  Estimated time: ${Math.ceil((totalBatches * 2) / 60)} minutes`);

			let recordsInserted = 0;
			const startTime = Date.now();

			for (let batchNum = 0; batchNum < totalBatches; batchNum++) {
				// Calculate time offset for this batch (spread evenly across the days)
				const timeOffsetMs = (batchNum / totalBatches) * days * 24 * 60 * 60 * 1000;

				// Generate batch with timestamp offset
				const records = this.generator.generateBatch(this.config.batchSize, timeOffsetMs);

				// Insert into BigQuery
				await this.bigquery.insertBatch(records);

				recordsInserted += records.length;

				// Emit progress
				const progress = (((batchNum + 1) / totalBatches) * 100).toFixed(1);
				this.emit('init:progress', {
					batchNum: batchNum + 1,
					totalBatches,
					recordsInserted,
					totalRecords,
					progress: parseFloat(progress),
				});

				// Log progress periodically
				if ((batchNum + 1) % 10 === 0 || batchNum === totalBatches - 1) {
					const elapsed = (Date.now() - startTime) / 1000;
					const rate = recordsInserted / elapsed;
					const remaining = (totalRecords - recordsInserted) / rate;

					console.log(
						`Progress: ${progress}% | ` +
							`Batch ${batchNum + 1}/${totalBatches} | ` +
							`Records: ${recordsInserted.toLocaleString()}/${totalRecords.toLocaleString()} | ` +
							`Rate: ${Math.floor(rate)} records/sec | ` +
							`ETA: ${Math.ceil(remaining / 60)} min`
					);
				}

				// Rate limiting: wait 1 second between batches to avoid overwhelming BigQuery
				if (batchNum < totalBatches - 1) {
					await new Promise((resolve) => setTimeout(resolve, 1000));
				}
			}

			const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
			console.log(`Historical data loaded: ${recordsInserted.toLocaleString()} records in ${totalTime} minutes`);

			this.emit('init:data-generation-completed', {
				recordsInserted,
				totalTime: parseFloat(totalTime),
			});
		} catch (error) {
			console.error('Error loading historical data:', error);
			this.emit('init:data-generation-error', { error });
			throw error;
		}
	}

	/**
	 * Check current data range and determine if backfill is needed
	 */
	async checkDataRange() {
		try {
			const query = `
        SELECT
          MIN(timestamp) as oldest,
          MAX(timestamp) as newest,
          COUNT(*) as total_records
        FROM \`${this.bigquery.projectId}.${this.bigquery.datasetId}.${this.bigquery.tableId}\`
      `;

			const [rows] = await this.bigquery.bigquery.query({ query });

			if (rows.length === 0 || rows[0].total_records === '0') {
				return {
					hasData: false,
					oldestTimestamp: null,
					newestTimestamp: null,
					totalRecords: 0,
					daysCovered: 0,
				};
			}

			const oldest = new Date(rows[0].oldest.value);
			const newest = new Date(rows[0].newest.value);
			const daysCovered = (newest - oldest) / (24 * 60 * 60 * 1000);

			return {
				hasData: true,
				oldestTimestamp: oldest,
				newestTimestamp: newest,
				totalRecords: parseInt(rows[0].total_records),
				daysCovered: Math.floor(daysCovered),
			};
		} catch (error) {
			// Table might not exist yet
			if (error.message.includes('Not found')) {
				return {
					hasData: false,
					oldestTimestamp: null,
					newestTimestamp: null,
					totalRecords: 0,
					daysCovered: 0,
				};
			}
			throw error;
		}
	}

	/**
	 * Start the data generation service with rolling window support
	 */
	async start(options = {}) {
		if (this.isRunning) {
			console.log('Service is already running');
			return;
		}

		const maintainWindow = options.maintainWindow !== false; // Default true
		const targetDays = options.targetDays || this.config.retentionDays;

		try {
			this.emit('service:starting');

			this.isRunning = true;
			this.stats.startTime = new Date();

			// Check if we need to backfill
			if (maintainWindow) {
				console.log(`Checking data range (target: ${targetDays} days)...`);
				const dataRange = await this.checkDataRange();

				if (!dataRange.hasData) {
					console.log('No existing data found. Initializing with historical data...');
					await this.initialize(targetDays);
				} else {
					console.log(
						`Found ${dataRange.totalRecords.toLocaleString()} records covering ${dataRange.daysCovered} days`
					);
					console.log(`  Oldest: ${dataRange.oldestTimestamp.toISOString()}`);
					console.log(`  Newest: ${dataRange.newestTimestamp.toISOString()}`);

					const daysNeeded = targetDays - dataRange.daysCovered;
					if (daysNeeded > 1) {
						console.log(`Backfilling ${Math.floor(daysNeeded)} days to reach ${targetDays}-day window...`);
						await this.backfillHistoricalData(Math.floor(daysNeeded), dataRange.oldestTimestamp);
					} else {
						console.log(`Data window is sufficient (${dataRange.daysCovered}/${targetDays} days)`);
					}
				}
			}

			// Start generation loop
			console.log('\nStarting continuous generation...');
			await this.generateAndInsertBatch(); // Run immediately
			this.generationTimer = setInterval(() => this.generateAndInsertBatch(), this.config.generationIntervalMs);

			// Start cleanup loop
			setTimeout(() => {
				this.cleanupOldData();
				this.cleanupTimer = setInterval(() => this.cleanupOldData(), this.config.cleanupIntervalHours * 60 * 60 * 1000);
			}, 60000); // Wait 1 minute before first cleanup

			console.log('\nMaritime Data Synthesizer started');
			console.log(`  Batch size: ${this.config.batchSize} vessels`);
			console.log(`  Generation interval: ${this.config.generationIntervalMs / 1000} seconds`);
			console.log(
				`  Records per day: ~${Math.floor((24 * 60 * 60 * 1000) / this.config.generationIntervalMs) * this.config.batchSize}`
			);
			console.log(`  Rolling window: ${this.config.retentionDays} days`);
			console.log(`  Cleanup interval: ${this.config.cleanupIntervalHours} hours`);

			this.emit('service:started');
		} catch (error) {
			this.isRunning = false;
			this.emit('service:error', { error });
			throw error;
		}
	}

	/**
	 * Backfill historical data before a specific timestamp
	 */
	async backfillHistoricalData(days, beforeTimestamp) {
		try {
			this.emit('backfill:starting', { days, beforeTimestamp });

			const recordsPerDay =
				Math.floor((24 * 60 * 60 * 1000) / this.config.generationIntervalMs) * this.config.batchSize;
			const totalRecords = recordsPerDay * days;
			const totalBatches = Math.ceil(totalRecords / this.config.batchSize);

			console.log(`Backfilling ${days} days of historical data...`);
			console.log(`  Records per day: ${recordsPerDay.toLocaleString()}`);
			console.log(`  Total records: ${totalRecords.toLocaleString()}`);
			console.log(`  Total batches: ${totalBatches.toLocaleString()}`);

			let recordsInserted = 0;
			const startTime = Date.now();
			const oldestTimestamp = beforeTimestamp.getTime();

			for (let batchNum = 0; batchNum < totalBatches; batchNum++) {
				// Calculate time offset - going backwards from beforeTimestamp
				const timeOffsetMs = (batchNum / totalBatches) * days * 24 * 60 * 60 * 1000;
				const batchTimestamp = oldestTimestamp - days * 24 * 60 * 60 * 1000 + timeOffsetMs;

				// Generate batch with timestamp offset
				const records = this.generator.generateBatch(this.config.batchSize, Date.now() - batchTimestamp);

				// Insert into BigQuery
				await this.bigquery.insertBatch(records);

				recordsInserted += records.length;

				// Emit progress
				const progress = (((batchNum + 1) / totalBatches) * 100).toFixed(1);
				this.emit('backfill:progress', {
					batchNum: batchNum + 1,
					totalBatches,
					recordsInserted,
					totalRecords,
					progress: parseFloat(progress),
				});

				// Log progress periodically
				if ((batchNum + 1) % 10 === 0 || batchNum === totalBatches - 1) {
					const elapsed = (Date.now() - startTime) / 1000;
					const rate = recordsInserted / elapsed;
					const remaining = (totalRecords - recordsInserted) / rate;

					console.log(
						`Backfill: ${progress}% | ` +
							`Batch ${batchNum + 1}/${totalBatches} | ` +
							`Records: ${recordsInserted.toLocaleString()}/${totalRecords.toLocaleString()} | ` +
							`Rate: ${Math.floor(rate)} records/sec | ` +
							`ETA: ${Math.ceil(remaining / 60)} min`
					);
				}

				// Rate limiting
				if (batchNum < totalBatches - 1) {
					await new Promise((resolve) => setTimeout(resolve, 1000));
				}
			}

			const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
			console.log(`Backfill completed: ${recordsInserted.toLocaleString()} records in ${totalTime} minutes`);

			this.emit('backfill:completed', {
				recordsInserted,
				totalTime: parseFloat(totalTime),
			});
		} catch (error) {
			console.error('Error backfilling historical data:', error);
			this.emit('backfill:error', { error });
			throw error;
		}
	}

	/**
	 * Generate a batch of records and insert into BigQuery
	 */
	async generateAndInsertBatch() {
		try {
			this.emit('batch:generating', { size: this.config.batchSize });

			const records = this.generator.generateBatch(this.config.batchSize);

			this.emit('batch:generated', {
				records: records.length,
				sample: records[0],
			});

			this.emit('batch:inserting', { records: records.length });

			const result = await this.bigquery.insertBatch(records);

			this.stats.totalBatchesGenerated++;
			this.stats.totalRecordsInserted += records.length;

			this.emit('batch:inserted', {
				records: records.length,
				totalBatches: this.stats.totalBatchesGenerated,
				totalRecords: this.stats.totalRecordsInserted,
				jobId: result.jobId,
			});

			console.log(
				`Batch inserted: ${records.length} records | ` +
					`Total: ${this.stats.totalRecordsInserted.toLocaleString()} records | ` +
					`Batches: ${this.stats.totalBatchesGenerated}`
			);
		} catch (error) {
			this.stats.errors++;
			this.emit('batch:error', { error, errorCount: this.stats.errors });
			console.error('Error generating/inserting batch:', error.message);
		}
	}

	/**
	 * Clean up old data based on retention policy
	 */
	async cleanupOldData() {
		try {
			this.emit('cleanup:starting', { retentionDays: this.config.retentionDays });

			const result = await this.bigquery.cleanupOldData();

			this.emit('cleanup:completed', {
				cutoffDate: result.cutoffDate,
				deletedRows: result.deletedRows,
			});

			console.log(`Cleanup completed: deleted ${result.deletedRows} rows older than ${result.cutoffDate}`);
		} catch (error) {
			this.emit('cleanup:error', { error });
			console.error('Error during cleanup:', error.message);
		}
	}

	/**
	 * Stop the service
	 */
	async stop() {
		if (!this.isRunning) {
			console.log('Service is not running');
			return;
		}

		try {
			this.emit('service:stopping');

			this.isRunning = false;

			if (this.generationTimer) {
				clearInterval(this.generationTimer);
				this.generationTimer = null;
			}

			if (this.cleanupTimer) {
				clearInterval(this.cleanupTimer);
				this.cleanupTimer = null;
			}

			console.log('Maritime Data Synthesizer stopped');
			this.emit('service:stopped', { stats: this.getStats() });
		} catch (error) {
			this.emit('service:error', { error });
			throw error;
		}
	}

	/**
	 * Get service statistics
	 */
	getStats() {
		const uptime = this.stats.startTime ? (Date.now() - this.stats.startTime.getTime()) / 1000 : 0;

		return {
			...this.stats,
			uptime: Math.floor(uptime),
			isRunning: this.isRunning,
			generatorStats: this.generator.getStats(),
			config: this.config,
		};
	}

	/**
	 * Get BigQuery table statistics
	 */
	async getBigQueryStats() {
		return await this.bigquery.getStats();
	}

	/**
	 * Clear all data from table (truncate) without deleting the table
	 */
	async clear() {
		try {
			this.emit('clear:starting');

			await this.bigquery.clearData();

			this.emit('clear:completed');
			console.log('All data cleared from table');

			return true;
		} catch (error) {
			this.emit('clear:error', { error });
			throw error;
		}
	}

	/**
	 * Delete all data and table
	 */
	async clean() {
		try {
			this.emit('clean:starting');

			await this.bigquery.deleteTable();

			this.emit('clean:completed');
			console.log('All data and table deleted');

			return true;
		} catch (error) {
			this.emit('clean:error', { error });
			throw error;
		}
	}

	/**
	 * Reset: delete and reinitialize with historical data
	 */
	async reset(daysOfHistoricalData = 30) {
		try {
			console.log('Resetting maritime data synthesizer...');

			// Stop if running
			if (this.isRunning) {
				await this.stop();
			}

			// Delete table
			await this.clean();

			// Reinitialize
			await this.initialize(daysOfHistoricalData);

			console.log('Reset completed');
			return true;
		} catch (error) {
			console.error('Error during reset:', error);
			throw error;
		}
	}
}

export default MaritimeDataSynthesizer;
