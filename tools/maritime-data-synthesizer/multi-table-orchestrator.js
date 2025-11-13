#!/usr/bin/env node

/**
 * Multi-Table Data Orchestrator
 *
 * Coordinates data generation across multiple BigQuery tables:
 * - vessel_positions (high frequency position data)
 * - port_events (medium frequency port activity)
 * - vessel_metadata (low frequency vessel details)
 *
 * Ensures consistent MMSI identifiers across all tables and
 * realistic data relationships.
 */

import { BigQuery } from '@google-cloud/bigquery';
import { VesselPositionsGenerator } from './generators/vessel-positions-generator.js';
import { PortEventsGenerator } from './generators/port-events-generator.js';
import { VesselMetadataGenerator } from './generators/vessel-metadata-generator.js';
import { TEST_SCENARIOS, SAMPLE_VESSELS } from '../../test/fixtures/multi-table-test-data.js';
import fs from 'fs';
import os from 'os';
import path from 'path';

export class MultiTableOrchestrator {
	/**
	 * Creates a new MultiTableOrchestrator
	 * @param {Object} options - Configuration options
	 * @param {Object} options.bigquery - BigQuery configuration
	 * @param {string} options.bigquery.projectId - GCP project ID
	 * @param {string} options.bigquery.keyFilename - Path to service account key
	 * @param {string} options.bigquery.location - BigQuery location
	 * @param {string} options.scenario - Scenario name ('small', 'realistic', 'stress')
	 * @param {Date} options.startTime - Start timestamp
	 */
	constructor(options) {
		this.projectId = options.bigquery.projectId;
		this.keyFilename = options.bigquery.keyFilename;
		this.location = options.bigquery.location;

		// Get scenario configuration
		this.scenario = TEST_SCENARIOS[options.scenario] || TEST_SCENARIOS.realistic;
		this.startTime = options.startTime ? new Date(options.startTime) : new Date();

		// Continuous generation configuration
		this.config = {
			batchSize: parseInt(options.batchSize || 100, 10),
			generationIntervalMs: parseInt(options.generationIntervalMs || 60000, 10), // 1 minute default
			retentionDays: parseInt(options.retentionDays || 30, 10),
			cleanupIntervalHours: parseInt(options.cleanupIntervalHours || 24, 10),
			dataset: options.dataset,
		};

		// Initialize BigQuery client
		this.bigquery = new BigQuery({
			projectId: this.projectId,
			keyFilename: this.keyFilename,
			location: this.location,
		});

		// Generate consistent MMSI list for all tables
		this.mmsiList = this.generateMmsiList();

		// State management for continuous generation
		this.isRunning = false;
		this.generationTimer = null;
		this.cleanupTimer = null;
		this.stats = {
			totalBatchesGenerated: 0,
			totalRecordsInserted: 0,
			errors: 0,
			startTime: null,
		};

		// Initialize generators for continuous mode
		this.generators = null;

		console.log(`\nMulti-Table Orchestrator initialized:`);
		console.log(`  Scenario: ${options.scenario} (${this.scenario.description})`);
		console.log(`  Start time: ${this.startTime.toISOString()}`);
		console.log(`  Duration: ${this.scenario.duration}`);
		console.log(`  Vessels (MMSI): ${this.mmsiList.length}`);
		console.log(`  Tables to generate:`);
		console.log(`    - vessel_positions: ${this.scenario.vessel_positions} records`);
		console.log(`    - port_events: ${this.scenario.port_events} records`);
		console.log(`    - vessel_metadata: ${this.scenario.vessel_metadata} records`);
	}

	/**
	 * Generates a consistent list of MMSI identifiers
	 * @returns {Array<string>} List of MMSI identifiers
	 * @private
	 */
	generateMmsiList() {
		// Start with sample vessels
		const mmsiList = SAMPLE_VESSELS.map((v) => v.mmsi);

		// Add more MMSI if needed for the scenario
		const neededCount = Math.max(
			this.scenario.vessel_metadata,
			Math.floor(this.scenario.vessel_positions / 100),
			Math.floor(this.scenario.port_events / 10)
		);

		while (mmsiList.length < neededCount) {
			// Generate random 9-digit MMSI starting with 3 (US vessels)
			const mmsi = `367${String(Math.floor(Math.random() * 1000000)).padStart(6, '0')}`;
			if (!mmsiList.includes(mmsi)) {
				mmsiList.push(mmsi);
			}
		}

		return mmsiList;
	}

	/**
	 * Generates all tables for the scenario
	 * @param {Object} options - Generation options
	 * @param {string} options.dataset - BigQuery dataset name
	 * @param {boolean} options.createDataset - Whether to create dataset if missing
	 * @param {boolean} options.truncateTables - Whether to truncate existing tables
	 * @returns {Promise<Object>} Generation results
	 */
	async generateAll({ dataset, createDataset = true, truncateTables = false }) {
		console.log(`\n=== Starting Multi-Table Generation ===\n`);

		const startTime = Date.now();

		try {
			// Step 1: Setup dataset
			if (createDataset) {
				await this.createDataset(dataset);
			}

			// Step 2: Create tables
			await this.createTables(dataset);

			// Step 3: Truncate if requested
			if (truncateTables) {
				await this.truncateTables(dataset);
			}

			// Step 4: Generate and insert vessel_metadata (slowest changing)
			console.log(`\n[1/3] Generating vessel_metadata...`);
			const metadataResults = await this.generateVesselMetadata(dataset);

			// Step 5: Generate and insert port_events (medium frequency)
			console.log(`\n[2/3] Generating port_events...`);
			const eventsResults = await this.generatePortEvents(dataset);

			// Step 6: Generate and insert vessel_positions (highest frequency)
			console.log(`\n[3/3] Generating vessel_positions...`);
			const positionsResults = await this.generateVesselPositions(dataset);

			const duration = Date.now() - startTime;

			console.log(`\n=== Generation Complete ===`);
			console.log(`  Total time: ${(duration / 1000).toFixed(1)}s`);
			console.log(`  Dataset: ${dataset}`);
			console.log(`  Tables generated: 3`);
			console.log(`  Total records: ${metadataResults.count + eventsResults.count + positionsResults.count}`);

			return {
				success: true,
				duration,
				tables: {
					vessel_metadata: metadataResults,
					port_events: eventsResults,
					vessel_positions: positionsResults,
				},
			};
		} catch (error) {
			console.error(`\nError during generation:`, error);
			throw error;
		}
	}

	/**
	 * Creates BigQuery dataset if it doesn't exist
	 * @param {string} dataset - Dataset name
	 * @private
	 */
	async createDataset(dataset) {
		try {
			const [exists] = await this.bigquery.dataset(dataset).exists();

			if (!exists) {
				console.log(`Creating dataset: ${dataset}`);
				await this.bigquery.createDataset(dataset, {
					location: this.location,
				});
				console.log(`✓ Dataset created`);
			} else {
				console.log(`✓ Dataset exists: ${dataset}`);
			}
		} catch (error) {
			console.error(`Error creating dataset:`, error);
			throw error;
		}
	}

	/**
	 * Creates all required tables with schemas
	 * @param {string} dataset - Dataset name
	 * @private
	 */
	async createTables(dataset) {
		console.log(`\nCreating tables in dataset: ${dataset}`);

		const tables = [
			{
				name: 'vessel_positions',
				schema: [
					{ name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
					{ name: 'mmsi', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'latitude', type: 'FLOAT64', mode: 'REQUIRED' },
					{ name: 'longitude', type: 'FLOAT64', mode: 'REQUIRED' },
					{ name: 'speed_knots', type: 'FLOAT64' },
					{ name: 'heading', type: 'FLOAT64' },
					{ name: 'course', type: 'FLOAT64' },
					{ name: 'status', type: 'STRING' },
					{ name: 'vessel_name', type: 'STRING' },
					{ name: 'vessel_type', type: 'STRING' },
					{ name: 'destination', type: 'STRING' },
					{ name: 'eta', type: 'TIMESTAMP' },
				],
			},
			{
				name: 'port_events',
				schema: [
					{ name: 'event_time', type: 'TIMESTAMP', mode: 'REQUIRED' },
					{ name: 'port_id', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'port_name', type: 'STRING' },
					{ name: 'vessel_mmsi', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'event_type', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'status', type: 'STRING' },
					{ name: 'latitude', type: 'FLOAT64' },
					{ name: 'longitude', type: 'FLOAT64' },
				],
			},
			{
				name: 'vessel_metadata',
				schema: [
					{ name: 'last_updated', type: 'TIMESTAMP', mode: 'REQUIRED' },
					{ name: 'mmsi', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'imo', type: 'STRING' },
					{ name: 'vessel_name', type: 'STRING' },
					{ name: 'vessel_type', type: 'STRING' },
					{ name: 'flag', type: 'STRING' },
					{ name: 'callsign', type: 'STRING' },
					{ name: 'length', type: 'INTEGER' },
					{ name: 'beam', type: 'INTEGER' },
					{ name: 'draft', type: 'INTEGER' },
					{ name: 'gross_tonnage', type: 'INTEGER' },
					{ name: 'deadweight', type: 'INTEGER' },
					{ name: 'year_built', type: 'INTEGER' },
					{ name: 'home_port', type: 'STRING' },
					{ name: 'owner', type: 'STRING' },
					{ name: 'status', type: 'STRING' },
				],
			},
		];

		for (const tableConfig of tables) {
			try {
				const table = this.bigquery.dataset(dataset).table(tableConfig.name);
				const [exists] = await table.exists();

				if (!exists) {
					console.log(`  Creating table: ${tableConfig.name}`);
					await this.bigquery.dataset(dataset).createTable(tableConfig.name, {
						schema: tableConfig.schema,
					});
					console.log(`  ✓ Table created: ${tableConfig.name}`);
				} else {
					console.log(`  ✓ Table exists: ${tableConfig.name}`);
				}
			} catch (error) {
				console.error(`  Error creating table ${tableConfig.name}:`, error);
				throw error;
			}
		}
	}

	/**
	 * Truncates all tables
	 * @param {string} dataset - Dataset name
	 * @private
	 */
	async truncateTables(dataset) {
		console.log(`\nTruncating tables...`);

		const tables = ['vessel_positions', 'port_events', 'vessel_metadata'];

		for (const tableName of tables) {
			try {
				await this.bigquery.query({
					query: `DELETE FROM \`${this.projectId}.${dataset}.${tableName}\` WHERE true`,
				});
				console.log(`  ✓ Truncated: ${tableName}`);
			} catch (error) {
				console.error(`  Error truncating ${tableName}:`, error.message);
			}
		}
	}

	/**
	 * Generates and inserts vessel_metadata
	 * @param {string} dataset - Dataset name
	 * @returns {Promise<Object>} Generation results
	 * @private
	 */
	async generateVesselMetadata(dataset) {
		const generator = new VesselMetadataGenerator({
			startTime: this.startTime,
			durationMs: this.scenario.durationMs,
			mmsiList: this.mmsiList,
		});

		const records = generator.generate(this.scenario.vessel_metadata);

		console.log(`  Generated ${records.length} vessel_metadata records`);
		console.log(`  Inserting into BigQuery...`);

		const startInsert = Date.now();
		await this.insertRecords(dataset, 'vessel_metadata', records);
		const insertDuration = Date.now() - startInsert;

		console.log(`  ✓ Inserted in ${(insertDuration / 1000).toFixed(1)}s`);

		return {
			count: records.length,
			duration: insertDuration,
			stats: generator.getStatistics(records),
		};
	}

	/**
	 * Generates and inserts port_events
	 * @param {string} dataset - Dataset name
	 * @returns {Promise<Object>} Generation results
	 * @private
	 */
	async generatePortEvents(dataset) {
		const generator = new PortEventsGenerator({
			startTime: this.startTime,
			durationMs: this.scenario.durationMs,
			mmsiList: this.mmsiList,
		});

		const records = generator.generate(this.scenario.port_events);

		console.log(`  Generated ${records.length} port_events records`);
		console.log(`  Inserting into BigQuery...`);

		const startInsert = Date.now();
		await this.insertRecords(dataset, 'port_events', records);
		const insertDuration = Date.now() - startInsert;

		console.log(`  ✓ Inserted in ${(insertDuration / 1000).toFixed(1)}s`);

		return {
			count: records.length,
			duration: insertDuration,
			stats: generator.getStatistics(records),
		};
	}

	/**
	 * Generates and inserts vessel_positions
	 * @param {string} dataset - Dataset name
	 * @returns {Promise<Object>} Generation results
	 * @private
	 */
	async generateVesselPositions(dataset) {
		const generator = new VesselPositionsGenerator({
			startTime: this.startTime,
			durationMs: this.scenario.durationMs,
			vessels: this.mmsiList.map((mmsi, i) => ({
				mmsi,
				startLat: 37.7749 + (i % 10) * 0.1,
				startLon: -122.4194 + Math.floor(i / 10) * 0.1,
				vesselName: `VESSEL_${mmsi}`,
				vesselType: 'Container Ship',
			})),
		});

		const records = generator.generate(this.scenario.vessel_positions);

		console.log(`  Generated ${records.length} vessel_positions records`);
		console.log(`  Inserting into BigQuery...`);

		const startInsert = Date.now();
		await this.insertRecords(dataset, 'vessel_positions', records);
		const insertDuration = Date.now() - startInsert;

		console.log(`  ✓ Inserted in ${(insertDuration / 1000).toFixed(1)}s`);

		return {
			count: records.length,
			duration: insertDuration,
		};
	}

	/**
	 * Inserts records into BigQuery table using load job API
	 * @param {string} dataset - Dataset name
	 * @param {string} table - Table name
	 * @param {Array<Object>} records - Records to insert
	 * @private
	 */
	async insertRecords(dataset, table, records) {
		if (records.length === 0) return;

		// Use load job API instead of streaming insert to support free tier
		// BigQuery has a limit on request size, so batch the inserts
		const batchSize = 10000;
		const batches = Math.ceil(records.length / batchSize);

		for (let i = 0; i < batches; i++) {
			const start = i * batchSize;
			const end = Math.min(start + batchSize, records.length);
			const batch = records.slice(start, end);

			try {
				// Write records to temporary file
				const tmpFile = path.join(
					os.tmpdir(),
					`bigquery-load-${Date.now()}-${Math.random().toString(36).substr(2, 9)}.json`
				);
				const ndjson = batch.map((record) => JSON.stringify(record)).join('\n');
				fs.writeFileSync(tmpFile, ndjson);

				// Load file into BigQuery using load job API
				await this.bigquery.dataset(dataset).table(table).load(tmpFile, {
					sourceFormat: 'NEWLINE_DELIMITED_JSON',
					writeDisposition: 'WRITE_APPEND',
					autodetect: false,
				});

				// Clean up temp file
				fs.unlinkSync(tmpFile);

				if (batches > 1) {
					const progress = Math.floor((end / records.length) * 100);
					process.stdout.write(`\r  Progress: ${progress}%`);
				}
			} catch (error) {
				console.error(`\n  Error inserting batch ${i + 1}/${batches}:`, error);
				throw error;
			}
		}

		if (batches > 1) {
			process.stdout.write(`\r  Progress: 100%\n`);
		}
	}

	/**
	 * Verifies data was inserted correctly
	 * @param {string} dataset - Dataset name
	 * @returns {Promise<Object>} Verification results
	 */
	async verify(dataset) {
		console.log(`\n=== Verifying Data ===\n`);

		// Map table names to their timestamp columns
		const tableConfigs = {
			vessel_metadata: 'last_updated',
			port_events: 'event_time',
			vessel_positions: 'timestamp',
		};

		const results = {};

		for (const [table, timestampCol] of Object.entries(tableConfigs)) {
			try {
				const [rows] = await this.bigquery.query({
					query: `
            SELECT
              COUNT(*) as count,
              MIN(${timestampCol}) as min_timestamp,
              MAX(${timestampCol}) as max_timestamp
            FROM \`${this.projectId}.${dataset}.${table}\`
          `,
					location: this.location,
				});

				results[table] = rows[0];
				console.log(`  ${table}: ${rows[0].count} records`);
			} catch (error) {
				console.error(`  Error verifying ${table}:`, error.message);
				results[table] = { error: error.message };
			}
		}

		return results;
	}

	/**
	 * Initialize generators for continuous mode
	 * @private
	 */
	initializeGenerators() {
		if (this.generators) return;

		this.generators = {
			positions: new VesselPositionsGenerator({
				startTime: new Date(),
				durationMs: this.config.generationIntervalMs,
				vessels: this.mmsiList.map((mmsi, i) => ({
					mmsi,
					startLat: 37.7749 + (i % 10) * 0.1,
					startLon: -122.4194 + Math.floor(i / 10) * 0.1,
					vesselName: `VESSEL_${mmsi}`,
					vesselType: 'Container Ship',
				})),
			}),
			events: new PortEventsGenerator({
				startTime: new Date(),
				durationMs: this.config.generationIntervalMs,
				mmsiList: this.mmsiList,
			}),
			metadata: new VesselMetadataGenerator({
				startTime: new Date(),
				durationMs: this.config.generationIntervalMs,
				mmsiList: this.mmsiList,
			}),
		};
	}

	/**
	 * Check data range for a specific table
	 * @param {string} dataset - Dataset name
	 * @param {string} table - Table name
	 * @param {string} timestampCol - Timestamp column name
	 * @returns {Promise<Object>} Data range information
	 */
	async checkDataRange(dataset, table, timestampCol) {
		try {
			const query = `
				SELECT
					MIN(${timestampCol}) as oldest,
					MAX(${timestampCol}) as newest,
					COUNT(*) as total_records
				FROM \`${this.projectId}.${dataset}.${table}\`
			`;

			const [rows] = await this.bigquery.query({ query, location: this.location });

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
	 * Start continuous data generation with rolling window support
	 * @param {Object} options - Start options
	 * @param {string} options.dataset - Dataset name
	 * @param {boolean} options.maintainWindow - Whether to maintain rolling window (default: true)
	 * @param {number} options.targetDays - Target days of historical data (default: retentionDays)
	 * @returns {Promise<void>}
	 */
	async start(options = {}) {
		if (this.isRunning) {
			console.log('Service is already running');
			return;
		}

		const dataset = options.dataset || this.config.dataset;
		const maintainWindow = options.maintainWindow !== false;
		const targetDays = options.targetDays || this.config.retentionDays;

		try {
			this.isRunning = true;
			this.stats.startTime = new Date();

			console.log(`\n=== Starting Multi-Table Continuous Generation ===\n`);

			// Initialize generators
			this.initializeGenerators();

			// Create dataset and tables if needed
			await this.createDataset(dataset);
			await this.createTables(dataset);

			// Check and backfill each table if needed
			if (maintainWindow) {
				const tables = [
					{ name: 'vessel_positions', timestampCol: 'timestamp', recordsPerDay: 1440 },
					{ name: 'port_events', timestampCol: 'event_time', recordsPerDay: 100 },
					{ name: 'vessel_metadata', timestampCol: 'last_updated', recordsPerDay: 10 },
				];

				for (const table of tables) {
					console.log(`\nChecking ${table.name} (target: ${targetDays} days)...`);
					const dataRange = await this.checkDataRange(dataset, table.name, table.timestampCol);

					if (!dataRange.hasData) {
						console.log(`  No existing data. Initializing with ${targetDays} days...`);
						await this.backfillTable(dataset, table.name, targetDays, new Date(), table.recordsPerDay);
					} else {
						console.log(`  Found ${dataRange.totalRecords.toLocaleString()} records covering ${dataRange.daysCovered} days`);
						console.log(`    Oldest: ${dataRange.oldestTimestamp.toISOString()}`);
						console.log(`    Newest: ${dataRange.newestTimestamp.toISOString()}`);

						const daysNeeded = targetDays - dataRange.daysCovered;
						if (daysNeeded > 1) {
							console.log(`  Backfilling ${Math.floor(daysNeeded)} days...`);
							await this.backfillTable(dataset, table.name, Math.floor(daysNeeded), dataRange.oldestTimestamp, table.recordsPerDay);
						} else {
							console.log(`  Data window is sufficient (${dataRange.daysCovered}/${targetDays} days)`);
						}
					}
				}
			}

			// Start generation loop
			console.log('\n=== Starting Continuous Generation ===\n');
			await this.generateAndInsertBatch(dataset);
			this.generationTimer = setInterval(() => this.generateAndInsertBatch(dataset), this.config.generationIntervalMs);

			// Start cleanup loop
			setTimeout(() => {
				this.cleanupOldData(dataset);
				this.cleanupTimer = setInterval(
					() => this.cleanupOldData(dataset),
					this.config.cleanupIntervalHours * 60 * 60 * 1000
				);
			}, 60000);

			console.log('Multi-Table Orchestrator started');
			console.log(`  Dataset: ${dataset}`);
			console.log(`  Batch size: ${this.config.batchSize} records per table`);
			console.log(`  Generation interval: ${this.config.generationIntervalMs / 1000} seconds`);
			console.log(`  Rolling window: ${this.config.retentionDays} days`);
			console.log(`  Cleanup interval: ${this.config.cleanupIntervalHours} hours`);
			console.log(`  Tables: vessel_positions, port_events, vessel_metadata`);
		} catch (error) {
			this.isRunning = false;
			console.error('Error starting service:', error);
			throw error;
		}
	}

	/**
	 * Stop continuous generation
	 */
	stop() {
		if (!this.isRunning) {
			console.log('Service is not running');
			return;
		}

		console.log('\nStopping Multi-Table Orchestrator...');

		if (this.generationTimer) {
			clearInterval(this.generationTimer);
			this.generationTimer = null;
		}

		if (this.cleanupTimer) {
			clearInterval(this.cleanupTimer);
			this.cleanupTimer = null;
		}

		this.isRunning = false;

		const runtime = ((Date.now() - this.stats.startTime) / 1000 / 60).toFixed(1);
		console.log(`\nService stopped after ${runtime} minutes`);
		console.log(`  Total batches: ${this.stats.totalBatchesGenerated}`);
		console.log(`  Total records: ${this.stats.totalRecordsInserted.toLocaleString()}`);
		console.log(`  Errors: ${this.stats.errors}`);
	}

	/**
	 * Generate and insert one batch for all tables
	 * @param {string} dataset - Dataset name
	 * @private
	 */
	async generateAndInsertBatch(dataset) {
		try {
			const now = new Date();

			// Generate records for each table
			const positionsRecords = this.generators.positions.generate(this.config.batchSize);
			const eventsRecords = this.generators.events.generate(Math.floor(this.config.batchSize / 10));
			const metadataRecords = this.generators.metadata.generate(Math.floor(this.config.batchSize / 100));

			// Insert in parallel
			await Promise.all([
				this.insertRecords(dataset, 'vessel_positions', positionsRecords),
				this.insertRecords(dataset, 'port_events', eventsRecords),
				this.insertRecords(dataset, 'vessel_metadata', metadataRecords),
			]);

			this.stats.totalBatchesGenerated++;
			this.stats.totalRecordsInserted += positionsRecords.length + eventsRecords.length + metadataRecords.length;

			console.log(
				`[${now.toISOString()}] Batch #${this.stats.totalBatchesGenerated}: ` +
					`${positionsRecords.length} positions, ` +
					`${eventsRecords.length} events, ` +
					`${metadataRecords.length} metadata`
			);
		} catch (error) {
			this.stats.errors++;
			console.error('Error generating batch:', error);
		}
	}

	/**
	 * Backfill historical data for a specific table
	 * @param {string} dataset - Dataset name
	 * @param {string} tableName - Table name
	 * @param {number} days - Number of days to backfill
	 * @param {Date} beforeTimestamp - Backfill before this timestamp
	 * @param {number} recordsPerDay - Average records per day
	 * @private
	 */
	async backfillTable(dataset, tableName, days, beforeTimestamp, recordsPerDay) {
		const totalRecords = recordsPerDay * days;
		const totalBatches = Math.ceil(totalRecords / this.config.batchSize);

		console.log(`  Backfilling ${days} days (~${totalRecords.toLocaleString()} records in ${totalBatches} batches)...`);

		let recordsInserted = 0;
		const startTime = Date.now();
		const oldestTimestamp = beforeTimestamp.getTime();

		// Create temporary generator for backfill
		let generator;
		if (tableName === 'vessel_positions') {
			generator = new VesselPositionsGenerator({
				startTime: new Date(oldestTimestamp - days * 24 * 60 * 60 * 1000),
				durationMs: days * 24 * 60 * 60 * 1000,
				vessels: this.mmsiList.map((mmsi, i) => ({
					mmsi,
					startLat: 37.7749 + (i % 10) * 0.1,
					startLon: -122.4194 + Math.floor(i / 10) * 0.1,
					vesselName: `VESSEL_${mmsi}`,
					vesselType: 'Container Ship',
				})),
			});
		} else if (tableName === 'port_events') {
			generator = new PortEventsGenerator({
				startTime: new Date(oldestTimestamp - days * 24 * 60 * 60 * 1000),
				durationMs: days * 24 * 60 * 60 * 1000,
				mmsiList: this.mmsiList,
			});
		} else if (tableName === 'vessel_metadata') {
			generator = new VesselMetadataGenerator({
				startTime: new Date(oldestTimestamp - days * 24 * 60 * 60 * 1000),
				durationMs: days * 24 * 60 * 60 * 1000,
				mmsiList: this.mmsiList,
			});
		}

		for (let batchNum = 0; batchNum < totalBatches; batchNum++) {
			const batchSize = Math.min(this.config.batchSize, totalRecords - recordsInserted);
			const records = generator.generate(batchSize);

			await this.insertRecords(dataset, tableName, records);
			recordsInserted += records.length;

			if ((batchNum + 1) % 10 === 0 || batchNum === totalBatches - 1) {
				const progress = ((recordsInserted / totalRecords) * 100).toFixed(1);
				const elapsed = (Date.now() - startTime) / 1000;
				const rate = recordsInserted / elapsed;
				const remaining = (totalRecords - recordsInserted) / rate;

				process.stdout.write(
					`\r  Progress: ${progress}% | ${recordsInserted.toLocaleString()}/${totalRecords.toLocaleString()} | ` +
						`Rate: ${Math.floor(rate)} records/sec | ETA: ${Math.ceil(remaining / 60)} min`
				);
			}

			if (batchNum < totalBatches - 1) {
				await new Promise((resolve) => setTimeout(resolve, 1000));
			}
		}

		const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
		console.log(`\n  ✓ Backfilled ${recordsInserted.toLocaleString()} records in ${totalTime} minutes`);
	}

	/**
	 * Clean up old data beyond retention period
	 * @param {string} dataset - Dataset name
	 * @private
	 */
	async cleanupOldData(dataset) {
		console.log(`\n[${new Date().toISOString()}] Running cleanup (retention: ${this.config.retentionDays} days)...`);

		const cutoffDate = new Date(Date.now() - this.config.retentionDays * 24 * 60 * 60 * 1000);

		const tables = [
			{ name: 'vessel_positions', timestampCol: 'timestamp' },
			{ name: 'port_events', timestampCol: 'event_time' },
			{ name: 'vessel_metadata', timestampCol: 'last_updated' },
		];

		for (const table of tables) {
			try {
				const [result] = await this.bigquery.query({
					query: `
						DELETE FROM \`${this.projectId}.${dataset}.${table.name}\`
						WHERE ${table.timestampCol} < TIMESTAMP('${cutoffDate.toISOString()}')
					`,
					location: this.location,
				});

				const numDeleted = result.numDmlAffectedRows || 0;
				if (numDeleted > 0) {
					console.log(`  ${table.name}: Deleted ${numDeleted} old records`);
				} else {
					console.log(`  ${table.name}: No records to delete`);
				}
			} catch (error) {
				console.error(`  Error cleaning ${table.name}:`, error.message);
			}
		}
	}
}

// CLI interface
if (import.meta.url === `file://${process.argv[1]}`) {
	const args = process.argv.slice(2);

	if (args.length < 4) {
		console.log(`
Multi-Table Data Orchestrator

Usage:
  node multi-table-orchestrator.js <projectId> <keyFilename> <dataset> <scenario> [options]

Arguments:
  projectId     - GCP project ID
  keyFilename   - Path to service account key JSON
  dataset       - BigQuery dataset name
  scenario      - Scenario name: small, realistic, or stress

Options:
  --start-time  - Start timestamp (ISO 8601) [default: now]
  --truncate    - Truncate tables before generating data

Examples:
  node multi-table-orchestrator.js my-project ./key.json maritime_tracking realistic
  node multi-table-orchestrator.js my-project ./key.json test_data small --truncate
  node multi-table-orchestrator.js my-project ./key.json prod_data stress --start-time 2024-01-01T00:00:00Z
    `);
		process.exit(1);
	}

	const [projectId, keyFilename, dataset, scenario] = args;

	const options = {
		bigquery: {
			projectId,
			keyFilename,
			location: 'US',
		},
		scenario,
		startTime: args.includes('--start-time') ? args[args.indexOf('--start-time') + 1] : new Date(),
	};

	const orchestrator = new MultiTableOrchestrator(options);

	orchestrator
		.generateAll({
			dataset,
			createDataset: true,
			truncateTables: args.includes('--truncate'),
		})
		.then(() => orchestrator.verify(dataset))
		.then(() => {
			console.log(`\n✓ Complete!\n`);
			process.exit(0);
		})
		.catch((error) => {
			console.error(`\n✗ Failed:`, error);
			process.exit(1);
		});
}

export default MultiTableOrchestrator;

