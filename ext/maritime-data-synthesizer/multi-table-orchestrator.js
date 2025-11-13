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

		// Initialize BigQuery client
		this.bigquery = new BigQuery({
			projectId: this.projectId,
			keyFilename: this.keyFilename,
			location: this.location,
		});

		// Generate consistent MMSI list for all tables
		this.mmsiList = this.generateMmsiList();

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

