#!/usr/bin/env node

/**
 * Maritime Vessel Data Synthesizer CLI
 *
 * Command-line tool for generating synthetic maritime test data to BigQuery.
 * Uses the data generators from tools/maritime-data-synthesizer/
 *
 * This is a standalone utility - the main BigQuery sync plugin is in src/
 */

import { MaritimeDataSynthesizer } from '../../src/maritime-synthesizer.js';
import { MultiTableOrchestrator } from './multi-table-orchestrator.js';
import { getSynthesizerConfig } from '../../src/config-loader.js';

const COMMANDS = {
	initialize: 'Initialize BigQuery resources and load historical data',
	start: 'Start continuous data generation',
	stats: 'Display table statistics',
	clear: 'Clear all data from table (keeps schema)',
	clean: 'Delete table and all data',
	reset: 'Delete table and reinitialize with historical data',
	help: 'Show this help message',
};

function showHelp() {
	console.log('\nMaritime Vessel Data Synthesizer CLI\n');
	console.log('Usage: maritime-data-synthesizer <command> [options]\n');
	console.log('Commands:');

	for (const [cmd, desc] of Object.entries(COMMANDS)) {
		console.log(`  ${cmd.padEnd(15)} ${desc}`);
	}

	console.log('\nExamples:');
	console.log('  maritime-data-synthesizer initialize 30    # Load 30 days of historical data');
	console.log('  maritime-data-synthesizer start            # Start with auto-backfill (rolling window)');
	console.log('  maritime-data-synthesizer start --no-backfill  # Start without backfill');
	console.log('  maritime-data-synthesizer stats            # View statistics');
	console.log('  maritime-data-synthesizer clear            # Clear all data (keeps table)');
	console.log('  maritime-data-synthesizer reset 60         # Reset with 60 days of data');
	console.log('\nConfiguration:');
	console.log('  All settings are loaded from config.yaml');
	console.log('  - Uses same BigQuery connection as the plugin (bigquery section)');
	console.log('  - Synthesizer settings in synthesizer section');
	console.log('  - See config.yaml for all available options');
	console.log('');
}

async function main() {
	const command = process.argv[2];
	const arg = process.argv[3];

	if (!command || command === 'help') {
		showHelp();
		process.exit(0);
	}

	if (!COMMANDS[command]) {
		console.error(`Unknown command: ${command}`);
		showHelp();
		process.exit(1);
	}

	try {
		// Load configuration from config.yaml
		const config = getSynthesizerConfig();
		console.log(`Configuration loaded from config.yaml`);
		console.log(`  Project: ${config.projectId}`);
		console.log(`  Dataset: ${config.datasetId}`);
		console.log(`  Table: ${config.tableId}`);
		console.log('');

		// Check if multi-table mode is configured
		// Multi-table mode is used when config has bigquery.tables array
		const isMultiTable = config.multiTableConfig && config.multiTableConfig.length > 0;

		if (isMultiTable) {
			console.log('Multi-table mode detected - using orchestrator');
			console.log(`  Tables: ${config.multiTableConfig.map((t) => t.table).join(', ')}`);
			console.log('');
			await runMultiTableMode(command, arg, config);
		} else {
			console.log('Single-table mode - using legacy synthesizer');
			console.log('');
			await runSingleTableMode(command, arg, config);
		}
	} catch (error) {
		console.error('Error:', error.message);
		if (error.message.includes('config.yaml')) {
			console.error('\nMake sure config.yaml exists and has valid bigquery and synthesizer sections');
		}
		if (error.code === 'ENOENT' && error.message.includes('service-account-key')) {
			console.error('\nMake sure the credentials file specified in config.yaml exists');
		}
		process.exit(1);
	}
}

async function runSingleTableMode(command, arg, config) {
	const synthesizer = new MaritimeDataSynthesizer(config);

	switch (command) {
		case 'initialize': {
			const days = parseInt(arg || '30', 10);
			if (days < 1 || days > 365) {
				console.error('Days must be between 1 and 365');
				process.exit(1);
			}

			console.log(`Initializing with ${days} days of historical data...`);
			await synthesizer.initialize(days);
			console.log('Initialization complete!');
			break;
		}

		case 'start': {
			// Check for optional flags
			const maintainWindow = !process.argv.includes('--no-backfill');
			const targetDays = config.retentionDays;

			console.log('Starting Maritime Data Synthesizer...\n');

			if (maintainWindow) {
				console.log(`Rolling window mode: Will maintain ${targetDays}-day data window`);
				console.log('  - Automatically backfills if data is missing');
				console.log('  - Continuously generates new data');
				console.log('  - Automatically cleans up old data\n');
			} else {
				console.log('Generation-only mode: Will only generate new data (no backfill)\n');
			}

			// Set up event listeners
			synthesizer.on('batch:inserted', () => {
				// Already logged by the service
			});

			synthesizer.on('batch:error', (data) => {
				console.error('Batch error:', data.error.message);
			});

			synthesizer.on('cleanup:completed', (data) => {
				console.log(`Cleanup: deleted ${data.deletedRows} rows older than ${data.cutoffDate}`);
			});

			synthesizer.on('backfill:starting', (data) => {
				console.log(`\nBackfill starting: ${data.days} days before ${data.beforeTimestamp.toISOString()}`);
			});

			synthesizer.on('backfill:completed', (data) => {
				console.log(
					`Backfill completed: ${data.recordsInserted.toLocaleString()} records in ${data.totalTime} minutes\n`
				);
			});

			// Handle shutdown gracefully
			process.on('SIGINT', async () => {
				console.log('\nShutting down...');
				await synthesizer.stop();
				console.log('Service stopped');
				process.exit(0);
			});

			process.on('SIGTERM', async () => {
				console.log('\nShutting down...');
				await synthesizer.stop();
				console.log('Service stopped');
				process.exit(0);
			});

			await synthesizer.start({
				maintainWindow,
				targetDays,
			});

			// Keep the process running
			console.log('\nPress Ctrl+C to stop\n');
			break;
		}

		case 'stats': {
			console.log('Fetching statistics...\n');

			const stats = await synthesizer.getBigQueryStats();

			console.log('Table Metadata:');
			console.log(`  Size: ${(parseInt(stats.tableMetadata.numBytes) / 1024 / 1024).toFixed(2)} MB`);
			console.log(`  Rows: ${parseInt(stats.tableMetadata.numRows).toLocaleString()}`);
			console.log(`  Created: ${new Date(parseInt(stats.tableMetadata.creationTime)).toLocaleString()}`);
			console.log(`  Modified: ${new Date(parseInt(stats.tableMetadata.lastModifiedTime)).toLocaleString()}`);
			console.log('');

			console.log('Data Statistics:');
			console.log(`  Total Records: ${parseInt(stats.statistics.total_records).toLocaleString()}`);
			console.log(`  Unique Vessels: ${parseInt(stats.statistics.unique_vessels).toLocaleString()}`);
			console.log(`  Vessel Types: ${stats.statistics.vessel_types}`);
			console.log(`  Unique Positions: ${parseInt(stats.statistics.unique_positions).toLocaleString()}`);
			console.log(`  Oldest Record: ${stats.statistics.oldest_record?.value || 'N/A'}`);
			console.log(`  Newest Record: ${stats.statistics.newest_record?.value || 'N/A'}`);
			console.log('');

			break;
		}

		case 'clear': {
			console.log('This will clear all data from the table (schema will be preserved).');
			console.log('Are you sure? (Ctrl+C to cancel)');
			await new Promise((resolve) => setTimeout(resolve, 3000));

			console.log('Clearing data...');
			await synthesizer.clear();
			console.log('Clear complete! Table is empty but schema remains.');
			break;
		}

		case 'clean': {
			console.log('This will delete all data and the table. Are you sure? (Ctrl+C to cancel)');
			await new Promise((resolve) => setTimeout(resolve, 3000));

			console.log('Cleaning...');
			await synthesizer.clean();
			console.log('Clean complete!');
			break;
		}

		case 'reset': {
			const days = parseInt(arg || '30', 10);
			if (days < 1 || days > 365) {
				console.error('Days must be between 1 and 365');
				process.exit(1);
			}

			console.log(`This will delete all data and reinitialize with ${days} days. Are you sure? (Ctrl+C to cancel)`);
			await new Promise((resolve) => setTimeout(resolve, 3000));

			console.log('Resetting...');
			await synthesizer.reset(days);
			console.log('Reset complete!');
			break;
		}

		default:
			console.error(`Command not implemented: ${command}`);
			process.exit(1);
	}

	// Exit for non-start commands
	if (command !== 'start') {
		process.exit(0);
	}
}

async function runMultiTableMode(command, arg, config) {
	console.log('\nMulti-table orchestrator mode - generating all configured tables');
	console.log('Note: Only "initialize" command is currently supported for multi-table mode');
	console.log('For continuous generation, use single-table mode\n');

	// TODO: Add rolling window support for multi-table mode
	// Currently, multi-table orchestrator only supports one-time 'initialize' command
	// To add continuous generation with rolling window:
	// 1. Implement maintainWindow logic in MultiTableOrchestrator
	// 2. Add backfill capability (check data range per table)
	// 3. Add continuous generation loop
	// 4. Add automatic cleanup (per-table retention)
	// 5. Support 'start', 'stats', 'clear', 'clean', 'reset' commands
	// See single-table MaritimeDataSynthesizer for reference implementation

	switch (command) {
		case 'initialize': {
			const scenario = arg || 'realistic';
			const validScenarios = ['small', 'realistic', 'stress'];

			if (!validScenarios.includes(scenario)) {
				console.error(`Invalid scenario: ${scenario}`);
				console.error(`Valid scenarios: ${validScenarios.join(', ')}`);
				process.exit(1);
			}

			console.log(`Generating data for scenario: ${scenario}`);

			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: config.projectId,
					keyFilename: config.credentials,
					location: config.location,
				},
				scenario,
				startTime: new Date(),
			});

			await orchestrator.generateAll({
				dataset: config.datasetId,
				createDataset: true,
				truncateTables: false,
			});

			console.log('\nVerifying generated data...');
			await orchestrator.verify(config.datasetId);

			console.log('\nInitialization complete!');

			// Force exit after completion (BigQuery client may keep process alive)
			setTimeout(() => process.exit(0), 100);
			break;
		}

		case 'start':
		case 'stats':
		case 'clear':
		case 'clean':
		case 'reset':
			console.error(`\nCommand "${command}" is not yet supported in multi-table mode.`);
			console.error('Available commands for multi-table mode:');
			console.error('  initialize <scenario>  - Generate test data (scenarios: small, realistic, stress)');
			console.error('\nFor continuous generation, switch to single-table mode in config.yaml');
			process.exit(1);
			break;

		default:
			console.error(`Command not implemented: ${command}`);
			process.exit(1);
	}

	// Note: process.exit(0) is handled in initialize case above with setTimeout
	// to allow BigQuery client to finish cleanup
}

main();
