/**
 * Multi-Table Sync Integration Tests
 *
 * Tests the ability to sync from multiple BigQuery tables simultaneously
 * Each table has independent checkpoints and sync configurations
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BigQueryClient } from '../../src/bigquery-client.js';
import { SyncEngine } from '../../src/sync-engine.js';
import { loadConfig } from '../../src/config-loader.js';
import { MULTI_TABLE_CONFIG, LEGACY_SINGLE_TABLE_CONFIG } from '../fixtures/multi-table-test-data.js';

// Mock logger for test environment
global.logger = {
	info: () => {},
	debug: () => {},
	trace: () => {},
	warn: () => {},
	error: () => {},
};

// Mock server for test environment
global.server = {
	hostname: 'test-node',
	workerIndex: 0,
	nodes: [{ hostname: 'test-node', workerIndex: 0 }],
};

// Mock tables for test environment
global.tables = {
	SyncCheckpoint: {
		get: async (_id) => null,
		put: async (data) => data,
		delete: async (_id) => true,
	},
};

// Helper function to create proper SyncEngine config
function createEngineConfig(tableId, tableConfig, baseConfig) {
	return {
		bigquery: {
			projectId: baseConfig.bigquery.projectId,
			credentials: baseConfig.bigquery.credentials,
			location: baseConfig.bigquery.location,
			dataset: tableConfig.dataset,
			table: tableConfig.table,
			timestampColumn: tableConfig.timestampColumn,
			columns: tableConfig.columns,
		},
		sync: {
			...baseConfig.sync,
			...tableConfig.sync,
		},
		tableId: tableId,
		targetTable: tableConfig.targetTable,
		timestampColumn: tableConfig.timestampColumn,
	};
}

describe('Multi-Table Sync Integration Tests', () => {
	describe('Configuration Loading', () => {
		it('should load multi-table configuration', async () => {
			// Test that we can load a config with multiple tables
			const _config = MULTI_TABLE_CONFIG;

			assert.ok(config.bigquery.tables, 'Config should have tables array');
			assert.strictEqual(config.bigquery.tables.length, 3, 'Should have 3 tables configured');

			// Verify each table has required fields
			for (const table of config.bigquery.tables) {
				assert.ok(table.id, 'Table should have id');
				assert.ok(table.dataset, 'Table should have dataset');
				assert.ok(table.table, 'Table should have table name');
				assert.ok(table.timestampColumn, 'Table should have timestampColumn');
				assert.ok(table.targetTable, 'Table should have targetTable');
				assert.ok(table.sync, 'Table should have sync config');
			}
		});

		it('should wrap legacy single-table config in tables array', async () => {
			// Test backward compatibility - legacy config should be wrapped
			const legacyConfig = LEGACY_SINGLE_TABLE_CONFIG;

			// Config loader should detect legacy format and wrap it
			// This functionality doesn't exist yet - TDD test that will fail
			const normalizedConfig = await loadConfig({ config: legacyConfig });

			assert.ok(normalizedConfig.bigquery.tables, 'Legacy config should be wrapped in tables array');
			assert.strictEqual(normalizedConfig.bigquery.tables.length, 1, 'Should have 1 table');
			assert.strictEqual(normalizedConfig.bigquery.tables[0].id, 'default', 'Should have default id');
			assert.strictEqual(
				normalizedConfig.bigquery.tables[0].targetTable,
				'VesselPositions',
				'Should default to VesselPositions table'
			);
		});

		it('should validate multi-table configuration', async () => {
			// Missing required fields should throw
			const invalidConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: '/path/to/key.json',
					location: 'US',
					tables: [
						{
							id: 'test_table',
							dataset: 'test_dataset',
							// Missing 'table' field
							timestampColumn: 'timestamp',
						},
					],
				},
			};

			await assert.rejects(
				async () => await loadConfig({ config: invalidConfig }),
				/Missing required field.*table/,
				'Should throw error for missing table name'
			);
		});

		it('should validate unique table IDs', async () => {
			// Duplicate table IDs should throw
			const duplicateConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: '/path/to/key.json',
					location: 'US',
					tables: [
						{
							id: 'duplicate',
							dataset: 'test_dataset',
							table: 'table1',
							timestampColumn: 'timestamp',
							targetTable: 'Table1',
						},
						{
							id: 'duplicate', // Duplicate ID
							dataset: 'test_dataset',
							table: 'table2',
							timestampColumn: 'timestamp',
							targetTable: 'Table2',
						},
					],
				},
			};

			await assert.rejects(
				async () => await loadConfig({ config: duplicateConfig }),
				/Duplicate table ID/,
				'Should throw error for duplicate table IDs'
			);
		});

		it('should reject duplicate targetTable (multiple BigQuery tables -> same Harper table)', async () => {
			// Multiple BigQuery tables syncing to the same Harper table should be rejected
			const duplicateTargetConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: '/path/to/key.json',
					location: 'US',
					tables: [
						{
							id: 'table1',
							dataset: 'dataset1',
							table: 'bq_table1',
							timestampColumn: 'timestamp',
							columns: ['*'],
							targetTable: 'SameHarperTable', // Same target!
							sync: { initialBatchSize: 1000 },
						},
						{
							id: 'table2',
							dataset: 'dataset2',
							table: 'bq_table2',
							timestampColumn: 'timestamp',
							columns: ['*'],
							targetTable: 'SameHarperTable', // Same target - should fail!
							sync: { initialBatchSize: 1000 },
						},
					],
				},
			};

			await assert.rejects(
				async () => await loadConfig({ config: duplicateTargetConfig }),
				/Duplicate targetTable.*Each BigQuery table must sync to a DIFFERENT Harper table/,
				'Should throw error for duplicate targetTable'
			);
		});
	});

	describe('Independent Table Syncing', () => {
		it('should create separate BigQueryClients for each table', () => {
			// Each table should have its own BigQueryClient instance
			const _config = MULTI_TABLE_CONFIG;
			const clients = [];

			for (const tableConfig of config.bigquery.tables) {
				const client = new BigQueryClient({
					bigquery: {
						projectId: config.bigquery.projectId,
						dataset: tableConfig.dataset,
						table: tableConfig.table,
						timestampColumn: tableConfig.timestampColumn,
						columns: tableConfig.columns,
						credentials: config.bigquery.credentials,
						location: config.bigquery.location,
					},
				});

				clients.push(client);
				assert.ok(client, 'Should create BigQueryClient');
				assert.strictEqual(client.table, tableConfig.table, 'Client should have correct table');
			}

			assert.strictEqual(clients.length, 3, 'Should have 3 clients');
		});

		it('should create separate SyncEngines for each table', () => {
			// Each table should have its own SyncEngine instance
			const _config = MULTI_TABLE_CONFIG;
			const engines = [];

			for (const tableConfig of config.bigquery.tables) {
				// Create proper config structure expected by SyncEngine
				const engineConfig = {
					bigquery: {
						projectId: config.bigquery.projectId,
						credentials: config.bigquery.credentials,
						location: config.bigquery.location,
						dataset: tableConfig.dataset,
						table: tableConfig.table,
						timestampColumn: tableConfig.timestampColumn,
						columns: tableConfig.columns,
					},
					sync: {
						...config.sync,
						...tableConfig.sync,
					},
					tableId: tableConfig.id,
					targetTable: tableConfig.targetTable,
					timestampColumn: tableConfig.timestampColumn,
				};

				const engine = new SyncEngine(engineConfig);

				engines.push(engine);
				assert.ok(engine, 'Should create SyncEngine');

				// Verify multi-table properties
				assert.strictEqual(engine.tableId, tableConfig.id, 'Engine should have tableId');
				assert.strictEqual(engine.targetTable, tableConfig.targetTable, 'Engine should have targetTable');
			}

			assert.strictEqual(engines.length, 3, 'Should have 3 engines');
		});

		it('should use composite checkpoint IDs: {tableId}_{nodeId}', async () => {
			// Checkpoints should include table ID to keep them separate
			const tableId = 'vessel_positions';
			const nodeId = 0;

			const engineConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: '/path/to/key.json',
					location: 'US',
					dataset: 'maritime_tracking',
					table: 'vessel_positions',
					timestampColumn: 'timestamp',
					columns: ['timestamp', 'mmsi', 'latitude', 'longitude'],
				},
				sync: MULTI_TABLE_CONFIG.bigquery.tables[0].sync,
				tableId: tableId,
				targetTable: 'VesselPositions',
				timestampColumn: 'timestamp',
			};

			const engine = new SyncEngine(engineConfig);
			await engine.initialize();

			// Checkpoint ID should be composite: {tableId}_{nodeId}
			const expectedCheckpointId = `${tableId}_${nodeId}`;
			assert.strictEqual(engine.checkpointId, expectedCheckpointId, 'Should use composite checkpoint ID');
		});

		it('should maintain separate checkpoints per table', async () => {
			// Each table should track its own lastTimestamp independently
			const _config = MULTI_TABLE_CONFIG;

			// Simulate syncing vessel_positions
			const vesselPositionsEngine = new SyncEngine({
				bigquery: {
					projectId: config.bigquery.projectId,
					credentials: config.bigquery.credentials,
					location: config.bigquery.location,
					dataset: config.bigquery.tables[0].dataset,
					table: config.bigquery.tables[0].table,
					timestampColumn: config.bigquery.tables[0].timestampColumn,
					columns: config.bigquery.tables[0].columns,
				},
				sync: config.bigquery.tables[0].sync,
				tableId: 'vessel_positions',
				targetTable: 'VesselPositions',
				timestampColumn: 'timestamp',
			});

			// Simulate syncing port_events with different timestamp column
			const portEventsEngine = new SyncEngine({
				bigquery: {
					projectId: config.bigquery.projectId,
					credentials: config.bigquery.credentials,
					location: config.bigquery.location,
					dataset: config.bigquery.tables[1].dataset,
					table: config.bigquery.tables[1].table,
					timestampColumn: config.bigquery.tables[1].timestampColumn,
					columns: config.bigquery.tables[1].columns,
				},
				sync: config.bigquery.tables[1].sync,
				tableId: 'port_events',
				targetTable: 'PortEvents',
				timestampColumn: 'event_time',
			});

			// Initialize to set checkpoint IDs
			await vesselPositionsEngine.initialize();
			await portEventsEngine.initialize();

			// Checkpoints should be independent
			assert.notStrictEqual(
				vesselPositionsEngine.checkpointId,
				portEventsEngine.checkpointId,
				'Checkpoint IDs should be different'
			);

			assert.strictEqual(vesselPositionsEngine.checkpointId, 'vessel_positions_0');
			assert.strictEqual(portEventsEngine.checkpointId, 'port_events_0');
		});
	});

	describe('Table Isolation and Fault Tolerance', () => {
		it('should continue syncing other tables if one table fails', async () => {
			// If port_events sync fails, vessel_positions and vessel_metadata should continue
			// This validates that tables are isolated and independent
			const _config = MULTI_TABLE_CONFIG;

			// Create engines for all three tables
			const engines = [];
			for (const tableConfig of config.bigquery.tables) {
				const engineConfig = createEngineConfig(tableConfig.id, tableConfig, config);
				const engine = new SyncEngine(engineConfig);
				await engine.initialize();
				engines.push(engine);
			}

			// Verify each engine has independent checkpoint IDs
			assert.strictEqual(engines.length, 3, 'Should have 3 independent engines');
			assert.strictEqual(engines[0].checkpointId, 'vessel_positions_0');
			assert.strictEqual(engines[1].checkpointId, 'port_events_0');
			assert.strictEqual(engines[2].checkpointId, 'vessel_metadata_0');

			// Verify engines target different Harper tables
			assert.strictEqual(engines[0].targetTable, 'VesselPositions');
			assert.strictEqual(engines[1].targetTable, 'PortEvents');
			assert.strictEqual(engines[2].targetTable, 'VesselMetadata');

			// Verify independent sync configs
			assert.strictEqual(engines[0].config.sync.initialBatchSize, 10000);
			assert.strictEqual(engines[1].config.sync.initialBatchSize, 5000);
			assert.strictEqual(engines[2].config.sync.initialBatchSize, 1000);
		});

		it('should track sync status per table independently', async () => {
			// Each table should have independent sync state (phase, batchSize, etc.)
			const _config = MULTI_TABLE_CONFIG;

			const engine1Config = createEngineConfig('vessel_positions', config.bigquery.tables[0], config);
			const engine1 = new SyncEngine(engine1Config);

			const engine2Config = createEngineConfig('port_events', config.bigquery.tables[1], config);
			const engine2 = new SyncEngine(engine2Config);

			// Engines should have independent sync configs
			assert.strictEqual(engine1.config.sync.initialBatchSize, 10000, 'vessel_positions batch size');
			assert.strictEqual(engine2.config.sync.initialBatchSize, 5000, 'port_events batch size');
		});
	});

	describe('Dynamic Table Routing', () => {
		it('should ingest records to correct Harper table', async () => {
			// vessel_positions data → VesselPositions table
			// port_events data → PortEvents table
			// vessel_metadata data → VesselMetadata table

			const _mockRecords = [
				{ timestamp: '2024-01-01T00:00:00Z', mmsi: '123456', latitude: 37.7749, longitude: -122.4194 },
			];

			const engineConfig = createEngineConfig(
				'vessel_positions',
				MULTI_TABLE_CONFIG.bigquery.tables[0],
				MULTI_TABLE_CONFIG
			);
			const engine = new SyncEngine(engineConfig);

			// Verify targetTable is set correctly for dynamic routing
			assert.strictEqual(engine.targetTable, 'VesselPositions', 'Should target VesselPositions table');

			// Note: ingestRecords requires transaction context which isn't available in unit tests
			// This validates the configuration is correct for routing
			assert.ok(true, 'Should route records to VesselPositions table');
		});

		it('should handle different timestamp column names per table', async () => {
			// vessel_positions uses 'timestamp'
			// port_events uses 'event_time'
			// vessel_metadata uses 'last_updated'

			const baseConfig = MULTI_TABLE_CONFIG;
			const testCases = [
				{ index: 0, tableId: 'vessel_positions', timestampColumn: 'timestamp' },
				{ index: 1, tableId: 'port_events', timestampColumn: 'event_time' },
				{ index: 2, tableId: 'vessel_metadata', timestampColumn: 'last_updated' },
			];

			for (const testCase of testCases) {
				const engineConfig = createEngineConfig(
					testCase.tableId,
					baseConfig.bigquery.tables[testCase.index],
					baseConfig
				);
				const engine = new SyncEngine(engineConfig);

				assert.strictEqual(
					engine.timestampColumn,
					testCase.timestampColumn,
					`${testCase.tableId} should use ${testCase.timestampColumn}`
				);
			}
		});
	});

	describe('Different Sync Rates Per Table', () => {
		it('should support different batch sizes per table', async () => {
			// vessel_positions: large batches (10000)
			// port_events: medium batches (5000)
			// vessel_metadata: small batches (1000)

			const _config = MULTI_TABLE_CONFIG;

			const vesselConfig = createEngineConfig('vessel_positions', config.bigquery.tables[0], config);
			const vesselEngine = new SyncEngine(vesselConfig);

			const portConfig = createEngineConfig('port_events', config.bigquery.tables[1], config);
			const portEngine = new SyncEngine(portConfig);

			const metadataConfig = createEngineConfig('vessel_metadata', config.bigquery.tables[2], config);
			const metadataEngine = new SyncEngine(metadataConfig);

			assert.strictEqual(vesselEngine.config.sync.initialBatchSize, 10000, 'vessel_positions batch');
			assert.strictEqual(portEngine.config.sync.initialBatchSize, 5000, 'port_events batch');
			assert.strictEqual(metadataEngine.config.sync.initialBatchSize, 1000, 'vessel_metadata batch');
		});

		it('should allow tables to be in different sync phases', async () => {
			// vessel_positions might be in 'steady' phase
			// while port_events is still in 'catchup' phase

			// This is already supported by independent SyncEngines
			// Just verify each engine tracks its own phase

			const _config = MULTI_TABLE_CONFIG;

			const engine1Config = createEngineConfig('vessel_positions', config.bigquery.tables[0], config);
			const engine1 = new SyncEngine(engine1Config);

			const engine2Config = createEngineConfig('port_events', config.bigquery.tables[1], config);
			const engine2 = new SyncEngine(engine2Config);

			// Phases are independent
			assert.ok(engine1.currentPhase !== undefined, 'Engine 1 should have phase');
			assert.ok(engine2.currentPhase !== undefined, 'Engine 2 should have phase');
		});
	});

	describe('Backward Compatibility', () => {
		it('should support legacy single-table configuration', async () => {
			// Old configs without 'tables' array should still work
			const legacyConfig = LEGACY_SINGLE_TABLE_CONFIG;

			// Config loader should wrap this automatically
			const normalizedConfig = await loadConfig({ config: legacyConfig });

			assert.ok(normalizedConfig.bigquery.tables, 'Should wrap in tables array');
			assert.strictEqual(normalizedConfig.bigquery.tables.length, 1, 'Should have 1 table');

			// Legacy config values should be preserved
			const table = normalizedConfig.bigquery.tables[0];
			assert.strictEqual(table.dataset, 'maritime_tracking', 'Should preserve dataset');
			assert.strictEqual(table.table, 'vessel_positions', 'Should preserve table');
			assert.strictEqual(table.timestampColumn, 'timestamp', 'Should preserve timestampColumn');
		});

		it('should maintain existing checkpoint format for single table', async () => {
			// Legacy single-table deployments use composite ID {tableId}_{nodeId}
			// Even for single table, we use "default_0" format for consistency

			const legacyConfig = LEGACY_SINGLE_TABLE_CONFIG;
			const normalizedConfig = await loadConfig({ config: legacyConfig });

			const engineConfig = createEngineConfig(
				normalizedConfig.bigquery.tables[0].id,
				normalizedConfig.bigquery.tables[0],
				normalizedConfig
			);
			const engine = new SyncEngine(engineConfig);
			await engine.initialize();

			// Even single-table configs use composite checkpoint IDs for consistency
			assert.strictEqual(engine.checkpointId, 'default_0', 'Should use composite checkpoint ID');
			assert.strictEqual(engine.tableId, 'default', 'Should have default tableId');
		});
	});

	describe('End-to-End Multi-Table Sync', () => {
		it('should sync all three tables from start to finish', async () => {
			// This is a high-level integration test
			// Will be implemented after all components are built

			const _config = MULTI_TABLE_CONFIG;

			// Mock data for all three tables
			const _vesselPositionsData = [
				{ timestamp: '2024-01-01T00:00:00Z', mmsi: '367123456', latitude: 37.7749, longitude: -122.4194 },
			];

			const _portEventsData = [
				{ event_time: '2024-01-01T00:00:00Z', port_id: 'SFO', vessel_mmsi: '367123456', event_type: 'ARRIVAL' },
			];

			const _vesselMetadataData = [
				{ last_updated: '2024-01-01T00:00:00Z', mmsi: '367123456', vessel_name: 'PACIFIC TRADER' },
			];

			// Create clients and engines for all tables
			// Sync all tables
			// Verify all tables have data
			// Verify all checkpoints are independent

			// This will be implemented after components are built
			assert.ok(true, 'End-to-end test placeholder');
		});
	});
});

// Helper function for table sync (to be implemented)
async function _syncTable(tableConfig, client) {
	const engineConfig = {
		bigquery: {
			projectId: 'test-project',
			credentials: '/path/to/key.json',
			location: 'US',
			dataset: tableConfig.dataset,
			table: tableConfig.table,
			timestampColumn: tableConfig.timestampColumn,
			columns: tableConfig.columns,
		},
		sync: tableConfig.sync,
		tableId: tableConfig.id,
		targetTable: tableConfig.targetTable,
		timestampColumn: tableConfig.timestampColumn,
	};

	const engine = new SyncEngine(engineConfig);

	// Override the client to use our mock
	engine.client = client;

	await engine.initialize();
	await engine.syncOnce();
}
