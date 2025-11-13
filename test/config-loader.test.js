/**
 * Tests for config-loader.js
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { getSynthesizerConfig, getPluginConfig } from '../src/config-loader.js';

describe('Config Loader', () => {
	describe('getSynthesizerConfig', () => {
		it('should use bigquery config as defaults', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					credentials: 'test-key.json',
					location: 'US',
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.strictEqual(config.projectId, 'test-project');
			assert.strictEqual(config.datasetId, 'test_dataset');
			assert.strictEqual(config.tableId, 'test_table');
			assert.strictEqual(config.credentials, 'test-key.json');
			assert.strictEqual(config.location, 'US');
		});

		it('should allow synthesizer overrides for dataset/table', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'default_dataset',
					table: 'default_table',
					credentials: 'test-key.json',
					location: 'US',
				},
				synthesizer: {
					dataset: 'override_dataset',
					table: 'override_table',
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.strictEqual(config.datasetId, 'override_dataset');
			assert.strictEqual(config.tableId, 'override_table');
		});

		it('should use default values for synthesizer settings', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					credentials: 'test-key.json',
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.strictEqual(config.totalVessels, 100000);
			assert.strictEqual(config.batchSize, 100);
			assert.strictEqual(config.generationIntervalMs, 60000);
			assert.strictEqual(config.retentionDays, 30);
			assert.strictEqual(config.cleanupIntervalHours, 24);
		});

		it('should allow custom synthesizer settings', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					credentials: 'test-key.json',
				},
				synthesizer: {
					totalVessels: 50000,
					batchSize: 200,
					generationIntervalMs: 30000,
					retentionDays: 60,
					cleanupIntervalHours: 12,
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.strictEqual(config.totalVessels, 50000);
			assert.strictEqual(config.batchSize, 200);
			assert.strictEqual(config.generationIntervalMs, 30000);
			assert.strictEqual(config.retentionDays, 60);
			assert.strictEqual(config.cleanupIntervalHours, 12);
		});

		it('should not include multiTableConfig when tables array is absent', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					credentials: 'test-key.json',
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.strictEqual(config.multiTableConfig, undefined);
		});

		it('should include multiTableConfig when tables array is present', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: 'test-key.json',
					location: 'US',
					tables: [
						{
							id: 'table1',
							dataset: 'dataset1',
							table: 'table1',
							timestampColumn: 'timestamp',
							columns: ['*'],
							targetTable: 'Table1',
						},
						{
							id: 'table2',
							dataset: 'dataset2',
							table: 'table2',
							timestampColumn: 'event_time',
							columns: ['event_time', 'data'],
							targetTable: 'Table2',
						},
					],
				},
				synthesizer: {
					dataset: 'dataset1',
					table: 'table1',
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.ok(config.multiTableConfig, 'multiTableConfig should be present');
			assert.strictEqual(config.multiTableConfig.length, 2);
			assert.strictEqual(config.multiTableConfig[0].id, 'table1');
			assert.strictEqual(config.multiTableConfig[1].id, 'table2');
		});

		it('should include multiTableConfig even without synthesizer overrides', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: 'test-key.json',
					location: 'US',
					tables: [
						{
							id: 'vessel_positions',
							dataset: 'maritime_tracking',
							table: 'vessel_positions',
							timestampColumn: 'timestamp',
							columns: ['timestamp', 'mmsi'],
							targetTable: 'VesselPositions',
						},
					],
				},
			};

			const config = getSynthesizerConfig(mockConfig);

			assert.ok(config.multiTableConfig, 'multiTableConfig should be present');
			assert.strictEqual(config.multiTableConfig.length, 1);
			assert.strictEqual(config.multiTableConfig[0].id, 'vessel_positions');
		});
	});

	describe('getPluginConfig - Legacy single-table format', () => {
		it('should extract basic BigQuery config from legacy format', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					location: 'US',
				},
			};

			const config = getPluginConfig(mockConfig);

			// getPluginConfig normalizes to multi-table format
			assert.strictEqual(config.bigquery.projectId, 'test-project');
			assert.strictEqual(config.bigquery.credentials, 'test-key.json');
			assert.strictEqual(config.bigquery.location, 'US');
			assert.ok(Array.isArray(config.bigquery.tables));
			assert.strictEqual(config.bigquery.tables.length, 1);
			assert.strictEqual(config.bigquery.tables[0].dataset, 'test_dataset');
			assert.strictEqual(config.bigquery.tables[0].table, 'test_table');
			assert.strictEqual(config.bigquery.tables[0].timestampColumn, 'timestamp');
		});

		it('should default to wildcard columns when not specified', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					location: 'US',
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.deepStrictEqual(config.bigquery.tables[0].columns, ['*']);
		});

		it('should normalize columns array', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					location: 'US',
					columns: ['timestamp', 'mmsi', 'latitude', 'longitude'],
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.deepStrictEqual(config.bigquery.tables[0].columns, ['timestamp', 'mmsi', 'latitude', 'longitude']);
		});

		it('should normalize wildcard string to array', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					location: 'US',
					columns: '*',
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.deepStrictEqual(config.bigquery.tables[0].columns, ['*']);
		});

		it('should throw error when timestamp column not in column list', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					location: 'US',
					columns: ['mmsi', 'latitude', 'longitude'], // missing timestamp
				},
			};

			assert.throws(() => getPluginConfig(mockConfig), {
				message: /Timestamp column 'timestamp' must be included in columns list/,
			});
		});

		it('should throw error for empty columns array', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					location: 'US',
					columns: [],
				},
			};

			assert.throws(() => getPluginConfig(mockConfig), { message: /Column array cannot be empty/ });
		});

		it('should default location to US when not specified', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					dataset: 'test_dataset',
					table: 'test_table',
					timestampColumn: 'timestamp',
					credentials: 'test-key.json',
					// location not specified
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.strictEqual(config.bigquery.location, 'US');
		});
	});

	describe('getPluginConfig - Multi-table format', () => {
		it('should handle multi-table config format', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: 'test-key.json',
					location: 'EU',
					tables: [
						{
							id: 'vessel_positions',
							dataset: 'maritime',
							table: 'positions',
							timestampColumn: 'timestamp',
							targetTable: 'VesselPositions',
							columns: ['timestamp', 'mmsi', 'latitude', 'longitude'],
						},
						{
							id: 'port_events',
							dataset: 'maritime',
							table: 'events',
							timestampColumn: 'event_time',
							targetTable: 'PortEvents',
							columns: ['*'],
						},
					],
				},
				sync: {
					pollInterval: 30000,
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.strictEqual(config.bigquery.projectId, 'test-project');
			assert.strictEqual(config.bigquery.credentials, 'test-key.json');
			assert.strictEqual(config.bigquery.location, 'EU');
			assert.ok(Array.isArray(config.bigquery.tables));
			assert.strictEqual(config.bigquery.tables.length, 2);

			// First table
			assert.strictEqual(config.bigquery.tables[0].id, 'vessel_positions');
			assert.strictEqual(config.bigquery.tables[0].dataset, 'maritime');
			assert.strictEqual(config.bigquery.tables[0].table, 'positions');
			assert.strictEqual(config.bigquery.tables[0].timestampColumn, 'timestamp');
			assert.strictEqual(config.bigquery.tables[0].targetTable, 'VesselPositions');
			assert.deepStrictEqual(config.bigquery.tables[0].columns, ['timestamp', 'mmsi', 'latitude', 'longitude']);

			// Second table
			assert.strictEqual(config.bigquery.tables[1].id, 'port_events');
			assert.strictEqual(config.bigquery.tables[1].dataset, 'maritime');
			assert.strictEqual(config.bigquery.tables[1].table, 'events');
			assert.strictEqual(config.bigquery.tables[1].timestampColumn, 'event_time');
			assert.strictEqual(config.bigquery.tables[1].targetTable, 'PortEvents');
			assert.deepStrictEqual(config.bigquery.tables[1].columns, ['*']);
		});

		it('should normalize wildcard string in multi-table format', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: 'test-key.json',
					location: 'US',
					tables: [
						{
							id: 'vessels',
							dataset: 'maritime',
							table: 'vessels',
							timestampColumn: 'ts',
							targetTable: 'Vessels',
							columns: '*', // String instead of array
						},
					],
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.deepStrictEqual(config.bigquery.tables[0].columns, ['*']);
		});

		it('should validate timestamp column is in columns list for multi-table', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: 'test-key.json',
					tables: [
						{
							id: 'vessels',
							dataset: 'maritime',
							table: 'vessels',
							timestampColumn: 'timestamp',
							targetTable: 'Vessels',
							columns: ['mmsi', 'latitude'], // missing timestamp
						},
					],
				},
			};

			assert.throws(() => getPluginConfig(mockConfig), {
				message: /Timestamp column 'timestamp' must be included in columns list/,
			});
		});

		it('should default location to US in multi-table format', () => {
			const mockConfig = {
				bigquery: {
					projectId: 'test-project',
					credentials: 'test-key.json',
					// location not specified
					tables: [
						{
							id: 'vessels',
							dataset: 'maritime',
							table: 'vessels',
							timestampColumn: 'ts',
							targetTable: 'Vessels',
							columns: ['*'],
						},
					],
				},
			};

			const config = getPluginConfig(mockConfig);

			assert.strictEqual(config.bigquery.location, 'US');
		});
	});
});

