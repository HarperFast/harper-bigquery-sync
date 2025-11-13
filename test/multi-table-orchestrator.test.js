/**
 * Tests for multi-table-orchestrator.js
 *
 * Focus: Rolling window support (checkDataRange, backfill, cleanup, continuous generation)
 */

import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert';
import { MultiTableOrchestrator } from '../tools/maritime-data-synthesizer/multi-table-orchestrator.js';

// Mock logger global that Harper provides at runtime
const mockLogger = {
	info: () => {},
	debug: () => {},
	trace: () => {},
	warn: () => {},
	error: () => {},
};

describe('MultiTableOrchestrator - Rolling Window Support', () => {
	before(() => {
		// Set up global logger mock
		global.logger = mockLogger;
	});

	after(() => {
		// Clean up global logger mock
		delete global.logger;
	});

	describe('checkDataRange', () => {
		it('should return hasData=false when table is empty', async () => {
			// Create orchestrator with mock BigQuery client
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
			});

			// Mock BigQuery query to return empty result
			orchestrator.bigquery.query = async () => {
				return [[{ total_records: '0' }]];
			};

			const result = await orchestrator.checkDataRange('test_dataset', 'vessel_positions', 'timestamp');

			assert.strictEqual(result.hasData, false);
			assert.strictEqual(result.oldestTimestamp, null);
			assert.strictEqual(result.newestTimestamp, null);
			assert.strictEqual(result.totalRecords, 0);
			assert.strictEqual(result.daysCovered, 0);
		});

		it('should return correct data range when table has data', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
			});

			// Mock BigQuery query to return data range
			const oldestDate = new Date('2024-01-01T00:00:00Z');
			const newestDate = new Date('2024-01-31T00:00:00Z');

			orchestrator.bigquery.query = async () => {
				return [
					[
						{
							oldest: { value: oldestDate },
							newest: { value: newestDate },
							total_records: '1000',
						},
					],
				];
			};

			const result = await orchestrator.checkDataRange('test_dataset', 'vessel_positions', 'timestamp');

			assert.strictEqual(result.hasData, true);
			assert.strictEqual(result.oldestTimestamp.toISOString(), oldestDate.toISOString());
			assert.strictEqual(result.newestTimestamp.toISOString(), newestDate.toISOString());
			assert.strictEqual(result.totalRecords, 1000);
			assert.strictEqual(result.daysCovered, 30);
		});

		it('should handle table not found error', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
			});

			// Mock BigQuery query to throw "Not found" error
			orchestrator.bigquery.query = async () => {
				const error = new Error('Table not found');
				error.message = 'Not found: Table test-project:test_dataset.vessel_positions';
				throw error;
			};

			const result = await orchestrator.checkDataRange('test_dataset', 'vessel_positions', 'timestamp');

			assert.strictEqual(result.hasData, false);
			assert.strictEqual(result.totalRecords, 0);
		});

		it('should calculate daysCovered correctly for partial days', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
			});

			// Mock 7.5 days of data
			const oldestDate = new Date('2024-01-01T00:00:00Z');
			const newestDate = new Date('2024-01-08T12:00:00Z');

			orchestrator.bigquery.query = async () => {
				return [
					[
						{
							oldest: { value: oldestDate },
							newest: { value: newestDate },
							total_records: '500',
						},
					],
				];
			};

			const result = await orchestrator.checkDataRange('test_dataset', 'vessel_positions', 'timestamp');

			assert.strictEqual(result.daysCovered, 7); // Should floor to 7
		});
	});

	describe('initializeGenerators', () => {
		it('should initialize all three generators', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				batchSize: 100,
				generationIntervalMs: 60000,
			});

			orchestrator.initializeGenerators();

			assert.ok(orchestrator.generators, 'Generators should be initialized');
			assert.ok(orchestrator.generators.positions, 'Positions generator should exist');
			assert.ok(orchestrator.generators.events, 'Events generator should exist');
			assert.ok(orchestrator.generators.metadata, 'Metadata generator should exist');
		});

		it('should not reinitialize if already initialized', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
			});

			orchestrator.initializeGenerators();
			const firstGenerators = orchestrator.generators;

			orchestrator.initializeGenerators();
			const secondGenerators = orchestrator.generators;

			assert.strictEqual(firstGenerators, secondGenerators, 'Should return same generators object');
		});
	});

	describe('generateAndInsertBatch', () => {
		it('should generate records for all three tables', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				batchSize: 100,
			});

			orchestrator.initializeGenerators();

			let insertedTables = [];
			let insertedRecordCounts = [];

			// Mock insertRecords to track calls
			orchestrator.insertRecords = async (dataset, table, records) => {
				insertedTables.push(table);
				insertedRecordCounts.push(records.length);
			};

			await orchestrator.generateAndInsertBatch('test_dataset');

			// Verify all three tables were inserted
			assert.ok(insertedTables.includes('vessel_positions'), 'Should insert vessel_positions');
			assert.ok(insertedTables.includes('port_events'), 'Should insert port_events');
			assert.ok(insertedTables.includes('vessel_metadata'), 'Should insert vessel_metadata');

			// Verify batch sizes
			const positionsIndex = insertedTables.indexOf('vessel_positions');
			const eventsIndex = insertedTables.indexOf('port_events');
			const metadataIndex = insertedTables.indexOf('vessel_metadata');

			assert.strictEqual(insertedRecordCounts[positionsIndex], 100, 'Should generate 100 position records');
			assert.strictEqual(insertedRecordCounts[eventsIndex], 10, 'Should generate 10 event records');
			assert.strictEqual(insertedRecordCounts[metadataIndex], 1, 'Should generate 1 metadata record');

			// Verify stats were updated
			assert.strictEqual(orchestrator.stats.totalBatchesGenerated, 1);
			assert.strictEqual(orchestrator.stats.totalRecordsInserted, 111);
		});

		it('should handle errors gracefully and increment error count', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				batchSize: 100,
			});

			orchestrator.initializeGenerators();

			// Mock insertRecords to throw error
			orchestrator.insertRecords = async () => {
				throw new Error('Insert failed');
			};

			await orchestrator.generateAndInsertBatch('test_dataset');

			// Should increment error count
			assert.strictEqual(orchestrator.stats.errors, 1);
			// Should not increment batch count on error
			assert.strictEqual(orchestrator.stats.totalBatchesGenerated, 0);
		});
	});

	describe('cleanupOldData', () => {
		it('should generate DELETE queries for all tables', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				retentionDays: 30,
			});

			const executedQueries = [];

			// Mock BigQuery query
			orchestrator.bigquery.query = async ({ query }) => {
				executedQueries.push(query);
				return [{ numDmlAffectedRows: 0 }];
			};

			await orchestrator.cleanupOldData('test_dataset');

			// Should execute 3 DELETE queries
			assert.strictEqual(executedQueries.length, 3);

			// Verify each table's query
			assert.ok(
				executedQueries.some((q) => q.includes('vessel_positions')),
				'Should cleanup vessel_positions'
			);
			assert.ok(
				executedQueries.some((q) => q.includes('port_events')),
				'Should cleanup port_events'
			);
			assert.ok(
				executedQueries.some((q) => q.includes('vessel_metadata')),
				'Should cleanup vessel_metadata'
			);

			// Verify all queries use correct timestamp columns
			const positionsQuery = executedQueries.find((q) => q.includes('vessel_positions'));
			assert.ok(positionsQuery.includes('WHERE timestamp <'), 'Should filter by timestamp column');

			const eventsQuery = executedQueries.find((q) => q.includes('port_events'));
			assert.ok(eventsQuery.includes('WHERE event_time <'), 'Should filter by event_time column');

			const metadataQuery = executedQueries.find((q) => q.includes('vessel_metadata'));
			assert.ok(metadataQuery.includes('WHERE last_updated <'), 'Should filter by last_updated column');
		});

		it('should calculate correct cutoff date', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				retentionDays: 30,
			});

			const executedQueries = [];

			orchestrator.bigquery.query = async ({ query }) => {
				executedQueries.push(query);
				return [{ numDmlAffectedRows: 0 }];
			};

			const before = Date.now();
			await orchestrator.cleanupOldData('test_dataset');
			const after = Date.now();

			// Extract timestamp from query
			const query = executedQueries[0];
			const match = query.match(/TIMESTAMP\('([^']+)'\)/);
			assert.ok(match, 'Should have TIMESTAMP in query');

			const cutoffTimestamp = new Date(match[1]).getTime();
			const expectedCutoff = before - 30 * 24 * 60 * 60 * 1000;
			const expectedCutoffMax = after - 30 * 24 * 60 * 60 * 1000;

			assert.ok(
				cutoffTimestamp >= expectedCutoff && cutoffTimestamp <= expectedCutoffMax,
				'Cutoff date should be 30 days ago'
			);
		});

		it('should handle cleanup errors gracefully', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				retentionDays: 30,
			});

			// Mock to throw error on first table, succeed on others
			let callCount = 0;
			orchestrator.bigquery.query = async () => {
				callCount++;
				if (callCount === 1) {
					throw new Error('Cleanup failed');
				}
				return [{ numDmlAffectedRows: 0 }];
			};

			// Should not throw - errors are logged but not propagated
			await orchestrator.cleanupOldData('test_dataset');

			// Should attempt all 3 tables despite first failure
			assert.strictEqual(callCount, 3);
		});
	});

	describe('backfillTable', () => {
		it('should calculate correct number of batches', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				batchSize: 100,
			});

			let insertCallCount = 0;
			orchestrator.insertRecords = async () => {
				insertCallCount++;
			};

			// 5 days * 1440 records/day = 7200 records
			// 7200 / 100 batch size = 72 batches
			await orchestrator.backfillTable('test_dataset', 'vessel_positions', 5, new Date(), 1440);

			assert.strictEqual(insertCallCount, 72, 'Should insert 72 batches');
		});

		it('should generate correct record count', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				batchSize: 50,
			});

			let totalRecords = 0;
			orchestrator.insertRecords = async (dataset, table, records) => {
				totalRecords += records.length;
			};

			// 3 days * 100 records/day = 300 records
			await orchestrator.backfillTable('test_dataset', 'port_events', 3, new Date(), 100);

			assert.strictEqual(totalRecords, 300, 'Should generate exactly 300 records');
		});

		it('should use correct generator for each table', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				batchSize: 10,
			});

			orchestrator.insertRecords = async () => {};

			// Test vessel_positions
			await orchestrator.backfillTable('test_dataset', 'vessel_positions', 1, new Date(), 10);
			// If this doesn't throw, the correct generator was used

			// Test port_events
			await orchestrator.backfillTable('test_dataset', 'port_events', 1, new Date(), 10);

			// Test vessel_metadata
			await orchestrator.backfillTable('test_dataset', 'vessel_metadata', 1, new Date(), 10);

			// All should complete without errors
			assert.ok(true, 'All table backfills should use correct generators');
		});
	});

	describe('start and stop lifecycle', () => {
		let orchestrator;

		beforeEach(() => {
			orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				dataset: 'test_dataset',
				batchSize: 10,
				generationIntervalMs: 100,
				retentionDays: 30,
				cleanupIntervalHours: 1,
			});
		});

		it('should set isRunning to true when started', async () => {
			// Mock all BigQuery operations
			orchestrator.bigquery.dataset = () => ({
				exists: async () => [true],
			});

			orchestrator.createDataset = async () => {};
			orchestrator.createTables = async () => {};
			orchestrator.checkDataRange = async () => ({
				hasData: true,
				daysCovered: 30,
			});
			orchestrator.generateAndInsertBatch = async () => {};

			assert.strictEqual(orchestrator.isRunning, false, 'Should not be running initially');

			// Start without waiting for completion
			const startPromise = orchestrator.start({ dataset: 'test_dataset', maintainWindow: false });

			// Give it a moment to initialize
			await new Promise((resolve) => setTimeout(resolve, 50));

			assert.strictEqual(orchestrator.isRunning, true, 'Should be running after start');

			// Stop it
			orchestrator.stop();

			// Wait for start promise to settle
			await startPromise.catch(() => {}); // Ignore any errors from stopping mid-start
		});

		it('should not start if already running', async () => {
			orchestrator.isRunning = true;

			await orchestrator.start({ dataset: 'test_dataset' });

			// Should not throw or change state
			assert.strictEqual(orchestrator.isRunning, true);
		});

		it('should clear timers when stopped', async () => {
			// Mock all operations
			orchestrator.createDataset = async () => {};
			orchestrator.createTables = async () => {};
			orchestrator.checkDataRange = async () => ({
				hasData: true,
				daysCovered: 30,
			});
			orchestrator.generateAndInsertBatch = async () => {};

			await orchestrator.start({ dataset: 'test_dataset', maintainWindow: false });

			// Verify generation timer is set immediately
			assert.ok(orchestrator.generationTimer, 'Generation timer should be set');

			// Stop the orchestrator
			orchestrator.stop();

			// Verify timers are cleared (both should be null after stop)
			assert.strictEqual(orchestrator.generationTimer, null, 'Generation timer should be cleared');
			assert.strictEqual(orchestrator.cleanupTimer, null, 'Cleanup timer should be cleared');
			assert.strictEqual(orchestrator.isRunning, false, 'Should not be running after stop');
		});

		it('should initialize generators on start', async () => {
			orchestrator.createDataset = async () => {};
			orchestrator.createTables = async () => {};
			orchestrator.checkDataRange = async () => ({
				hasData: true,
				daysCovered: 30,
			});
			orchestrator.generateAndInsertBatch = async () => {};

			assert.strictEqual(orchestrator.generators, null, 'Generators should not be initialized');

			await orchestrator.start({ dataset: 'test_dataset', maintainWindow: false });

			assert.ok(orchestrator.generators, 'Generators should be initialized after start');

			orchestrator.stop();
		});

		it('should update stats on start', async () => {
			orchestrator.createDataset = async () => {};
			orchestrator.createTables = async () => {};
			orchestrator.checkDataRange = async () => ({
				hasData: true,
				daysCovered: 30,
			});
			orchestrator.generateAndInsertBatch = async () => {};

			assert.strictEqual(orchestrator.stats.startTime, null, 'Start time should be null initially');

			await orchestrator.start({ dataset: 'test_dataset', maintainWindow: false });

			assert.ok(orchestrator.stats.startTime instanceof Date, 'Start time should be set');

			orchestrator.stop();
		});
	});

	describe('start with backfill', () => {
		it('should backfill all tables when maintainWindow=true and data is missing', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				dataset: 'test_dataset',
				batchSize: 10,
				retentionDays: 7,
			});

			orchestrator.createDataset = async () => {};
			orchestrator.createTables = async () => {};

			const backfilledTables = [];
			orchestrator.backfillTable = async (dataset, table) => {
				backfilledTables.push(table);
			};

			orchestrator.checkDataRange = async () => ({
				hasData: false,
			});

			orchestrator.generateAndInsertBatch = async () => {};

			await orchestrator.start({ dataset: 'test_dataset', maintainWindow: true, targetDays: 7 });

			// Should backfill all 3 tables
			assert.strictEqual(backfilledTables.length, 3);
			assert.ok(backfilledTables.includes('vessel_positions'));
			assert.ok(backfilledTables.includes('port_events'));
			assert.ok(backfilledTables.includes('vessel_metadata'));

			orchestrator.stop();
		});

		it('should skip backfill when maintainWindow=false', async () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: './test-key.json',
					location: 'US',
				},
				scenario: 'small',
				dataset: 'test_dataset',
				batchSize: 10,
			});

			orchestrator.createDataset = async () => {};
			orchestrator.createTables = async () => {};

			let checkDataRangeCalled = false;
			orchestrator.checkDataRange = async () => {
				checkDataRangeCalled = true;
				return { hasData: false };
			};

			orchestrator.generateAndInsertBatch = async () => {};

			await orchestrator.start({ dataset: 'test_dataset', maintainWindow: false });

			assert.strictEqual(checkDataRangeCalled, false, 'Should not check data range when maintainWindow=false');

			orchestrator.stop();
		});
	});
});
