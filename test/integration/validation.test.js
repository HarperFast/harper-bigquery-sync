/**
 * Validation Service Integration Tests
 * Tests multi-table validation functionality
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { ValidationService } from '../../src/validation.js';
import { MULTI_TABLE_CONFIG } from '../fixtures/multi-table-test-data.js';

// Mock logger for test environment
global.logger = {
	info: () => {},
	debug: () => {},
	trace: () => {},
	warn: () => {},
	error: () => {},
};

// Mock harperCluster for test environment
global.harperCluster = {
	currentNode: { id: 'test-node-1' },
	getNodes: async () => [{ id: 'test-node-1' }, { id: 'test-node-2' }],
};

describe('ValidationService - Multi-Table Support', () => {
	let validationService;
	let mockTables;

	beforeEach(() => {
		// Create mock tables for testing
		mockTables = {
			VesselPositions: {
				search: async () => [],
				get: async () => null,
			},
			PortEvents: {
				search: async () => [],
				get: async () => null,
			},
			VesselMetadata: {
				search: async () => [],
				get: async () => null,
			},
			SyncCheckpoint: {
				get: async () => null,
				put: async (data) => data,
			},
			SyncAudit: {
				put: async (data) => data,
			},
		};

		global.tables = mockTables;

		// Initialize validation service with multi-table config
		validationService = new ValidationService(MULTI_TABLE_CONFIG);
	});

	afterEach(() => {
		// Clean up
		validationService = null;
	});

	describe('Constructor', () => {
		it('should initialize with multi-table configuration', () => {
			assert.strictEqual(validationService.tables.length, 3, 'Should have 3 tables');
			assert.strictEqual(validationService.bigqueryClients.size, 3, 'Should have 3 BigQuery clients');
		});

		it('should create BigQuery client for each table', () => {
			assert.ok(validationService.bigqueryClients.has('vessel_positions'), 'Should have vessel_positions client');
			assert.ok(validationService.bigqueryClients.has('port_events'), 'Should have port_events client');
			assert.ok(validationService.bigqueryClients.has('vessel_metadata'), 'Should have vessel_metadata client');
		});

		it('should store table-specific configuration', () => {
			const vesselPosClient = validationService.bigqueryClients.get('vessel_positions');
			assert.strictEqual(vesselPosClient.targetTable, 'VesselPositions');
			assert.strictEqual(vesselPosClient.timestampColumn, 'timestamp');

			const portEventsClient = validationService.bigqueryClients.get('port_events');
			assert.strictEqual(portEventsClient.targetTable, 'PortEvents');
			assert.strictEqual(portEventsClient.timestampColumn, 'event_time');
		});
	});

	describe('validateProgress', () => {
		it('should return no_checkpoint when checkpoint does not exist', async () => {
			mockTables.SyncCheckpoint.get = async () => null;

			const result = await validationService.validateProgress('vessel_positions');

			assert.strictEqual(result.status, 'no_checkpoint');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.ok(result.message.includes('No checkpoint found'));
		});

		it('should detect stalled sync (no progress in 10+ minutes)', async () => {
			const elevenMinutesAgo = new Date(Date.now() - 11 * 60 * 1000).toISOString();
			mockTables.SyncCheckpoint.get = async (id) => ({
				id,
				lastSyncTime: elevenMinutesAgo,
				lastTimestamp: elevenMinutesAgo,
				recordsIngested: 1000,
				phase: 'steady',
			});

			const result = await validationService.validateProgress('vessel_positions');

			assert.strictEqual(result.status, 'stalled');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.ok(result.timeSinceLastSync > 600000);
		});

		it('should detect healthy sync with minimal lag', async () => {
			const thirtySecondsAgo = new Date(Date.now() - 30 * 1000).toISOString();
			mockTables.SyncCheckpoint.get = async (id) => ({
				id,
				lastSyncTime: thirtySecondsAgo,
				lastTimestamp: thirtySecondsAgo,
				recordsIngested: 1000,
				phase: 'steady',
			});

			const result = await validationService.validateProgress('vessel_positions');

			assert.strictEqual(result.status, 'healthy');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.ok(result.lagSeconds < 60);
		});

		it('should use composite checkpoint ID format', async () => {
			let capturedCheckpointId;
			mockTables.SyncCheckpoint.get = async (id) => {
				capturedCheckpointId = id;
				return null;
			};

			await validationService.validateProgress('vessel_positions');

			// Should be in format: {tableId}_{nodeId}
			assert.ok(capturedCheckpointId.includes('vessel_positions_'));
		});
	});

	describe('smokeTest', () => {
		it('should return table_not_found when target table does not exist', async () => {
			const result = await validationService.smokeTest('vessel_positions', 'NonExistentTable', 'timestamp');

			assert.strictEqual(result.status, 'table_not_found');
			assert.strictEqual(result.tableId, 'vessel_positions');
		});

		it('should return no_recent_data when no records in last 5 minutes', async () => {
			mockTables.VesselPositions.search = async () => [];

			const result = await validationService.smokeTest('vessel_positions', 'VesselPositions', 'timestamp');

			assert.strictEqual(result.status, 'no_recent_data');
			assert.strictEqual(result.tableId, 'vessel_positions');
		});

		it('should return healthy when recent records exist', async () => {
			const twoMinutesAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();
			mockTables.VesselPositions.search = async () => [
				{
					id: 'test-record',
					timestamp: twoMinutesAgo,
					mmsi: '367123456',
				},
			];

			const result = await validationService.smokeTest('vessel_positions', 'VesselPositions', 'timestamp');

			assert.strictEqual(result.status, 'healthy');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.ok(result.lagSeconds < 180);
		});

		it('should work with different timestamp column names', async () => {
			const twoMinutesAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();
			mockTables.PortEvents.search = async () => [
				{
					id: 'test-event',
					event_time: twoMinutesAgo,
					port_id: 'SFO',
				},
			];

			const result = await validationService.smokeTest('port_events', 'PortEvents', 'event_time');

			assert.strictEqual(result.status, 'healthy');
			assert.strictEqual(result.tableId, 'port_events');
			assert.strictEqual(result.latestTimestamp, twoMinutesAgo);
		});
	});

	describe('spotCheckRecords', () => {
		it('should return config_error when BigQuery client not found', async () => {
			const result = await validationService.spotCheckRecords('non_existent_table');

			assert.strictEqual(result.status, 'config_error');
			assert.ok(result.message.includes('No BigQuery client found'));
		});

		it('should return table_not_found when target table does not exist', async () => {
			// Override bigqueryClients to include a client for a non-existent table
			validationService.bigqueryClients.set('test_table', {
				client: { verifyRecord: async () => true, pullPartition: async () => [] },
				targetTable: 'NonExistentTable',
				timestampColumn: 'timestamp',
			});

			const result = await validationService.spotCheckRecords('test_table');

			assert.strictEqual(result.status, 'table_not_found');
		});

		it('should return no_data when no records in Harper', async () => {
			mockTables.VesselPositions.search = async () => [];

			const result = await validationService.spotCheckRecords('vessel_positions');

			assert.strictEqual(result.status, 'no_data');
			assert.strictEqual(result.tableId, 'vessel_positions');
		});

		it('should detect phantom records (in Harper but not in BigQuery)', async () => {
			const timestamp = new Date(Date.now() - 30 * 1000).toISOString();

			// Harper has records
			mockTables.VesselPositions.search = async () => [
				{ id: 'vessel-1', timestamp, mmsi: '367123456' },
				{ id: 'vessel-2', timestamp, mmsi: '367789012' },
			];

			mockTables.VesselPositions.get = async (id) => {
				// Simulate Harper having the records
				return { id, timestamp, mmsi: '367123456' };
			};

			// Mock BigQuery client to say records DON'T exist in BigQuery
			const mockBQClient = {
				verifyRecord: async (_record) => false, // Records don't exist in BQ
				pullPartition: async () => [], // No BQ records to check in reverse
			};

			validationService.bigqueryClients.set('vessel_positions', {
				client: mockBQClient,
				targetTable: 'VesselPositions',
				timestampColumn: 'timestamp',
			});

			const result = await validationService.spotCheckRecords('vessel_positions');

			assert.strictEqual(result.status, 'issues_found');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.strictEqual(result.issues.length, 2, 'Should find 2 phantom records');
			assert.strictEqual(result.issues[0].type, 'phantom_record');
			assert.ok(result.message.includes('2 mismatches'));
		});

		it('should detect missing records (in BigQuery but not in Harper)', async () => {
			const timestamp = new Date(Date.now() - 30 * 1000).toISOString();

			// Harper has ONE record in search (so we don't hit the no_data early return)
			mockTables.VesselPositions.search = async () => [
				{
					id: 'vessel-existing',
					timestamp,
					mmsi: '367111111',
				},
			];

			// Harper get returns the existing record, but NOT the ones from BigQuery
			mockTables.VesselPositions.get = async (id) => {
				if (id.includes('existing')) {
					return { id, timestamp, mmsi: '367111111' };
				}
				return null; // Missing records return null
			};

			// Mock BigQuery client to return records that should exist in Harper
			const mockBQClient = {
				verifyRecord: async (_record) => true, // Existing record verified in BQ
				pullPartition: async () => [
					// These records exist in BQ but NOT in Harper
					{ timestamp, mmsi: '367123456', latitude: 37.7749, longitude: -122.4194 },
					{ timestamp, mmsi: '367789012', latitude: 33.7405, longitude: -118.272 },
				],
			};

			validationService.bigqueryClients.set('vessel_positions', {
				client: mockBQClient,
				targetTable: 'VesselPositions',
				timestampColumn: 'timestamp',
			});

			const result = await validationService.spotCheckRecords('vessel_positions');

			assert.strictEqual(result.status, 'issues_found');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.strictEqual(result.issues.length, 2, 'Should find 2 missing records');
			assert.strictEqual(result.issues[0].type, 'missing_record');
			assert.ok(result.message.includes('2 mismatches'));
		});

		it('should return healthy when all records match', async () => {
			const timestamp = new Date(Date.now() - 30 * 1000).toISOString();

			// Harper has records
			mockTables.VesselPositions.search = async () => [{ id: 'vessel-1', timestamp, mmsi: '367123456' }];

			mockTables.VesselPositions.get = async (id) => {
				// Simulate Harper having the records
				return { id, timestamp, mmsi: '367123456' };
			};

			// Mock BigQuery client - all records verified
			const mockBQClient = {
				verifyRecord: async (_record) => true, // Records exist in BQ
				pullPartition: async () => [{ timestamp, mmsi: '367123456', latitude: 37.7749, longitude: -122.4194 }],
			};

			validationService.bigqueryClients.set('vessel_positions', {
				client: mockBQClient,
				targetTable: 'VesselPositions',
				timestampColumn: 'timestamp',
			});

			const result = await validationService.spotCheckRecords('vessel_positions');

			assert.strictEqual(result.status, 'healthy');
			assert.strictEqual(result.tableId, 'vessel_positions');
			assert.strictEqual(result.issues.length, 0);
			assert.ok(result.message.includes('all match'));
		});
	});

	describe('runValidation', () => {
		beforeEach(() => {
			// Set up reasonable defaults for a complete validation run
			const recentTimestamp = new Date(Date.now() - 30 * 1000).toISOString();

			mockTables.SyncCheckpoint.get = async () => ({
				lastSyncTime: recentTimestamp,
				lastTimestamp: recentTimestamp,
				recordsIngested: 1000,
				phase: 'steady',
			});

			mockTables.VesselPositions.search = async () => [
				{
					id: 'vessel-1',
					timestamp: recentTimestamp,
					mmsi: '367123456',
				},
			];

			mockTables.PortEvents.search = async () => [
				{
					id: 'port-1',
					event_time: recentTimestamp,
					port_id: 'SFO',
				},
			];

			mockTables.VesselMetadata.search = async () => [
				{
					id: 'meta-1',
					last_updated: recentTimestamp,
					mmsi: '367123456',
				},
			];
		});

		it('should validate all tables independently', async () => {
			const results = await validationService.runValidation();

			assert.ok(results.tables.vessel_positions, 'Should have vessel_positions results');
			assert.ok(results.tables.port_events, 'Should have port_events results');
			assert.ok(results.tables.vessel_metadata, 'Should have vessel_metadata results');
		});

		it('should run all check types for each table', async () => {
			const results = await validationService.runValidation();

			// Check vessel_positions
			assert.ok(results.tables.vessel_positions.checks.progress, 'Should have progress check');
			assert.ok(results.tables.vessel_positions.checks.smokeTest, 'Should have smoke test');
			assert.ok(results.tables.vessel_positions.checks.spotCheck, 'Should have spot check');

			// Check port_events
			assert.ok(results.tables.port_events.checks.progress, 'Should have progress check');
			assert.ok(results.tables.port_events.checks.smokeTest, 'Should have smoke test');
			assert.ok(results.tables.port_events.checks.spotCheck, 'Should have spot check');
		});

		it('should determine per-table status correctly', async () => {
			const results = await validationService.runValidation();

			assert.ok(results.tables.vessel_positions.overallStatus, 'Should have overall status for vessel_positions');
			assert.ok(results.tables.port_events.overallStatus, 'Should have overall status for port_events');
			assert.ok(results.tables.vessel_metadata.overallStatus, 'Should have overall status for vessel_metadata');
		});

		it('should determine overall status across all tables', async () => {
			const results = await validationService.runValidation();

			assert.ok(['healthy', 'issues_detected'].includes(results.overallStatus), 'Should have valid overall status');
		});

		it('should log audit entry with multi-table results', async () => {
			let auditEntry;
			mockTables.SyncAudit.put = async (data) => {
				auditEntry = data;
				return data;
			};

			await validationService.runValidation();

			assert.ok(auditEntry, 'Should create audit entry');
			assert.ok(auditEntry.id, 'Audit entry should have ID');
			assert.ok(auditEntry.timestamp, 'Audit entry should have timestamp');
			assert.ok(auditEntry.checkResults, 'Audit entry should have check results');

			// Check results should be stringified tables object
			const parsedResults = JSON.parse(auditEntry.checkResults);
			assert.ok(parsedResults.vessel_positions, 'Should have vessel_positions in audit');
			assert.ok(parsedResults.port_events, 'Should have port_events in audit');
		});

		it('should handle validation errors gracefully', async () => {
			mockTables.SyncCheckpoint.get = async () => {
				throw new Error('Database connection failed');
			};

			try {
				await validationService.runValidation();
				assert.fail('Should have thrown error');
			} catch (error) {
				assert.strictEqual(error.message, 'Database connection failed');
			}
		});

		it('should detect issues across multiple tables and set overall status to issues_detected', async () => {
			const recentTimestamp = new Date(Date.now() - 30 * 1000).toISOString();

			// Set up checkpoint as healthy
			mockTables.SyncCheckpoint.get = async () => ({
				lastSyncTime: recentTimestamp,
				lastTimestamp: recentTimestamp,
				recordsIngested: 1000,
				phase: 'steady',
			});

			// vessel_positions: healthy
			mockTables.VesselPositions.search = async () => [
				{
					id: 'vessel-1',
					timestamp: recentTimestamp,
					mmsi: '367123456',
				},
			];
			mockTables.VesselPositions.get = async (id) => ({ id, timestamp: recentTimestamp });

			// port_events: HAS PHANTOM RECORDS (in Harper but not in BigQuery)
			mockTables.PortEvents.search = async () => [
				{
					id: 'port-1',
					event_time: recentTimestamp,
					port_id: 'SFO',
				},
			];
			mockTables.PortEvents.get = async (id) => ({ id, event_time: recentTimestamp });

			// vessel_metadata: healthy
			mockTables.VesselMetadata.search = async () => [
				{
					id: 'meta-1',
					last_updated: recentTimestamp,
					mmsi: '367123456',
				},
			];
			mockTables.VesselMetadata.get = async (id) => ({ id, last_updated: recentTimestamp });

			// Mock BigQuery clients
			// vessel_positions: all records verified
			validationService.bigqueryClients.set('vessel_positions', {
				client: {
					verifyRecord: async () => true,
					pullPartition: async () => [{ timestamp: recentTimestamp, mmsi: '367123456' }],
				},
				targetTable: 'VesselPositions',
				timestampColumn: 'timestamp',
			});

			// port_events: has phantom records (verifyRecord returns false)
			validationService.bigqueryClients.set('port_events', {
				client: {
					verifyRecord: async () => false, // Phantom record!
					pullPartition: async () => [],
				},
				targetTable: 'PortEvents',
				timestampColumn: 'event_time',
			});

			// vessel_metadata: all records verified
			validationService.bigqueryClients.set('vessel_metadata', {
				client: {
					verifyRecord: async () => true,
					pullPartition: async () => [{ last_updated: recentTimestamp, mmsi: '367123456' }],
				},
				targetTable: 'VesselMetadata',
				timestampColumn: 'last_updated',
			});

			const results = await validationService.runValidation();

			// Overall status should be issues_detected because port_events has issues
			assert.strictEqual(results.overallStatus, 'issues_detected', 'Overall status should be issues_detected');

			// vessel_positions should be healthy
			assert.strictEqual(results.tables.vessel_positions.overallStatus, 'healthy');

			// port_events should have issues
			assert.strictEqual(results.tables.port_events.overallStatus, 'issues_detected');
			assert.strictEqual(results.tables.port_events.checks.spotCheck.status, 'issues_found');
			assert.ok(results.tables.port_events.checks.spotCheck.issues.length > 0);

			// vessel_metadata should be healthy
			assert.strictEqual(results.tables.vessel_metadata.overallStatus, 'healthy');
		});
	});

	describe('generateRecordId', () => {
		it('should generate consistent IDs for same record', () => {
			const record1 = { timestamp: '2024-01-01T00:00:00Z', id: 'test-123' };
			const id1 = validationService.generateRecordId(record1, 'timestamp');
			const id2 = validationService.generateRecordId(record1, 'timestamp');

			assert.strictEqual(id1, id2, 'Should generate same ID for same record');
		});

		it('should work with different timestamp column names', () => {
			const record1 = { event_time: '2024-01-01T00:00:00Z', id: 'test-123' };
			const record2 = { last_updated: '2024-01-01T00:00:00Z', id: 'test-123' };

			const id1 = validationService.generateRecordId(record1, 'event_time');
			const id2 = validationService.generateRecordId(record2, 'last_updated');

			assert.strictEqual(id1, id2, 'Should generate same ID for same timestamp value');
		});

		it('should generate 16-character hex IDs', () => {
			const record = { timestamp: '2024-01-01T00:00:00Z', id: 'test-123' };
			const id = validationService.generateRecordId(record, 'timestamp');

			assert.strictEqual(id.length, 16, 'ID should be 16 characters');
			assert.ok(/^[0-9a-f]{16}$/.test(id), 'ID should be hexadecimal');
		});
	});

	describe('discoverCluster', () => {
		it('should discover cluster topology', async () => {
			const clusterInfo = await validationService.discoverCluster();

			assert.ok(clusterInfo.nodeId !== undefined, 'Should have nodeId');
			assert.ok(clusterInfo.clusterSize !== undefined, 'Should have clusterSize');
			assert.strictEqual(typeof clusterInfo.nodeId, 'number', 'nodeId should be a number');
			assert.strictEqual(typeof clusterInfo.clusterSize, 'number', 'clusterSize should be a number');
		});

		it('should return consistent cluster size', async () => {
			const info1 = await validationService.discoverCluster();
			const info2 = await validationService.discoverCluster();

			assert.strictEqual(info1.clusterSize, info2.clusterSize, 'Cluster size should be consistent');
		});
	});
});

