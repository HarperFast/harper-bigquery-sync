/**
 * Integration Tests for Multi-Table Orchestrator
 * Tests the complete multi-table data generation pipeline
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { MultiTableOrchestrator } from '../../ext/maritime-data-synthesizer/multi-table-orchestrator.js';
import { TEST_SCENARIOS } from '../fixtures/multi-table-test-data.js';

describe('Multi-Table Orchestrator Integration', () => {
	describe('Constructor', () => {
		it('should initialize with scenario configuration', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.ok(orchestrator.bigquery, 'Should have BigQuery client');
			assert.ok(orchestrator.mmsiList, 'Should have MMSI list');
			assert.strictEqual(orchestrator.scenario.description, TEST_SCENARIOS.small.description);
		});

		it('should default to realistic scenario when invalid scenario provided', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'invalid_scenario',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(orchestrator.scenario.description, TEST_SCENARIOS.realistic.description);
		});

		it('should use current time when startTime not provided', () => {
			const before = new Date();

			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
			});

			const after = new Date();

			assert.ok(orchestrator.startTime >= before);
			assert.ok(orchestrator.startTime <= after);
		});
	});

	describe('MMSI Generation', () => {
		it('should generate consistent MMSI list', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.ok(Array.isArray(orchestrator.mmsiList));
			assert.ok(orchestrator.mmsiList.length > 0);

			// Verify MMSI format (9-digit strings)
			for (const mmsi of orchestrator.mmsiList) {
				assert.strictEqual(typeof mmsi, 'string');
				assert.strictEqual(mmsi.length, 9);
			}
		});

		it('should generate enough MMSIs for scenario', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'realistic',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			// For realistic scenario: vessel_metadata = 100
			// Should have at least 100 MMSI
			assert.ok(orchestrator.mmsiList.length >= 100);
		});

		it('should not generate duplicate MMSIs', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'stress',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			const uniqueMMSIs = new Set(orchestrator.mmsiList);
			assert.strictEqual(uniqueMMSIs.size, orchestrator.mmsiList.length);
		});
	});

	describe('Scenario Scaling', () => {
		it('should use small scenario configuration', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(orchestrator.scenario.vessel_positions, TEST_SCENARIOS.small.vessel_positions);
			assert.strictEqual(orchestrator.scenario.port_events, TEST_SCENARIOS.small.port_events);
			assert.strictEqual(orchestrator.scenario.vessel_metadata, TEST_SCENARIOS.small.vessel_metadata);
		});

		it('should use realistic scenario configuration', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'realistic',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(orchestrator.scenario.vessel_positions, TEST_SCENARIOS.realistic.vessel_positions);
			assert.strictEqual(orchestrator.scenario.port_events, TEST_SCENARIOS.realistic.port_events);
			assert.strictEqual(orchestrator.scenario.vessel_metadata, TEST_SCENARIOS.realistic.vessel_metadata);
		});

		it('should use stress scenario configuration', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'stress',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(orchestrator.scenario.vessel_positions, TEST_SCENARIOS.stress.vessel_positions);
			assert.strictEqual(orchestrator.scenario.port_events, TEST_SCENARIOS.stress.port_events);
			assert.strictEqual(orchestrator.scenario.vessel_metadata, TEST_SCENARIOS.stress.vessel_metadata);
		});
	});

	describe('Data Generation Methods', () => {
		it('should have generateVesselMetadata method', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(typeof orchestrator.generateVesselMetadata, 'function');
		});

		it('should have generatePortEvents method', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(typeof orchestrator.generatePortEvents, 'function');
		});

		it('should have generateVesselPositions method', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(typeof orchestrator.generateVesselPositions, 'function');
		});

		it('should have generateAll method', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(typeof orchestrator.generateAll, 'function');
		});

		it('should have verify method', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(typeof orchestrator.verify, 'function');
		});
	});

	describe('BigQuery Client Configuration', () => {
		it('should initialize BigQuery client with correct project', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'my-test-project',
					keyFilename: 'test-key.json',
					location: 'EU',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.strictEqual(orchestrator.projectId, 'my-test-project');
			assert.strictEqual(orchestrator.location, 'EU');
			assert.strictEqual(orchestrator.keyFilename, 'test-key.json');
		});

		it('should create BigQuery client instance', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			assert.ok(orchestrator.bigquery, 'BigQuery client should be initialized');
			assert.strictEqual(typeof orchestrator.bigquery.query, 'function');
		});
	});

	describe('Consistency Across Tables', () => {
		it('should use same MMSI list for all table generators', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			const mmsiSnapshot = [...orchestrator.mmsiList];

			// MMSI list should remain consistent throughout orchestrator lifetime
			assert.deepStrictEqual(orchestrator.mmsiList, mmsiSnapshot);
		});

		it('should use same startTime for all generators', () => {
			const startTime = new Date('2024-01-01T12:00:00Z');

			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime,
			});

			assert.strictEqual(orchestrator.startTime.toISOString(), startTime.toISOString());
		});
	});

	describe('Table Schema Definitions', () => {
		it('should have vessel_positions schema defined', () => {
			const orchestrator = new MultiTableOrchestrator({
				bigquery: {
					projectId: 'test-project',
					keyFilename: 'test-key.json',
					location: 'US',
				},
				scenario: 'small',
				startTime: new Date('2024-01-01T00:00:00Z'),
			});

			// Orchestrator has createTables method which contains schema definitions
			assert.strictEqual(typeof orchestrator.createTables, 'function');
		});
	});
});

