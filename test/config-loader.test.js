/**
 * Tests for config-loader.js
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { getSynthesizerConfig } from '../src/config-loader.js';

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
	});
});
