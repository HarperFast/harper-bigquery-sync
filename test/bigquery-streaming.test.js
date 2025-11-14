/**
 * Tests for BigQuery Streaming Insert API
 *
 * TDD approach: Write tests first, then implement functionality
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert';
import MaritimeBigQueryClient from '../src/bigquery.js';

describe('BigQuery Streaming Inserts', () => {
	describe('Configuration', () => {
		it('should default to load job API when useStreamingAPIs is false', () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: false,
			});

			assert.strictEqual(client.useStreamingAPIs, false);
		});

		it('should use streaming API when useStreamingAPIs is true', () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: true,
			});

			assert.strictEqual(client.useStreamingAPIs, true);
		});

		it('should default to load job API when useStreamingAPIs not specified', () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
			});

			assert.strictEqual(client.useStreamingAPIs, false);
		});
	});

	describe('Insert method selection', () => {
		it('should call _insertLoadJob when useStreamingAPIs is false', async () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: false,
			});

			let loadJobCalled = false;
			client._insertLoadJob = async () => {
				loadJobCalled = true;
				return { success: true, recordCount: 1, method: 'load_job' };
			};

			const records = [{ mmsi: '123456789', timestamp: new Date().toISOString() }];
			await client.insertBatch(records);

			assert.strictEqual(loadJobCalled, true);
		});

		it('should call _insertStreaming when useStreamingAPIs is true', async () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: true,
			});

			let streamingCalled = false;
			client._insertStreaming = async () => {
				streamingCalled = true;
				return { success: true, recordCount: 1, method: 'streaming' };
			};

			const records = [{ mmsi: '123456789', timestamp: new Date().toISOString() }];
			await client.insertBatch(records);

			assert.strictEqual(streamingCalled, true);
		});
	});

	describe('Streaming insert method', () => {
		let client;

		beforeEach(() => {
			client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: true,
			});
		});

		it('should successfully insert records using streaming API', async () => {
			// Mock the BigQuery table.insert method
			client.table = {
				insert: async (records, _options) => {
					assert.strictEqual(Array.isArray(records), true);
					assert.strictEqual(records.length > 0, true);
					return {}; // Success (no errors thrown)
				},
			};

			const records = [{ mmsi: '123456789', vessel_name: 'Test Vessel', timestamp: new Date().toISOString() }];

			const result = await client._insertStreaming(records);

			assert.strictEqual(result.success, true);
			assert.strictEqual(result.recordCount, 1);
			assert.strictEqual(result.method, 'streaming');
		});

		it('should handle empty record array', async () => {
			await assert.rejects(
				async () => {
					await client._insertStreaming([]);
				},
				{
					message: 'No records to insert',
				}
			);
		});

		it('should include record count in success response', async () => {
			client.table = {
				insert: async () => ({}),
			};

			const records = [
				{ mmsi: '123', timestamp: new Date().toISOString() },
				{ mmsi: '456', timestamp: new Date().toISOString() },
				{ mmsi: '789', timestamp: new Date().toISOString() },
			];

			const result = await client._insertStreaming(records);

			assert.strictEqual(result.recordCount, 3);
		});

		it('should indicate method used as streaming', async () => {
			client.table = {
				insert: async () => ({}),
			};

			const records = [{ mmsi: '123', timestamp: new Date().toISOString() }];
			const result = await client._insertStreaming(records);

			assert.strictEqual(result.method, 'streaming');
		});
	});

	describe('Error handling', () => {
		let client;

		beforeEach(() => {
			client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: true,
			});
		});

		it('should handle partial failures with detailed error', async () => {
			client.table = {
				insert: async () => {
					const error = new Error('Partial failure');
					error.name = 'PartialFailureError';
					error.errors = [{ message: 'Row 0 failed: Invalid field' }];
					throw error;
				},
			};

			const records = [{ mmsi: 'invalid', timestamp: new Date().toISOString() }];

			await assert.rejects(
				async () => {
					await client._insertStreaming(records);
				},
				{
					message: /Partial failure/,
				}
			);
		});

		it('should retry on quota exceeded (429)', async () => {
			let attempts = 0;

			client.table = {
				insert: async () => {
					attempts++;
					if (attempts < 2) {
						const error = new Error('Quota exceeded');
						error.code = 429;
						throw error;
					}
					return {}; // Success on second attempt
				},
			};

			const records = [{ mmsi: '123', timestamp: new Date().toISOString() }];
			const result = await client._insertStreaming(records, 3);

			assert.strictEqual(attempts, 2);
			assert.strictEqual(result.success, true);
		});

		it('should retry on service unavailable (503)', async () => {
			let attempts = 0;

			client.table = {
				insert: async () => {
					attempts++;
					if (attempts < 2) {
						const error = new Error('Service unavailable');
						error.code = 503;
						throw error;
					}
					return {};
				},
			};

			const records = [{ mmsi: '123', timestamp: new Date().toISOString() }];
			const result = await client._insertStreaming(records, 3);

			assert.strictEqual(attempts, 2);
			assert.strictEqual(result.success, true);
		});

		it('should not retry on schema errors', async () => {
			let attempts = 0;

			client.table = {
				insert: async () => {
					attempts++;
					const error = new Error('Schema mismatch');
					error.code = 400;
					throw error;
				},
			};

			const records = [{ mmsi: '123', timestamp: new Date().toISOString() }];

			await assert.rejects(async () => {
				await client._insertStreaming(records, 3);
			});

			assert.strictEqual(attempts, 1); // Should not retry
		});

		it('should respect maxRetries limit', async () => {
			let attempts = 0;

			client.table = {
				insert: async () => {
					attempts++;
					const error = new Error('Service unavailable');
					error.code = 503;
					throw error;
				},
			};

			const records = [{ mmsi: '123', timestamp: new Date().toISOString() }];

			await assert.rejects(async () => {
				await client._insertStreaming(records, 2);
			});

			assert.strictEqual(attempts, 2); // Should try exactly maxRetries times
		});
	});

	describe('Backward compatibility', () => {
		it('should use load job API by default (existing behavior)', () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
			});

			assert.strictEqual(client.useStreamingAPIs, false);
		});

		it('should maintain existing insertBatch signature', async () => {
			const client = new MaritimeBigQueryClient({
				projectId: 'test-project',
				datasetId: 'test_dataset',
				tableId: 'test_table',
				useStreamingAPIs: false,
			});

			// Mock load job method
			client._insertLoadJob = async (records, maxRetries) => {
				assert.strictEqual(Array.isArray(records), true);
				assert.strictEqual(typeof maxRetries, 'number');
				return { success: true, recordCount: records.length, method: 'load_job' };
			};

			const records = [{ mmsi: '123', timestamp: new Date().toISOString() }];

			// Should accept records and maxRetries parameters
			const result = await client.insertBatch(records, 5);

			assert.strictEqual(result.success, true);
			assert.strictEqual(result.method, 'load_job');
		});
	});
});
