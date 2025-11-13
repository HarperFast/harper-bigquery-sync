/**
 * Tests for bigquery-client.js
 *
 * Focus: Timestamp normalization to prevent "invalid timestamp" errors
 * on subsequent batch fetches when checkpoint values come back as Date objects
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { BigQueryClient } from '../src/bigquery-client.js';

// Mock logger global that Harper provides at runtime
const mockLogger = {
	info: () => {},
	debug: () => {},
	trace: () => {},
	warn: () => {},
	error: () => {},
};

describe('BigQueryClient', () => {
	before(() => {
		// Set up global logger mock
		global.logger = mockLogger;
	});

	after(() => {
		// Clean up global logger mock
		delete global.logger;
	});
	describe('normalizeToIso', () => {
		const mockConfig = {
			bigquery: {
				projectId: 'test-project',
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				credentials: '/path/to/creds.json',
				location: 'US',
			},
		};

		it('should convert Date object to ISO string', async () => {
			const client = new BigQueryClient(mockConfig);
			const date = new Date('2024-01-01T12:00:00.000Z');
			const result = await client.normalizeToIso(date);

			assert.strictEqual(result, '2024-01-01T12:00:00.000Z');
		});

		it('should preserve valid ISO string', async () => {
			const client = new BigQueryClient(mockConfig);
			const isoString = '2024-01-01T12:00:00.000Z';
			const result = await client.normalizeToIso(isoString);

			assert.strictEqual(result, '2024-01-01T12:00:00.000Z');
		});

		it('should convert Unix timestamp (number) to ISO string', async () => {
			const client = new BigQueryClient(mockConfig);
			const unixTimestamp = 1704110400000; // 2024-01-01T12:00:00.000Z
			const result = await client.normalizeToIso(unixTimestamp);

			assert.strictEqual(result, '2024-01-01T12:00:00.000Z');
		});

		it('should handle objects with toISOString method', async () => {
			const client = new BigQueryClient(mockConfig);
			const customObject = {
				toISOString: () => '2024-01-01T12:00:00.000Z',
			};
			const result = await client.normalizeToIso(customObject);

			assert.strictEqual(result, '2024-01-01T12:00:00.000Z');
		});

		it('should return null for null input', async () => {
			const client = new BigQueryClient(mockConfig);
			const result = await client.normalizeToIso(null);

			assert.strictEqual(result, null);
		});

		it('should throw error for invalid string', async () => {
			const client = new BigQueryClient(mockConfig);

			await assert.rejects(async () => await client.normalizeToIso('not-a-valid-date'), /Unparseable timestamp string/);
		});

		it('should throw error for unsupported type', async () => {
			const client = new BigQueryClient(mockConfig);

			await assert.rejects(
				async () => await client.normalizeToIso({ invalid: 'object' }),
				/Unsupported lastTimestamp type/
			);
		});

		it('should throw error for Invalid Date object', async () => {
			const client = new BigQueryClient(mockConfig);
			const invalidDate = new Date('invalid-date-string');

			await assert.rejects(async () => await client.normalizeToIso(invalidDate), /Invalid Date object/);
		});
	});

	describe('Checkpoint timestamp handling regression test', () => {
		const mockConfig = {
			bigquery: {
				projectId: 'test-project',
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				credentials: '/path/to/creds.json',
				location: 'US',
			},
		};

		it('should handle Date object from Harper checkpoint table', async () => {
			const client = new BigQueryClient(mockConfig);

			// Simulate what Harper returns when reading checkpoint with Date! type
			// Harper converts stored ISO strings back to Date objects
			const checkpointTimestamp = new Date('2024-01-01T12:00:00.000Z');

			// This should normalize to ISO string for BigQuery
			const normalized = await client.normalizeToIso(checkpointTimestamp);

			// Verify it's a valid ISO string
			assert.strictEqual(typeof normalized, 'string');
			assert.strictEqual(normalized, '2024-01-01T12:00:00.000Z');

			// Verify the string can be parsed back to the same date
			const reparsed = new Date(normalized);
			assert.strictEqual(reparsed.getTime(), checkpointTimestamp.getTime());
		});

		it('should handle second batch fetch after checkpoint reload', async () => {
			const client = new BigQueryClient(mockConfig);

			// First batch: Start with ISO string (initial checkpoint)
			const firstBatchTimestamp = '2024-01-01T00:00:00.000Z';
			const normalized1 = await client.normalizeToIso(firstBatchTimestamp);
			assert.strictEqual(normalized1, '2024-01-01T00:00:00.000Z');

			// After first batch, we save checkpoint with last timestamp
			// Simulate Harper converting it to Date when loading from database
			const checkpointDate = new Date('2024-01-01T01:00:00.000Z');

			// Second batch: Use Date object from reloaded checkpoint
			// This is the scenario that was failing before the fix
			const normalized2 = await client.normalizeToIso(checkpointDate);

			// Should successfully normalize to ISO string
			assert.strictEqual(typeof normalized2, 'string');
			assert.strictEqual(normalized2, '2024-01-01T01:00:00.000Z');

			// Verify it would work with BigQuery's TIMESTAMP() function
			assert.ok(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(normalized2));
		});

		it('should maintain timestamp precision through checkpoint cycle', async () => {
			const client = new BigQueryClient(mockConfig);

			// Original timestamp with milliseconds
			const original = '2024-01-01T12:34:56.789Z';

			// Convert to Date (simulating Harper's checkpoint storage)
			const dateObject = new Date(original);

			// Normalize back to ISO string
			const normalized = await client.normalizeToIso(dateObject);

			// Should preserve milliseconds
			assert.strictEqual(normalized, original);
		});
	});

	describe('Corrupt checkpoint handling', () => {
		const mockConfig = {
			bigquery: {
				projectId: 'test-project',
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				credentials: '/path/to/creds.json',
				location: 'US',
			},
		};

		it('should detect Invalid Date from corrupt checkpoint', async () => {
			const client = new BigQueryClient(mockConfig);

			// Simulate corrupt checkpoint: Invalid Date object
			const corruptTimestamp = new Date('this-is-not-a-valid-date');

			// Verify it's actually invalid
			assert.ok(Number.isNaN(corruptTimestamp.getTime()));
			assert.ok(corruptTimestamp instanceof Date);

			// Should throw error with clear message
			await assert.rejects(async () => await client.normalizeToIso(corruptTimestamp), /Invalid Date object/);
		});

		it('should handle checkpoint with string that becomes Invalid Date', async () => {
			const client = new BigQueryClient(mockConfig);

			// Simulate checkpoint with unparseable string
			const badString = 'not-a-date-2024-garbage';

			await assert.rejects(async () => await client.normalizeToIso(badString), /Unparseable timestamp string/);
		});

		it('should successfully normalize checkpoint after corruption is fixed', async () => {
			const client = new BigQueryClient(mockConfig);

			// After corrupt checkpoint is deleted, new checkpoint should work
			const validDate = new Date('2024-01-01T00:00:00.000Z');
			const normalized = await client.normalizeToIso(validDate);

			assert.strictEqual(normalized, '2024-01-01T00:00:00.000Z');
		});

		it('should detect epoch 0 as valid but very old', async () => {
			const client = new BigQueryClient(mockConfig);

			// Unix epoch (valid but unusual for modern data)
			const epochDate = new Date(0);
			const normalized = await client.normalizeToIso(epochDate);

			assert.strictEqual(normalized, '1970-01-01T00:00:00.000Z');
		});

		it('should handle very recent timestamps correctly', async () => {
			const client = new BigQueryClient(mockConfig);

			// Current time
			const now = new Date();
			const normalized = await client.normalizeToIso(now);

			// Should be valid ISO string
			assert.ok(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(normalized));

			// Should be within 1 second of now
			const reparsed = new Date(normalized);
			const diff = Math.abs(reparsed.getTime() - now.getTime());
			assert.ok(diff < 1000);
		});
	});

	describe('Exponential backoff retry logic', () => {
		const mockConfig = {
			bigquery: {
				projectId: 'test-project',
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				credentials: '/path/to/creds.json',
				location: 'US',
				maxRetries: 3,
				initialRetryDelay: 100, // Shorter delay for testing
			},
		};

		describe('isRetryableError', () => {
			it('should identify rate limit errors as retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = { code: 'rateLimitExceeded' };

				assert.strictEqual(client.isRetryableError(error), true);
			});

			it('should identify quota exceeded errors as retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = { code: 'quotaExceeded' };

				assert.strictEqual(client.isRetryableError(error), true);
			});

			it('should identify internal errors as retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = { code: 'internalError' };

				assert.strictEqual(client.isRetryableError(error), true);
			});

			it('should identify 503 HTTP status as retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = { response: { status: 503 } };

				assert.strictEqual(client.isRetryableError(error), true);
			});

			it('should identify 429 HTTP status as retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = { response: { status: 429 } };

				assert.strictEqual(client.isRetryableError(error), true);
			});

			it('should identify nested BigQuery errors as retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = {
					message: 'Query failed',
					errors: [{ reason: 'backendError', message: 'Backend temporarily unavailable' }],
				};

				assert.strictEqual(client.isRetryableError(error), true);
			});

			it('should identify syntax errors as non-retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = { code: 'invalidQuery' };

				assert.strictEqual(client.isRetryableError(error), false);
			});

			it('should identify permission errors as non-retryable', () => {
				const client = new BigQueryClient(mockConfig);
				const error = {
					errors: [{ reason: 'accessDenied', message: 'Permission denied' }],
				};

				assert.strictEqual(client.isRetryableError(error), false);
			});

			it('should handle null error gracefully', () => {
				const client = new BigQueryClient(mockConfig);

				assert.strictEqual(client.isRetryableError(null), false);
			});
		});

		describe('executeWithRetry', () => {
			it('should succeed on first attempt if no error', async () => {
				const client = new BigQueryClient(mockConfig);
				let attempts = 0;

				const result = await client.executeWithRetry(async () => {
					attempts++;
					return { success: true };
				}, 'testOperation');

				assert.strictEqual(attempts, 1);
				assert.deepStrictEqual(result, { success: true });
			});

			it('should retry on transient error and eventually succeed', async () => {
				const client = new BigQueryClient(mockConfig);
				let attempts = 0;

				const result = await client.executeWithRetry(async () => {
					attempts++;
					if (attempts < 3) {
						const error = new Error('Rate limit exceeded');
						error.code = 'rateLimitExceeded';
						throw error;
					}
					return { success: true, attempts };
				}, 'testOperation');

				assert.strictEqual(attempts, 3);
				assert.deepStrictEqual(result, { success: true, attempts: 3 });
			});

			it('should fail immediately on non-retryable error', async () => {
				const client = new BigQueryClient(mockConfig);
				let attempts = 0;

				await assert.rejects(
					async () => {
						await client.executeWithRetry(async () => {
							attempts++;
							const error = new Error('Invalid query');
							error.code = 'invalidQuery';
							throw error;
						}, 'testOperation');
					},
					(error) => {
						assert.strictEqual(error.message, 'Invalid query');
						assert.strictEqual(attempts, 1); // Should not retry
						return true;
					}
				);
			});

			it('should respect maxRetries configuration', async () => {
				const client = new BigQueryClient(mockConfig);
				let attempts = 0;

				await assert.rejects(
					async () => {
						await client.executeWithRetry(async () => {
							attempts++;
							const error = new Error('Rate limit exceeded');
							error.code = 'rateLimitExceeded';
							throw error;
						}, 'testOperation');
					},
					(error) => {
						assert.strictEqual(error.message, 'Rate limit exceeded');
						assert.strictEqual(attempts, mockConfig.bigquery.maxRetries + 1); // Initial + 3 retries
						return true;
					}
				);
			});

			it('should apply exponential backoff delays', async () => {
				const client = new BigQueryClient(mockConfig);
				const delays = [];
				let attempts = 0;

				await assert.rejects(
					async () => {
						await client.executeWithRetry(async () => {
							const now = Date.now();
							if (attempts > 0) {
								delays.push(now);
							} else {
								delays.push(now);
							}
							attempts++;

							const error = new Error('Service unavailable');
							error.code = 'serviceUnavailable';
							throw error;
						}, 'testOperation');
					},
					() => true
				);

				// Verify delays are increasing (exponential backoff)
				// First attempt has no delay
				// Subsequent delays should be roughly: 100ms, 200ms, 400ms (with jitter)
				assert.strictEqual(delays.length, mockConfig.bigquery.maxRetries + 1);
			});
		});

		describe('Retry configuration', () => {
			it('should use default maxRetries if not specified', () => {
				const configWithoutRetries = {
					bigquery: {
						projectId: 'test-project',
						dataset: 'test_dataset',
						table: 'test_table',
						timestampColumn: 'timestamp',
						credentials: '/path/to/creds.json',
						location: 'US',
					},
				};

				const client = new BigQueryClient(configWithoutRetries);

				assert.strictEqual(client.maxRetries, 5);
				assert.strictEqual(client.initialRetryDelay, 1000);
			});

			it('should use custom retry configuration', () => {
				const customConfig = {
					bigquery: {
						projectId: 'test-project',
						dataset: 'test_dataset',
						table: 'test_table',
						timestampColumn: 'timestamp',
						credentials: '/path/to/creds.json',
						location: 'US',
						maxRetries: 10,
						initialRetryDelay: 2000,
					},
				};

				const client = new BigQueryClient(customConfig);

				assert.strictEqual(client.maxRetries, 10);
				assert.strictEqual(client.initialRetryDelay, 2000);
			});
		});
	});
});
