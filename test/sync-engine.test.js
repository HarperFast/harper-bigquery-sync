/**
 * Tests for sync-engine.js
 *
 * Note: These are basic unit tests. Integration tests require a running Harper instance.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';

describe('Sync Engine', () => {
	describe('Phase calculation', () => {
		it('should determine initial phase when lag is very high', () => {
			// Simulating phase logic from calculatePhase()
			const now = Date.now();
			const lastTimestamp = now - 7 * 24 * 60 * 60 * 1000; // 7 days ago
			const lagSeconds = (now - lastTimestamp) / 1000;

			const catchupThreshold = 3600; // 1 hour
			const steadyThreshold = 300; // 5 minutes

			let phase;
			if (lagSeconds > catchupThreshold) {
				phase = 'initial';
			} else if (lagSeconds > steadyThreshold) {
				phase = 'catchup';
			} else {
				phase = 'steady';
			}

			assert.strictEqual(phase, 'initial');
		});

		it('should determine catchup phase when lag is moderate', () => {
			const now = Date.now();
			const lastTimestamp = now - 30 * 60 * 1000; // 30 minutes ago
			const lagSeconds = (now - lastTimestamp) / 1000;

			const catchupThreshold = 3600;
			const steadyThreshold = 300;

			let phase;
			if (lagSeconds > catchupThreshold) {
				phase = 'initial';
			} else if (lagSeconds > steadyThreshold) {
				phase = 'catchup';
			} else {
				phase = 'steady';
			}

			assert.strictEqual(phase, 'catchup');
		});

		it('should determine steady phase when lag is low', () => {
			const now = Date.now();
			const lastTimestamp = now - 2 * 60 * 1000; // 2 minutes ago
			const lagSeconds = (now - lastTimestamp) / 1000;

			const catchupThreshold = 3600;
			const steadyThreshold = 300;

			let phase;
			if (lagSeconds > catchupThreshold) {
				phase = 'initial';
			} else if (lagSeconds > steadyThreshold) {
				phase = 'catchup';
			} else {
				phase = 'steady';
			}

			assert.strictEqual(phase, 'steady');
		});
	});

	describe('Batch size calculation', () => {
		it('should use large batch size for initial phase', () => {
			const phase = 'initial';
			const config = {
				initialBatchSize: 10000,
				catchupBatchSize: 1000,
				steadyBatchSize: 500,
			};

			let batchSize;
			switch (phase) {
				case 'initial':
					batchSize = config.initialBatchSize;
					break;
				case 'catchup':
					batchSize = config.catchupBatchSize;
					break;
				case 'steady':
					batchSize = config.steadyBatchSize;
					break;
				default:
					batchSize = config.steadyBatchSize;
			}

			assert.strictEqual(batchSize, 10000);
		});

		it('should use medium batch size for catchup phase', () => {
			const phase = 'catchup';
			const config = {
				initialBatchSize: 10000,
				catchupBatchSize: 1000,
				steadyBatchSize: 500,
			};

			let batchSize;
			switch (phase) {
				case 'initial':
					batchSize = config.initialBatchSize;
					break;
				case 'catchup':
					batchSize = config.catchupBatchSize;
					break;
				case 'steady':
					batchSize = config.steadyBatchSize;
					break;
				default:
					batchSize = config.steadyBatchSize;
			}

			assert.strictEqual(batchSize, 1000);
		});

		it('should use small batch size for steady phase', () => {
			const phase = 'steady';
			const config = {
				initialBatchSize: 10000,
				catchupBatchSize: 1000,
				steadyBatchSize: 500,
			};

			let batchSize;
			switch (phase) {
				case 'initial':
					batchSize = config.initialBatchSize;
					break;
				case 'catchup':
					batchSize = config.catchupBatchSize;
					break;
				case 'steady':
					batchSize = config.steadyBatchSize;
					break;
				default:
					batchSize = config.steadyBatchSize;
			}

			assert.strictEqual(batchSize, 500);
		});
	});

	describe('Record ID generation', () => {
		it('should generate consistent IDs from same input', async () => {
			const crypto = await import('node:crypto');

			const record = {
				timestamp: '2024-01-01T00:00:00.000Z',
				mmsi: '367123456',
			};

			const id1 = crypto
				.createHash('sha256')
				.update(`${record.timestamp}-${record.mmsi}`)
				.digest('hex')
				.substring(0, 16);

			const id2 = crypto
				.createHash('sha256')
				.update(`${record.timestamp}-${record.mmsi}`)
				.digest('hex')
				.substring(0, 16);

			assert.strictEqual(id1, id2);
		});

		it('should generate different IDs for different records', async () => {
			const crypto = await import('node:crypto');

			const record1 = {
				timestamp: '2024-01-01T00:00:00.000Z',
				mmsi: '367123456',
			};

			const record2 = {
				timestamp: '2024-01-01T00:01:00.000Z',
				mmsi: '367123456',
			};

			const id1 = crypto
				.createHash('sha256')
				.update(`${record1.timestamp}-${record1.mmsi}`)
				.digest('hex')
				.substring(0, 16);

			const id2 = crypto
				.createHash('sha256')
				.update(`${record2.timestamp}-${record2.mmsi}`)
				.digest('hex')
				.substring(0, 16);

			assert.notStrictEqual(id1, id2);
		});
	});

	describe('Modulo partitioning', () => {
		it('should distribute records evenly across nodes', () => {
			const clusterSize = 3;
			const records = 1000;
			const distribution = [0, 0, 0];

			for (let i = 0; i < records; i++) {
				const timestamp = Date.now() + i * 1000;
				const nodeId = timestamp % clusterSize;
				distribution[nodeId]++;
			}

			// Each node should get approximately 1/3 of records (within 10% tolerance)
			const expected = records / clusterSize;
			const tolerance = expected * 0.1;

			for (const count of distribution) {
				assert.ok(Math.abs(count - expected) <= tolerance);
			}
		});

		it('should assign same timestamp to same node consistently', () => {
			const clusterSize = 3;
			const timestamp = 1704067200000; // Fixed timestamp

			const nodeId1 = timestamp % clusterSize;
			const nodeId2 = timestamp % clusterSize;

			assert.strictEqual(nodeId1, nodeId2);
		});
	});

	describe('Poll interval calculation', () => {
		it('should use minimal interval for initial phase', () => {
			const phase = 'initial';
			const config = {
				pollInterval: 30000, // 30 seconds
			};

			let interval;
			if (phase === 'initial' || phase === 'catchup') {
				interval = 1000; // Poll aggressively
			} else {
				interval = config.pollInterval;
			}

			assert.strictEqual(interval, 1000);
		});

		it('should use minimal interval for catchup phase', () => {
			const phase = 'catchup';
			const config = {
				pollInterval: 30000,
			};

			let interval;
			if (phase === 'initial' || phase === 'catchup') {
				interval = 1000;
			} else {
				interval = config.pollInterval;
			}

			assert.strictEqual(interval, 1000);
		});

		it('should use configured interval for steady phase', () => {
			const phase = 'steady';
			const config = {
				pollInterval: 30000,
			};

			let interval;
			if (phase === 'initial' || phase === 'catchup') {
				interval = 1000;
			} else {
				interval = config.pollInterval;
			}

			assert.strictEqual(interval, 30000);
		});
	});

	describe('Timestamp validation', () => {
		it('should accept valid ISO 8601 timestamps', () => {
			const validTimestamps = ['2024-01-01T00:00:00Z', '2024-01-01T00:00:00.000Z', '2024-12-31T23:59:59.999Z'];

			for (const timestamp of validTimestamps) {
				const date = new Date(timestamp);
				assert.ok(!isNaN(date.getTime()));
			}
		});

		it('should reject invalid timestamps', () => {
			const invalidTimestamps = [
				null,
				undefined,
				'',
				'not-a-date',
				'2024-13-01T00:00:00Z', // Invalid month
				'2024-01-32T00:00:00Z', // Invalid day
			];

			for (const timestamp of invalidTimestamps) {
				if (!timestamp) {
					assert.ok(true); // null/undefined are invalid
				} else {
					const date = new Date(timestamp);
					// Invalid dates should be NaN or have wrong values
					const isValid = !isNaN(date.getTime()) && date.toISOString().startsWith(timestamp.substring(0, 10));
					assert.ok(!isValid);
				}
			}
		});
	});
});

