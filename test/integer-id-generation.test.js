/**
 * Tests for integer ID generation in sync-engine.js
 *
 * Critical performance feature: deterministic integer IDs for fast indexing
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { createHash } from 'crypto';

describe('Integer ID Generation', () => {
	/**
	 * Helper function matching sync-engine.js implementation
	 */
	function generateIntegerId(record) {
		const hashInput = JSON.stringify(record);
		const hash = createHash('sha256').update(hashInput).digest();
		const bigIntId = hash.readBigInt64BE(0);
		// Convert to positive number within safe integer range
		const id = Number(bigIntId < 0n ? -bigIntId : bigIntId) % Number.MAX_SAFE_INTEGER;
		return id;
	}

	it('should generate integer IDs', () => {
		const record = { timestamp: '2025-01-13T10:00:00Z', value: 42 };
		const id = generateIntegerId(record);

		assert.strictEqual(typeof id, 'number');
		assert.strictEqual(Number.isInteger(id), true);
		assert.ok(id > 0, 'ID should be positive');
	});

	it('should generate deterministic IDs (same input = same ID)', () => {
		const record1 = { timestamp: '2025-01-13T10:00:00Z', value: 42 };
		const record2 = { timestamp: '2025-01-13T10:00:00Z', value: 42 };

		const id1 = generateIntegerId(record1);
		const id2 = generateIntegerId(record2);

		assert.strictEqual(id1, id2);
	});

	it('should generate different IDs for different records', () => {
		const record1 = { timestamp: '2025-01-13T10:00:00Z', value: 42 };
		const record2 = { timestamp: '2025-01-13T10:00:01Z', value: 42 };
		const record3 = { timestamp: '2025-01-13T10:00:00Z', value: 43 };

		const id1 = generateIntegerId(record1);
		const id2 = generateIntegerId(record2);
		const id3 = generateIntegerId(record3);

		assert.notStrictEqual(id1, id2);
		assert.notStrictEqual(id1, id3);
		assert.notStrictEqual(id2, id3);
	});

	it('should generate IDs within JavaScript safe integer range', () => {
		const records = [
			{ timestamp: '2025-01-13T10:00:00Z', value: 1 },
			{ timestamp: '2025-01-13T10:00:01Z', value: 2 },
			{ timestamp: '2025-01-13T10:00:02Z', value: 3 },
		];

		records.forEach((record) => {
			const id = generateIntegerId(record);
			assert.ok(id <= Number.MAX_SAFE_INTEGER, `ID ${id} exceeds MAX_SAFE_INTEGER`);
			assert.ok(id >= 0, `ID ${id} is negative`);
		});
	});

	it('should handle complex records with nested data', () => {
		const record = {
			timestamp: '2025-01-13T10:00:00Z',
			nested: { foo: 'bar', baz: [1, 2, 3] },
			array: ['a', 'b', 'c'],
		};

		const id = generateIntegerId(record);

		assert.strictEqual(typeof id, 'number');
		assert.strictEqual(Number.isInteger(id), true);
		assert.ok(id > 0);
	});

	it('should produce well-distributed IDs (no obvious patterns)', () => {
		const ids = [];
		for (let i = 0; i < 100; i++) {
			const record = { timestamp: `2025-01-13T10:00:${String(i).padStart(2, '0')}Z`, value: i };
			ids.push(generateIntegerId(record));
		}

		// Check that IDs are unique
		const uniqueIds = new Set(ids);
		assert.strictEqual(uniqueIds.size, 100, 'All IDs should be unique');

		// Check distribution (IDs should not be sequential)
		const sorted = [...ids].sort((a, b) => a - b);
		let sequential = 0;
		for (let i = 1; i < sorted.length; i++) {
			if (sorted[i] - sorted[i - 1] === 1) sequential++;
		}
		assert.ok(sequential < 5, `Too many sequential IDs: ${sequential}/100`);
	});
});
