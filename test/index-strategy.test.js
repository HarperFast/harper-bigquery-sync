/**
 * Tests for index-strategy.js
 *
 * Tests index detection logic for Harper table creation
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { IndexStrategy } from '../src/index-strategy.js';

describe('IndexStrategy', () => {
	describe('shouldIndex', () => {
		it('should index timestamp column from config', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'created_at' });

			assert.strictEqual(strategy.shouldIndex('created_at'), true);
		});

		it('should not index non-timestamp columns', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });

			assert.strictEqual(strategy.shouldIndex('name'), false);
		});

		it('should index columns ending with _id', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });

			assert.strictEqual(strategy.shouldIndex('user_id'), true);
			assert.strictEqual(strategy.shouldIndex('vessel_id'), true);
		});

		it('should index columns ending with Id (camelCase)', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });

			assert.strictEqual(strategy.shouldIndex('userId'), true);
			assert.strictEqual(strategy.shouldIndex('vesselId'), true);
		});

		it('should index column named just "id"', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });

			assert.strictEqual(strategy.shouldIndex('id'), true);
		});

		it('should not index columns with id in the middle', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });

			assert.strictEqual(strategy.shouldIndex('identity'), false);
			assert.strictEqual(strategy.shouldIndex('video'), false);
		});
	});

	describe('getIndexes', () => {
		it('should return indexes for timestamp and id columns', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });
			const fields = [
				{ name: 'id', type: 'STRING' },
				{ name: 'timestamp', type: 'TIMESTAMP' },
				{ name: 'name', type: 'STRING' },
			];

			const result = strategy.getIndexes(fields);

			assert.deepStrictEqual(result, ['id', 'timestamp']);
		});

		it('should return unique indexes', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });
			const fields = [
				{ name: 'timestamp', type: 'TIMESTAMP' },
			];

			const result = strategy.getIndexes(fields);

			assert.deepStrictEqual(result, ['timestamp']);
		});

		it('should handle tables with no indexable columns', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });
			const fields = [
				{ name: 'name', type: 'STRING' },
				{ name: 'description', type: 'STRING' },
			];

			const result = strategy.getIndexes(fields);

			assert.deepStrictEqual(result, []);
		});

		it('should handle multiple ID columns', () => {
			const strategy = new IndexStrategy({ timestampColumn: 'timestamp' });
			const fields = [
				{ name: 'id', type: 'STRING' },
				{ name: 'user_id', type: 'STRING' },
				{ name: 'vessel_id', type: 'STRING' },
				{ name: 'timestamp', type: 'TIMESTAMP' },
				{ name: 'name', type: 'STRING' },
			];

			const result = strategy.getIndexes(fields);

			assert.deepStrictEqual(result.sort(), ['id', 'timestamp', 'user_id', 'vessel_id']);
		});
	});
});
