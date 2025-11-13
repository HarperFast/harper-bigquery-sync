/**
 * Tests for query-builder.js
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
	formatColumnList,
	buildPullPartitionQuery,
	buildCountPartitionQuery,
	buildVerifyRecordQuery,
	QueryBuilder,
} from '../src/query-builder.js';

describe('Query Builder', () => {
	describe('formatColumnList', () => {
		it('should format single wildcard as *', () => {
			const result = formatColumnList(['*']);
			assert.strictEqual(result, '*');
		});

		it('should format multiple columns as comma-separated list', () => {
			const result = formatColumnList(['id', 'name', 'timestamp']);
			assert.strictEqual(result, 'id, name, timestamp');
		});

		it('should format single column without comma', () => {
			const result = formatColumnList(['id']);
			assert.strictEqual(result, 'id');
		});

		it('should throw error for non-array input', () => {
			assert.throws(() => formatColumnList('not an array'), { message: 'columns must be an array' });
		});

		it('should throw error for empty array', () => {
			assert.throws(() => formatColumnList([]), { message: 'columns array cannot be empty' });
		});
	});

	describe('buildPullPartitionQuery', () => {
		it('should build query with wildcard column', () => {
			const query = buildPullPartitionQuery({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				columns: ['*'],
			});

			assert.ok(query.includes('SELECT *'));
			assert.ok(query.includes('FROM `test_dataset.test_table`'));
			assert.ok(query.includes('MOD(UNIX_MICROS(timestamp)'));
			assert.ok(query.includes('ORDER BY timestamp ASC'));
		});

		it('should build query with specific columns', () => {
			const query = buildPullPartitionQuery({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: ['timestamp', 'mmsi', 'latitude', 'longitude'],
			});

			assert.ok(query.includes('SELECT timestamp, mmsi, latitude, longitude'));
			assert.ok(query.includes('FROM `maritime_tracking.vessel_positions`'));
			assert.ok(query.includes('timestamp > TIMESTAMP(@lastTimestamp)'));
		});

		it('should throw error for missing required fields', () => {
			assert.throws(
				() =>
					buildPullPartitionQuery({
						dataset: 'test_dataset',
						table: 'test_table',
						// missing timestampColumn
					}),
				{ message: 'dataset, table, and timestampColumn are required' }
			);
		});

		it('should throw error for missing columns', () => {
			assert.throws(
				() =>
					buildPullPartitionQuery({
						dataset: 'test_dataset',
						table: 'test_table',
						timestampColumn: 'timestamp',
						// missing columns
					}),
				{ message: 'columns must be a non-empty array' }
			);
		});
	});

	describe('buildCountPartitionQuery', () => {
		it('should build count query', () => {
			const query = buildCountPartitionQuery({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
			});

			assert.ok(query.includes('SELECT COUNT(*) as count'));
			assert.ok(query.includes('FROM `test_dataset.test_table`'));
			assert.ok(query.includes('FARM_FINGERPRINT(CAST(timestamp AS STRING))'));
		});

		it('should throw error for missing required fields', () => {
			assert.throws(
				() =>
					buildCountPartitionQuery({
						dataset: 'test_dataset',
						// missing table and timestampColumn
					}),
				{ message: 'dataset, table, and timestampColumn are required' }
			);
		});
	});

	describe('buildVerifyRecordQuery', () => {
		it('should build verify query', () => {
			const query = buildVerifyRecordQuery({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
			});

			assert.ok(query.includes('SELECT 1'));
			assert.ok(query.includes('FROM `test_dataset.test_table`'));
			assert.ok(query.includes('WHERE timestamp = @timestamp'));
			assert.ok(query.includes('AND id = @recordId'));
			assert.ok(query.includes('LIMIT 1'));
		});

		it('should throw error for missing required fields', () => {
			assert.throws(
				() =>
					buildVerifyRecordQuery({
						dataset: 'test_dataset',
						// missing table and timestampColumn
					}),
				{ message: 'dataset, table, and timestampColumn are required' }
			);
		});
	});

	describe('QueryBuilder class', () => {
		it('should create instance with default columns', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
			});

			assert.strictEqual(builder.dataset, 'test_dataset');
			assert.strictEqual(builder.table, 'test_table');
			assert.strictEqual(builder.timestampColumn, 'timestamp');
			assert.deepStrictEqual(builder.columns, ['*']);
		});

		it('should create instance with specific columns', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				columns: ['id', 'name'],
			});

			assert.deepStrictEqual(builder.columns, ['id', 'name']);
		});

		it('should throw error for missing required fields', () => {
			assert.throws(
				() =>
					new QueryBuilder({
						dataset: 'test_dataset',
						// missing table and timestampColumn
					}),
				{ message: 'dataset, table, and timestampColumn are required' }
			);
		});

		it('should build pull partition query', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				columns: ['id', 'name'],
			});

			const query = builder.buildPullPartitionQuery();
			assert.ok(query.includes('SELECT id, name'));
			assert.ok(query.includes('FROM `test_dataset.test_table`'));
		});

		it('should build count partition query', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
			});

			const query = builder.buildCountPartitionQuery();
			assert.ok(query.includes('SELECT COUNT(*) as count'));
		});

		it('should build verify record query', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
			});

			const query = builder.buildVerifyRecordQuery();
			assert.ok(query.includes('SELECT 1'));
		});

		it('should get column list', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
				columns: ['id', 'name', 'timestamp'],
			});

			const columnList = builder.getColumnList();
			assert.strictEqual(columnList, 'id, name, timestamp');
		});

		it('should get wildcard for default columns', () => {
			const builder = new QueryBuilder({
				dataset: 'test_dataset',
				table: 'test_table',
				timestampColumn: 'timestamp',
			});

			const columnList = builder.getColumnList();
			assert.strictEqual(columnList, '*');
		});
	});
});
