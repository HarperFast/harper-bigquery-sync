/**
 * Tests for type-converter.js
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
	convertBigInt,
	convertBigQueryTimestamp,
	convertValue,
	convertBigQueryTypes,
	convertBigQueryRecords,
} from '../src/type-converter.js';

describe('Type Converter', () => {
	describe('convertBigInt', () => {
		it('should convert small BigInt to Number', () => {
			const result = convertBigInt(BigInt(12345));
			assert.strictEqual(result, 12345);
			assert.strictEqual(typeof result, 'number');
		});

		it('should convert large BigInt to String', () => {
			const largeBigInt = BigInt(Number.MAX_SAFE_INTEGER) + BigInt(1000);
			const result = convertBigInt(largeBigInt);
			assert.strictEqual(typeof result, 'string');
		});

		it('should convert negative BigInt to Number', () => {
			const result = convertBigInt(BigInt(-12345));
			assert.strictEqual(result, -12345);
			assert.strictEqual(typeof result, 'number');
		});

		it('should convert zero BigInt to Number', () => {
			const result = convertBigInt(BigInt(0));
			assert.strictEqual(result, 0);
			assert.strictEqual(typeof result, 'number');
		});
	});

	describe('convertBigQueryTimestamp', () => {
		it('should convert BigQuery timestamp with value property', () => {
			const mockTimestamp = {
				value: '2025-11-10T12:00:00.000Z',
				constructor: { name: 'BigQueryTimestamp' },
			};

			const result = convertBigQueryTimestamp(mockTimestamp);
			assert.ok(result instanceof Date);
			assert.strictEqual(result.toISOString(), '2025-11-10T12:00:00.000Z');
		});

		it('should convert BigQuery timestamp with toJSON method', () => {
			const mockTimestamp = {
				toJSON: () => '2025-11-10T12:00:00.000Z',
				constructor: { name: 'BigQueryTimestamp' },
			};

			const result = convertBigQueryTimestamp(mockTimestamp);
			assert.ok(result instanceof Date);
			assert.strictEqual(result.toISOString(), '2025-11-10T12:00:00.000Z');
		});

		it('should return original value if conversion fails', () => {
			const mockTimestamp = {
				someOtherProperty: 'value',
				constructor: { name: 'BigQueryTimestamp' },
			};

			const result = convertBigQueryTimestamp(mockTimestamp);
			assert.strictEqual(result, mockTimestamp);
		});
	});

	describe('convertValue', () => {
		it('should return null for null input', () => {
			assert.strictEqual(convertValue(null), null);
		});

		it('should return undefined for undefined input', () => {
			assert.strictEqual(convertValue(undefined), undefined);
		});

		it('should convert BigInt', () => {
			const result = convertValue(BigInt(12345));
			assert.strictEqual(result, 12345);
		});

		it('should keep string as-is', () => {
			const result = convertValue('test string');
			assert.strictEqual(result, 'test string');
		});

		it('should keep number as-is', () => {
			const result = convertValue(123.45);
			assert.strictEqual(result, 123.45);
		});

		it('should keep boolean as-is', () => {
			const result = convertValue(true);
			assert.strictEqual(result, true);
		});

		it('should keep Date object as-is', () => {
			const date = new Date('2025-11-10T12:00:00.000Z');
			const result = convertValue(date);
			assert.strictEqual(result, date);
		});

		it('should convert BigQuery timestamp', () => {
			const mockTimestamp = {
				value: '2025-11-10T12:00:00.000Z',
				constructor: { name: 'BigQueryTimestamp' },
			};

			const result = convertValue(mockTimestamp);
			assert.ok(result instanceof Date);
		});

		it('should convert object with toJSON returning ISO date', () => {
			const mockObj = {
				toJSON: () => '2025-11-10T12:00:00.000Z',
			};

			const result = convertValue(mockObj);
			assert.ok(result instanceof Date);
			assert.strictEqual(result.toISOString(), '2025-11-10T12:00:00.000Z');
		});

		it('should use toJSON for non-date objects', () => {
			const mockObj = {
				toJSON: () => ({ key: 'value' }),
			};

			const result = convertValue(mockObj);
			assert.deepStrictEqual(result, { key: 'value' });
		});

		it('should keep plain objects as-is', () => {
			const obj = { key: 'value' };
			const result = convertValue(obj);
			assert.strictEqual(result, obj);
		});
	});

	describe('convertBigQueryTypes', () => {
		it('should convert record with multiple types', () => {
			const record = {
				id: 'test-id',
				count: BigInt(12345),
				timestamp: {
					value: '2025-11-10T12:00:00.000Z',
					constructor: { name: 'BigQueryTimestamp' },
				},
				name: 'Test Name',
				active: true,
				nullValue: null,
			};

			const result = convertBigQueryTypes(record);

			assert.strictEqual(result.id, 'test-id');
			assert.strictEqual(result.count, 12345);
			assert.ok(result.timestamp instanceof Date);
			assert.strictEqual(result.name, 'Test Name');
			assert.strictEqual(result.active, true);
			assert.strictEqual(result.nullValue, null);
		});

		it('should handle empty record', () => {
			const result = convertBigQueryTypes({});
			assert.deepStrictEqual(result, {});
		});

		it('should throw error for non-object input', () => {
			assert.throws(() => convertBigQueryTypes('not an object'), { message: 'Record must be an object' });
		});

		it('should throw error for null input', () => {
			assert.throws(() => convertBigQueryTypes(null), { message: 'Record must be an object' });
		});

		it('should preserve all field names', () => {
			const record = {
				field1: 'value1',
				field2: 'value2',
				field3: 'value3',
			};

			const result = convertBigQueryTypes(record);
			assert.deepStrictEqual(Object.keys(result), ['field1', 'field2', 'field3']);
		});

		it('should convert nested timestamp in record', () => {
			const record = {
				id: 'test-id',
				created_at: {
					value: '2025-11-10T10:00:00.000Z',
					constructor: { name: 'BigQueryTimestamp' },
				},
				updated_at: {
					value: '2025-11-10T12:00:00.000Z',
					constructor: { name: 'BigQueryDatetime' },
				},
			};

			const result = convertBigQueryTypes(record);

			assert.ok(result.created_at instanceof Date);
			assert.ok(result.updated_at instanceof Date);
			assert.strictEqual(result.created_at.toISOString(), '2025-11-10T10:00:00.000Z');
			assert.strictEqual(result.updated_at.toISOString(), '2025-11-10T12:00:00.000Z');
		});
	});

	describe('convertBigQueryRecords', () => {
		it('should convert array of records', () => {
			const records = [
				{ id: '1', count: BigInt(100) },
				{ id: '2', count: BigInt(200) },
				{ id: '3', count: BigInt(300) },
			];

			const result = convertBigQueryRecords(records);

			assert.strictEqual(result.length, 3);
			assert.strictEqual(result[0].count, 100);
			assert.strictEqual(result[1].count, 200);
			assert.strictEqual(result[2].count, 300);
		});

		it('should handle empty array', () => {
			const result = convertBigQueryRecords([]);
			assert.deepStrictEqual(result, []);
		});

		it('should throw error for non-array input', () => {
			assert.throws(() => convertBigQueryRecords('not an array'), { message: 'Records must be an array' });
		});

		it('should convert each record independently', () => {
			const records = [
				{
					id: '1',
					timestamp: {
						value: '2025-11-10T10:00:00.000Z',
						constructor: { name: 'BigQueryTimestamp' },
					},
				},
				{
					id: '2',
					timestamp: {
						value: '2025-11-10T11:00:00.000Z',
						constructor: { name: 'BigQueryTimestamp' },
					},
				},
			];

			const result = convertBigQueryRecords(records);

			assert.strictEqual(result.length, 2);
			assert.ok(result[0].timestamp instanceof Date);
			assert.ok(result[1].timestamp instanceof Date);
			assert.strictEqual(result[0].timestamp.toISOString(), '2025-11-10T10:00:00.000Z');
			assert.strictEqual(result[1].timestamp.toISOString(), '2025-11-10T11:00:00.000Z');
		});
	});
});

