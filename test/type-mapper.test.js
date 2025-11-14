/**
 * Tests for type-mapper.js
 *
 * Tests BigQuery to Harper type mapping
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { TypeMapper } from '../src/type-mapper.js';

// Mock logger global that Harper provides at runtime
const mockLogger = {
	info: () => {},
	debug: () => {},
	trace: () => {},
	warn: () => {},
	error: () => {},
};

describe('TypeMapper', () => {
	before(() => {
		// Set up global logger mock
		global.logger = mockLogger;
	});

	after(() => {
		// Clean up global logger mock
		delete global.logger;
	});

	describe('mapScalarType', () => {
		it('should map INTEGER to Int', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('INTEGER');

			assert.strictEqual(result, 'Int');
		});

		it('should map INT64 to Int', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('INT64');

			assert.strictEqual(result, 'Int');
		});

		it('should map FLOAT64 to Float', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('FLOAT64');

			assert.strictEqual(result, 'Float');
		});

		it('should map STRING to String', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('STRING');

			assert.strictEqual(result, 'String');
		});

		it('should map BOOL to Boolean', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('BOOL');

			assert.strictEqual(result, 'Boolean');
		});

		it('should map TIMESTAMP to Date', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('TIMESTAMP');

			assert.strictEqual(result, 'Date');
		});

		it('should map DATE to Date', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('DATE');

			assert.strictEqual(result, 'Date');
		});

		it('should handle case insensitivity', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('integer');

			assert.strictEqual(result, 'Int');
		});

		it('should map unknown types to String', () => {
			const mapper = new TypeMapper();
			const result = mapper.mapScalarType('UNKNOWN_TYPE');

			assert.strictEqual(result, 'String');
		});
	});

	describe('mapField', () => {
		it('should map a NULLABLE field', () => {
			const mapper = new TypeMapper();
			const field = {
				name: 'email',
				type: 'STRING',
				mode: 'NULLABLE',
			};

			const result = mapper.mapField(field);

			assert.strictEqual(result.name, 'email');
			assert.strictEqual(result.type, 'String');
			assert.strictEqual(result.required, false);
			assert.strictEqual(result.isArray, false);
		});

		it('should map a REQUIRED field', () => {
			const mapper = new TypeMapper();
			const field = {
				name: 'id',
				type: 'STRING',
				mode: 'REQUIRED',
			};

			const result = mapper.mapField(field);

			assert.strictEqual(result.name, 'id');
			assert.strictEqual(result.type, 'String');
			assert.strictEqual(result.required, true);
			assert.strictEqual(result.isArray, false);
		});

		it('should map a REPEATED field', () => {
			const mapper = new TypeMapper();
			const field = {
				name: 'tags',
				type: 'STRING',
				mode: 'REPEATED',
			};

			const result = mapper.mapField(field);

			assert.strictEqual(result.name, 'tags');
			assert.strictEqual(result.type, 'String');
			assert.strictEqual(result.required, false);
			assert.strictEqual(result.isArray, true);
		});

		it('should handle field with no mode as NULLABLE', () => {
			const mapper = new TypeMapper();
			const field = {
				name: 'optional_field',
				type: 'INTEGER',
			};

			const result = mapper.mapField(field);

			assert.strictEqual(result.name, 'optional_field');
			assert.strictEqual(result.type, 'Int');
			assert.strictEqual(result.required, false);
			assert.strictEqual(result.isArray, false);
		});
	});

	describe('buildTableAttributes', () => {
		it('should build attributes for simple table schema', () => {
			const mapper = new TypeMapper();
			const schema = {
				fields: [
					{ name: 'id', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'name', type: 'STRING', mode: 'NULLABLE' },
					{ name: 'count', type: 'INTEGER', mode: 'NULLABLE' },
				],
			};

			const result = mapper.buildTableAttributes(schema);

			assert.deepStrictEqual(result, {
				id: { type: 'String', required: true },
				name: { type: 'String', required: false },
				count: { type: 'Int', required: false },
			});
		});

		it('should handle array fields', () => {
			const mapper = new TypeMapper();
			const schema = {
				fields: [
					{ name: 'id', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'tags', type: 'STRING', mode: 'REPEATED' },
				],
			};

			const result = mapper.buildTableAttributes(schema);

			assert.deepStrictEqual(result, {
				id: { type: 'String', required: true },
				tags: { type: '[String]', required: false },
			});
		});

		it('should handle mixed field types', () => {
			const mapper = new TypeMapper();
			const schema = {
				fields: [
					{ name: 'mmsi', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
					{ name: 'latitude', type: 'FLOAT64', mode: 'NULLABLE' },
					{ name: 'longitude', type: 'FLOAT64', mode: 'NULLABLE' },
					{ name: 'is_active', type: 'BOOL', mode: 'NULLABLE' },
					{ name: 'metadata', type: 'JSON', mode: 'NULLABLE' },
				],
			};

			const result = mapper.buildTableAttributes(schema);

			assert.deepStrictEqual(result, {
				mmsi: { type: 'String', required: true },
				timestamp: { type: 'Date', required: true },
				latitude: { type: 'Float', required: false },
				longitude: { type: 'Float', required: false },
				is_active: { type: 'Boolean', required: false },
				metadata: { type: 'Json', required: false },
			});
		});

		it('should handle empty schema', () => {
			const mapper = new TypeMapper();
			const schema = { fields: [] };

			const result = mapper.buildTableAttributes(schema);

			assert.deepStrictEqual(result, {});
		});
	});
});
