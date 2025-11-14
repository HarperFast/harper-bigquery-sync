/**
 * Tests for schema-manager.js
 *
 * Tests schema management and table creation logic
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { SchemaManager } from '../src/schema-manager.js';

// Mock logger global that Harper provides at runtime
const mockLogger = {
	info: () => {},
	debug: () => {},
	trace: () => {},
	warn: () => {},
	error: () => {},
};

describe('SchemaManager', () => {
	before(() => {
		// Set up global logger mock
		global.logger = mockLogger;
	});

	after(() => {
		// Clean up global logger mock
		delete global.logger;
	});

	describe('constructor', () => {
		it('should initialize with required dependencies', () => {
			const mockBigQueryClient = {};
			const manager = new SchemaManager({
				bigQueryClient: mockBigQueryClient,
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			assert.ok(manager.bigQueryClient);
			assert.ok(manager.typeMapper);
			assert.ok(manager.indexStrategy);
			assert.ok(manager.operationsClient);
		});

		it('should throw if bigQueryClient is missing', () => {
			assert.throws(() => new SchemaManager({ config: {} }), /bigQueryClient is required/);
		});

		it('should throw if config is missing', () => {
			assert.throws(() => new SchemaManager({ bigQueryClient: {} }), /config is required/);
		});
	});

	describe('determineMigrationNeeds', () => {
		it('should identify new table when table does not exist', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			const bigQuerySchema = {
				fields: [{ name: 'id', type: 'STRING', mode: 'REQUIRED' }],
			};

			const result = manager.determineMigrationNeeds(null, bigQuerySchema);

			assert.strictEqual(result.action, 'create');
			assert.deepStrictEqual(result.attributesToAdd, {
				id: { type: 'String', required: true },
			});
		});

		it('should identify new columns when table exists', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			const harperSchema = {
				attributes: {
					id: { type: 'String', required: true },
				},
			};

			const bigQuerySchema = {
				fields: [
					{ name: 'id', type: 'STRING', mode: 'REQUIRED' },
					{ name: 'email', type: 'STRING', mode: 'NULLABLE' },
				],
			};

			const result = manager.determineMigrationNeeds(harperSchema, bigQuerySchema);

			assert.strictEqual(result.action, 'migrate');
			assert.deepStrictEqual(result.attributesToAdd, {
				email: { type: 'String', required: false },
			});
		});

		it('should return no action when schemas match', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			const harperSchema = {
				attributes: {
					id: { type: 'String', required: true },
				},
			};

			const bigQuerySchema = {
				fields: [{ name: 'id', type: 'STRING', mode: 'REQUIRED' }],
			};

			const result = manager.determineMigrationNeeds(harperSchema, bigQuerySchema);

			assert.strictEqual(result.action, 'none');
			assert.deepStrictEqual(result.attributesToAdd, {});
		});

		it('should handle type changes with versioned columns', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			const harperSchema = {
				attributes: {
					count: { type: 'String', required: false },
				},
			};

			const bigQuerySchema = {
				fields: [{ name: 'count', type: 'INTEGER', mode: 'NULLABLE' }],
			};

			const result = manager.determineMigrationNeeds(harperSchema, bigQuerySchema);

			assert.strictEqual(result.action, 'migrate');
			// Should create count_v2 instead of modifying count
			assert.ok(result.attributesToAdd.count_v2);
			assert.strictEqual(result.attributesToAdd.count_v2.type, 'Int');
		});
	});

	describe('compareTypes', () => {
		it('should return true for matching types', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			assert.strictEqual(manager.compareTypes('String', 'String'), true);
			assert.strictEqual(manager.compareTypes('Int', 'Int'), true);
		});

		it('should return false for different types', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			assert.strictEqual(manager.compareTypes('String', 'Int'), false);
			assert.strictEqual(manager.compareTypes('Float', 'Date'), false);
		});

		it('should handle array types', () => {
			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			assert.strictEqual(manager.compareTypes('[String]', '[String]'), true);
			assert.strictEqual(manager.compareTypes('[String]', 'String'), false);
		});
	});

	describe('ensureTable', () => {
		it('should create new table when it does not exist', async () => {
			const mockBigQueryTable = {
				getMetadata: async () => [
					{
						schema: {
							fields: [
								{ name: 'id', type: 'STRING', mode: 'REQUIRED' },
								{ name: 'name', type: 'STRING', mode: 'NULLABLE' },
							],
						},
					},
				],
			};

			const mockDataset = {
				table: () => mockBigQueryTable,
			};

			const mockBigQueryClient = {
				client: {
					dataset: () => mockDataset,
				},
			};

			let describedTable = null;
			let createdTable = null;
			let createdHashAttribute = null;

			const mockOperationsClient = {
				describeTable: async () => describedTable,
				createTable: async (table, hashAttribute) => {
					createdTable = table;
					createdHashAttribute = hashAttribute;
					return { success: true };
				},
			};

			const manager = new SchemaManager({
				bigQueryClient: mockBigQueryClient,
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			// Override the operations client with our mock
			manager.operationsClient = mockOperationsClient;

			const result = await manager.ensureTable('TestTable', 'test_dataset', 'test_table', 'timestamp');

			assert.strictEqual(result.action, 'created');
			assert.strictEqual(createdTable, 'TestTable');
			assert.strictEqual(createdHashAttribute, 'id');
			assert.ok(result.expectedFields);
			assert.ok(Array.isArray(result.expectedFields));
		});

		it('should return no-op when table exists (Harper handles schema evolution)', async () => {
			const existingSchema = {
				name: 'TestTable',
				attributes: [
					{ attribute: 'id', is_primary_key: true },
					{ attribute: 'name', indexed: true },
				],
			};

			const mockOperationsClient = {
				describeTable: async () => existingSchema,
			};

			const manager = new SchemaManager({
				bigQueryClient: {},
				config: { bigquery: { timestampColumn: 'timestamp' } },
			});

			manager.operationsClient = mockOperationsClient;

			const result = await manager.ensureTable('TestTable', 'test_dataset', 'test_table', 'timestamp');

			assert.strictEqual(result.action, 'none');
			assert.strictEqual(result.table, 'TestTable');
			assert.ok(result.message.includes('automatically'));
		});
	});
});
