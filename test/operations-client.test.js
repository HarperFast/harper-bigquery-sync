/**
 * Tests for operations-client.js
 *
 * Tests Harper Operations API client
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { OperationsClient } from '../src/operations-client.js';

describe('OperationsClient', () => {
	describe('constructor', () => {
		it('should initialize with default config', () => {
			const client = new OperationsClient();

			assert.strictEqual(client.port, 9925);
			assert.strictEqual(client.host, 'localhost');
		});

		it('should accept custom configuration', () => {
			const client = new OperationsClient({
				operations: {
					host: 'custom-host',
					port: 8080,
				},
			});

			assert.strictEqual(client.host, 'custom-host');
			assert.strictEqual(client.port, 8080);
		});
	});

	describe('buildUrl', () => {
		it('should build correct URL for endpoint', () => {
			const client = new OperationsClient();
			const url = client.buildUrl('/describe');

			assert.strictEqual(url, 'http://localhost:9925/describe');
		});

		it('should handle custom host and port', () => {
			const client = new OperationsClient({
				operations: { host: 'example.com', port: 3000 },
			});
			const url = client.buildUrl('/create');

			assert.strictEqual(url, 'http://example.com:3000/create');
		});
	});

	describe('isTableExistsError', () => {
		it('should identify table exists error', () => {
			const client = new OperationsClient();
			const error = new Error('Table already exists');

			assert.strictEqual(client.isTableExistsError(error), true);
		});

		it('should identify duplicate table error', () => {
			const client = new OperationsClient();
			const error = new Error('Duplicate table: TestTable');

			assert.strictEqual(client.isTableExistsError(error), true);
		});

		it('should not identify other errors', () => {
			const client = new OperationsClient();
			const error = new Error('Connection timeout');

			assert.strictEqual(client.isTableExistsError(error), false);
		});

		it('should handle null error', () => {
			const client = new OperationsClient();

			assert.strictEqual(client.isTableExistsError(null), false);
		});
	});

	describe('isAttributeExistsError', () => {
		it('should identify attribute exists error', () => {
			const client = new OperationsClient();
			const error = new Error('Attribute email already exists');

			assert.strictEqual(client.isAttributeExistsError(error), true);
		});

		it('should identify column exists error', () => {
			const client = new OperationsClient();
			const error = new Error('Column already exists: userId');

			assert.strictEqual(client.isAttributeExistsError(error), true);
		});

		it('should not identify other errors', () => {
			const client = new OperationsClient();
			const error = new Error('Invalid type');

			assert.strictEqual(client.isAttributeExistsError(error), false);
		});
	});
});
