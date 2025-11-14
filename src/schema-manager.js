// ============================================================================
// File: schema-manager.js
// Manages Harper table schemas based on BigQuery schemas

import { TypeMapper } from './type-mapper.js';
import { IndexStrategy } from './index-strategy.js';
import { OperationsClient } from './operations-client.js';

/**
 * Manages Harper table creation and schema migrations
 */
export class SchemaManager {
	/**
	 * Creates a new SchemaManager
	 * @param {Object} options - Options
	 * @param {Object} options.bigQueryClient - BigQuery client instance
	 * @param {Object} options.config - Configuration object
	 */
	constructor(options) {
		if (!options.bigQueryClient) {
			throw new Error('bigQueryClient is required');
		}
		if (!options.config) {
			throw new Error('config is required');
		}

		this.bigQueryClient = options.bigQueryClient;
		this.config = options.config;

		// Initialize supporting components
		this.typeMapper = new TypeMapper();
		this.indexStrategy = new IndexStrategy({
			timestampColumn: options.config.bigquery.timestampColumn,
		});
		this.operationsClient = new OperationsClient(options.config);
	}

	/**
	 * Compares two type strings for equality
	 * @param {string} type1 - First type
	 * @param {string} type2 - Second type
	 * @returns {boolean} True if types match
	 */
	compareTypes(type1, type2) {
		return type1 === type2;
	}

	/**
	 * Determines what migration actions are needed
	 * @param {Object|null} harperSchema - Current Harper schema (null if table doesn't exist)
	 * @param {Object} bigQuerySchema - BigQuery schema
	 * @returns {Object} Migration plan
	 */
	determineMigrationNeeds(harperSchema, bigQuerySchema) {
		// Build target attributes from BigQuery schema
		const targetAttributes = this.typeMapper.buildTableAttributes(bigQuerySchema);

		// If table doesn't exist, create it
		if (!harperSchema) {
			return {
				action: 'create',
				attributesToAdd: targetAttributes,
			};
		}

		// Find attributes that need to be added
		const attributesToAdd = {};
		const existingAttrs = harperSchema.attributes || {};

		for (const [name, targetAttr] of Object.entries(targetAttributes)) {
			if (!existingAttrs[name]) {
				// New attribute
				attributesToAdd[name] = targetAttr;
			} else {
				// Check for type changes
				const existingAttr = existingAttrs[name];
				if (!this.compareTypes(existingAttr.type, targetAttr.type)) {
					// Type changed - create versioned column
					const versionedName = `${name}_v2`;
					attributesToAdd[versionedName] = targetAttr;
				}
			}
		}

		// Determine action
		if (Object.keys(attributesToAdd).length === 0) {
			return {
				action: 'none',
				attributesToAdd: {},
			};
		}

		return {
			action: 'migrate',
			attributesToAdd,
		};
	}

	/**
	 * Ensures a Harper table exists for BigQuery data
	 * @param {string} harperTableName - Harper table name
	 * @param {string} bigQueryDataset - BigQuery dataset (for documentation)
	 * @param {string} bigQueryTable - BigQuery table name (for documentation)
	 * @param {string} _timestampColumn - Timestamp column name (for future use)
	 * @returns {Promise<Object>} Result of ensure operation
	 *
	 * Note: Harper automatically indexes all fields in schemaless tables.
	 * Simply create the table once, then insert BigQuery data - all fields
	 * will be stored and indexed automatically without pre-definition.
	 */
	async ensureTable(harperTableName, bigQueryDataset, bigQueryTable, _timestampColumn) {
		// 1. Check if Harper table exists
		const harperSchema = await this.operationsClient.describeTable(harperTableName);

		if (harperSchema) {
			// Table exists - Harper handles schema evolution automatically
			return {
				action: 'none',
				table: harperTableName,
				message: 'Table exists - Harper will handle any new fields automatically during insert',
			};
		}

		// 2. Get BigQuery schema for documentation
		const bqTable = this.bigQueryClient.client.dataset(bigQueryDataset).table(bigQueryTable);
		const [metadata] = await bqTable.getMetadata();
		const bigQuerySchema = metadata.schema;

		// Build expected attributes for documentation
		const expectedAttributes = this.typeMapper.buildTableAttributes(bigQuerySchema);

		// 3. Create table with minimal schema (just primary key)
		// Harper will auto-index all fields inserted later
		await this.operationsClient.createTable(harperTableName, 'id');

		return {
			action: 'created',
			table: harperTableName,
			hashAttribute: 'id',
			expectedFields: Object.keys(expectedAttributes),
			message: 'Table created - all BigQuery fields will be automatically indexed on insert',
		};
	}
}
