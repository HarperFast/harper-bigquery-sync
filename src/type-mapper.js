// ============================================================================
// File: type-mapper.js
// Maps BigQuery types to Harper GraphQL types

/**
 * Maps BigQuery types to Harper GraphQL types
 */
export class TypeMapper {
	/**
	 * Maps a BigQuery scalar type to a Harper type
	 * @param {string} bigQueryType - BigQuery type name
	 * @returns {string} Harper type name
	 */
	mapScalarType(bigQueryType) {
		const typeMap = {
			// Numeric types
			INTEGER: 'Int',
			INT64: 'Int',
			FLOAT: 'Float',
			FLOAT64: 'Float',
			NUMERIC: 'Float',
			BIGNUMERIC: 'Float',

			// String types
			STRING: 'String',
			BYTES: 'String',

			// Boolean
			BOOL: 'Boolean',
			BOOLEAN: 'Boolean',

			// Temporal types
			TIMESTAMP: 'Date',
			DATE: 'Date',
			TIME: 'String',
			DATETIME: 'Date',

			// Complex types (stored as JSON)
			RECORD: 'Json',
			STRUCT: 'Json',
			GEOGRAPHY: 'String',
			JSON: 'Json',
		};

		const normalized = bigQueryType.toUpperCase();
		return typeMap[normalized] || 'String';
	}

	/**
	 * Maps a BigQuery field definition to Harper field definition
	 * @param {Object} field - BigQuery field definition
	 * @param {string} field.name - Field name
	 * @param {string} field.type - Field type
	 * @param {string} [field.mode] - Field mode (NULLABLE, REQUIRED, REPEATED)
	 * @returns {Object} Harper field definition
	 */
	mapField(field) {
		const mode = field.mode || 'NULLABLE';
		const harperType = this.mapScalarType(field.type);

		return {
			name: field.name,
			type: harperType,
			required: mode === 'REQUIRED',
			isArray: mode === 'REPEATED',
		};
	}

	/**
	 * Builds Harper table attributes from BigQuery schema
	 * @param {Object} schema - BigQuery table schema
	 * @param {Array} schema.fields - Array of field definitions
	 * @returns {Object} Harper attributes object for Operations API
	 */
	buildTableAttributes(schema) {
		const attributes = {};

		for (const field of schema.fields) {
			const mapped = this.mapField(field);
			const type = mapped.isArray ? `[${mapped.type}]` : mapped.type;

			attributes[mapped.name] = {
				type,
				required: mapped.required,
			};
		}

		return attributes;
	}
}
