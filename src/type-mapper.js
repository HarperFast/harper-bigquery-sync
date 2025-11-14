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
		const harperType = typeMap[normalized];

		if (harperType) {
			logger.debug(`[TypeMapper.mapScalarType] Mapped ${bigQueryType} -> ${harperType}`);
		} else {
			logger.warn(`[TypeMapper.mapScalarType] Unsupported type '${bigQueryType}', defaulting to String`);
		}

		return harperType || 'String';
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
		logger.debug(
			`[TypeMapper.mapField] Mapping field '${field.name}' (type: ${field.type}, mode: ${field.mode || 'NULLABLE'})`
		);

		const mode = field.mode || 'NULLABLE';
		const harperType = this.mapScalarType(field.type);

		const result = {
			name: field.name,
			type: harperType,
			required: mode === 'REQUIRED',
			isArray: mode === 'REPEATED',
		};

		logger.debug(
			`[TypeMapper.mapField] Field '${field.name}' mapped to Harper type '${harperType}'${result.isArray ? '[]' : ''}, required: ${result.required}`
		);

		return result;
	}

	/**
	 * Builds Harper table attributes from BigQuery schema
	 * @param {Object} schema - BigQuery table schema
	 * @param {Array} schema.fields - Array of field definitions
	 * @returns {Object} Harper attributes object for Operations API
	 */
	buildTableAttributes(schema) {
		logger.info(
			`[TypeMapper.buildTableAttributes] Building table attributes from ${schema.fields.length} BigQuery fields`
		);

		const attributes = {};

		for (const field of schema.fields) {
			const mapped = this.mapField(field);
			const type = mapped.isArray ? `[${mapped.type}]` : mapped.type;

			attributes[mapped.name] = {
				type,
				required: mapped.required,
			};

			logger.debug(
				`[TypeMapper.buildTableAttributes] Added attribute '${mapped.name}': type=${type}, required=${mapped.required}`
			);
		}

		logger.info(`[TypeMapper.buildTableAttributes] Built ${Object.keys(attributes).length} Harper attributes`);

		return attributes;
	}
}
