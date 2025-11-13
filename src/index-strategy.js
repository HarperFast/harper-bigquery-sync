// ============================================================================
// File: index-strategy.js
// Determines which columns should be indexed in Harper tables

/**
 * Strategy for determining which columns to index
 */
export class IndexStrategy {
	/**
	 * Creates a new IndexStrategy
	 * @param {Object} config - Configuration
	 * @param {string} config.timestampColumn - Timestamp column name
	 */
	constructor(config) {
		this.timestampColumn = config.timestampColumn;
	}

	/**
	 * Determines if a column should be indexed
	 * @param {string} columnName - Column name to check
	 * @returns {boolean} True if column should be indexed
	 */
	shouldIndex(columnName) {
		// Always index timestamp column
		if (columnName === this.timestampColumn) {
			return true;
		}

		// Index columns that end with _id or Id
		if (columnName === 'id') {
			return true;
		}

		if (columnName.endsWith('_id') || columnName.endsWith('Id')) {
			return true;
		}

		return false;
	}

	/**
	 * Gets list of columns to index from schema fields
	 * @param {Array} fields - BigQuery field definitions
	 * @returns {Array<string>} Array of column names to index
	 */
	getIndexes(fields) {
		const indexes = [];

		for (const field of fields) {
			if (this.shouldIndex(field.name)) {
				indexes.push(field.name);
			}
		}

		return indexes;
	}
}
