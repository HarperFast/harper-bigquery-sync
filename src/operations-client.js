// ============================================================================
// File: operations-client.js
// Client for Harper Operations API

/**
 * Client for interacting with Harper Operations API
 * Provides table creation and schema modification capabilities
 */
export class OperationsClient {
	/**
	 * Creates a new OperationsClient
	 * @param {Object} [config] - Configuration
	 * @param {Object} [config.operations] - Operations API configuration
	 * @param {string} [config.operations.host] - API host (default: localhost)
	 * @param {number} [config.operations.port] - API port (default: 9925)
	 */
	constructor(config = {}) {
		const opsConfig = config.operations || {};
		this.host = opsConfig.host || 'localhost';
		this.port = opsConfig.port || 9925;
	}

	/**
	 * Builds full URL for an API endpoint
	 * @param {string} endpoint - API endpoint path
	 * @returns {string} Full URL
	 */
	buildUrl(endpoint) {
		return `http://${this.host}:${this.port}${endpoint}`;
	}

	/**
	 * Checks if error indicates table already exists
	 * @param {Error} error - Error to check
	 * @returns {boolean} True if table exists error
	 */
	isTableExistsError(error) {
		if (!error || !error.message) return false;

		const message = error.message.toLowerCase();
		return message.includes('already exists') || message.includes('duplicate table');
	}

	/**
	 * Checks if error indicates attribute/column already exists
	 * @param {Error} error - Error to check
	 * @returns {boolean} True if attribute exists error
	 */
	isAttributeExistsError(error) {
		if (!error || !error.message) return false;

		const message = error.message.toLowerCase();
		return message.includes('attribute') && message.includes('already exists') ||
		       message.includes('column already exists');
	}

	/**
	 * Describes a table (checks if it exists and gets schema)
	 * @param {string} _tableName - Table name
	 * @returns {Promise<Object|null>} Table schema or null if not found
	 */
	async describeTable(_tableName) {
		// Implementation will be added when we have actual API to test against
		throw new Error('Not implemented yet');
	}

	/**
	 * Creates a new table
	 * @param {string} _tableName - Table name
	 * @param {Object} _attributes - Attribute definitions
	 * @param {Array<string>} _indexes - Columns to index
	 * @returns {Promise<Object>} Creation result
	 */
	async createTable(_tableName, _attributes, _indexes) {
		// Implementation will be added when we have actual API to test against
		throw new Error('Not implemented yet');
	}

	/**
	 * Adds attributes to an existing table
	 * @param {string} _tableName - Table name
	 * @param {Object} _attributes - New attribute definitions
	 * @returns {Promise<Object>} Result
	 */
	async addAttributes(_tableName, _attributes) {
		// Implementation will be added when we have actual API to test against
		throw new Error('Not implemented yet');
	}
}
