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
	 * @param {string} [config.operations.username] - API username
	 * @param {string} [config.operations.password] - API password
	 */
	constructor(config = {}) {
		const opsConfig = config.operations || {};
		this.host = opsConfig.host || 'localhost';
		this.port = opsConfig.port || 9925;
		this.username = opsConfig.username;
		this.password = opsConfig.password;
		this.baseUrl = `http://${this.host}:${this.port}`;
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
	 * Makes an authenticated request to Harper Operations API
	 * @param {Object} operation - Operation payload
	 * @returns {Promise<Object>} Response data
	 * @throws {Error} On HTTP or API errors
	 */
	async makeRequest(operation) {
		const headers = {
			'Content-Type': 'application/json',
		};

		// Add Basic Authentication if credentials provided
		if (this.username && this.password) {
			const credentials = Buffer.from(`${this.username}:${this.password}`).toString('base64');
			headers['Authorization'] = `Basic ${credentials}`;
		}

		const response = await fetch(this.baseUrl, {
			method: 'POST',
			headers,
			body: JSON.stringify(operation),
		});

		// Get response text first for better error handling
		const text = await response.text();
		let data;

		try {
			data = JSON.parse(text);
		} catch {
			const error = new Error(`Invalid JSON response: ${text.substring(0, 200)}`);
			error.statusCode = response.status;
			error.responseText = text;
			throw error;
		}

		// Harper returns error in response body, not HTTP status
		if (data.error) {
			const error = new Error(data.error);
			error.statusCode = response.status;
			throw error;
		}

		if (!response.ok) {
			const error = new Error(`HTTP ${response.status}: ${response.statusText}`);
			error.statusCode = response.status;
			throw error;
		}

		return data;
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
		return (
			(message.includes('attribute') && message.includes('already exists')) || message.includes('column already exists')
		);
	}

	/**
	 * Describes a table (checks if it exists and gets schema)
	 * @param {string} tableName - Table name
	 * @returns {Promise<Object|null>} Table schema or null if not found
	 */
	async describeTable(tableName) {
		try {
			const result = await this.makeRequest({
				operation: 'describe_table',
				table: tableName,
			});

			// Return the schema with attributes
			return {
				name: tableName,
				attributes: result.attributes || {},
			};
		} catch (error) {
			// If table doesn't exist, return null instead of throwing
			if (error.message.includes('does not exist') || error.message.includes('not found')) {
				return null;
			}
			throw error;
		}
	}

	/**
	 * Creates a new table with minimal schema
	 * @param {string} tableName - Table name
	 * @param {string} [hashAttribute='id'] - Primary key attribute name
	 * @returns {Promise<Object>} Creation result
	 *
	 * Note: Harper automatically indexes all fields in schemaless tables.
	 * You can insert any fields during data ingestion - they will be stored
	 * and indexed automatically without pre-defining them.
	 */
	async createTable(tableName, hashAttribute = 'id') {
		try {
			const result = await this.makeRequest({
				operation: 'create_table',
				table: tableName,
				hash_attribute: hashAttribute,
			});

			return {
				success: true,
				table: tableName,
				hashAttribute,
				message: result.message || 'Table created successfully',
			};
		} catch (error) {
			// If table already exists, treat as success
			if (this.isTableExistsError(error)) {
				return {
					success: true,
					table: tableName,
					hashAttribute,
					message: 'Table already exists',
					alreadyExists: true,
				};
			}
			throw error;
		}
	}

	/**
	 * Note: Harper does not support ALTER operations via Operations API.
	 * However, tables created with just hash_attribute automatically accept
	 * any fields during INSERT operations. All inserted fields are automatically
	 * stored and indexed.
	 *
	 * This method is kept for API compatibility but is not needed in practice.
	 * Simply insert records with the desired fields and Harper will handle them.
	 *
	 * @deprecated Harper handles schema evolution automatically via schemaless inserts
	 * @param {string} _tableName - Table name
	 * @param {Object} _attributes - Attribute definitions (not used)
	 * @returns {Promise<Object>} Result indicating no action needed
	 */
	async addAttributes(_tableName, _attributes) {
		// No-op: Harper handles this automatically
		return {
			success: true,
			message: 'Harper handles schema evolution automatically - no explicit addAttributes needed',
			note: 'Simply insert records with new fields and Harper will store and index them automatically',
		};
	}
}
