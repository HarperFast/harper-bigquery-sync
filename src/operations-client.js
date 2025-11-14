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

		const data = await response.json();

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
	 * Creates a new table
	 * @param {string} tableName - Table name
	 * @param {Object} attributes - Attribute definitions
	 * @param {Array<string>} indexes - Columns to index
	 * @returns {Promise<Object>} Creation result
	 */
	async createTable(tableName, attributes, indexes = []) {
		try {
			// Build the create_table operation payload
			const operation = {
				operation: 'create_table',
				table: tableName,
				hash_attribute: 'id', // Default primary key
			};

			// Add attributes if provided
			if (attributes && Object.keys(attributes).length > 0) {
				operation.attributes = attributes;
			}

			const result = await this.makeRequest(operation);

			// Create indexes if specified
			if (indexes && indexes.length > 0) {
				for (const indexColumn of indexes) {
					try {
						await this.makeRequest({
							operation: 'create_attribute_index',
							table: tableName,
							attribute: indexColumn,
						});
					} catch (indexError) {
						// Log but don't fail if index creation fails
						console.warn(`Failed to create index on ${indexColumn}:`, indexError.message);
					}
				}
			}

			return {
				success: true,
				table: tableName,
				message: result.message || 'Table created successfully',
			};
		} catch (error) {
			// If table already exists, treat as success
			if (this.isTableExistsError(error)) {
				return {
					success: true,
					table: tableName,
					message: 'Table already exists',
					alreadyExists: true,
				};
			}
			throw error;
		}
	}

	/**
	 * Adds attributes to an existing table
	 * @param {string} tableName - Table name
	 * @param {Object} attributes - New attribute definitions
	 * @returns {Promise<Object>} Result
	 */
	async addAttributes(tableName, attributes) {
		// Harper DB doesn't have a bulk add_attributes operation
		// We need to add each attribute individually using alter table
		const results = [];
		const errors = [];

		for (const [attrName, attrDef] of Object.entries(attributes)) {
			try {
				await this.makeRequest({
					operation: 'alter',
					table: tableName,
					operation_type: 'add_attribute',
					attribute: attrName,
					...attrDef, // Spread type, required, etc.
				});
				results.push(attrName);
			} catch (error) {
				// If attribute already exists, consider it success
				if (this.isAttributeExistsError(error)) {
					results.push(attrName);
				} else {
					errors.push({ attribute: attrName, error: error.message });
				}
			}
		}

		// If any errors occurred that weren't "already exists", throw
		if (errors.length > 0) {
			const error = new Error(`Failed to add some attributes: ${errors.map((e) => e.attribute).join(', ')}`);
			error.details = errors;
			throw error;
		}

		return {
			success: true,
			table: tableName,
			attributesAdded: results,
			message: `Added ${results.length} attribute(s)`,
		};
	}
}
