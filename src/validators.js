/**
 * Centralized Validation Module
 * Provides validation functions for configuration and data
 */

/**
 * Validates BigQuery configuration
 * @param {Object} config - The bigquery configuration object
 * @throws {Error} If configuration is invalid
 */
export function validateBigQueryConfig(config) {
	if (!config) {
		throw new Error('BigQuery configuration is required');
	}

	const requiredFields = ['projectId', 'dataset', 'table', 'timestampColumn'];
	const missingFields = requiredFields.filter((field) => !config[field]);

	if (missingFields.length > 0) {
		throw new Error(`Missing required BigQuery config fields: ${missingFields.join(', ')}`);
	}

	// Validate credentials path
	if (!config.credentials) {
		throw new Error('BigQuery credentials file path is required');
	}

	return true;
}

/**
 * Validates and normalizes column configuration
 * @param {Array|string|undefined} columns - Column configuration (array, "*", or undefined)
 * @param {string} timestampColumn - The timestamp column name (required in column list)
 * @returns {Array<string>} Normalized column array
 * @throws {Error} If column configuration is invalid
 */
export function validateAndNormalizeColumns(columns, timestampColumn) {
	// Case 1: columns not specified (undefined/null) -> SELECT *
	if (columns === undefined || columns === null) {
		return ['*'];
	}

	// Case 2: columns is "*" string -> SELECT *
	if (columns === '*') {
		return ['*'];
	}

	// Case 3: columns is an array
	if (Array.isArray(columns)) {
		if (columns.length === 0) {
			throw new Error('Column array cannot be empty. Use "*" or omit for SELECT *');
		}

		// Check if array contains only "*"
		if (columns.length === 1 && columns[0] === '*') {
			return ['*'];
		}

		// Validate all columns are strings
		const nonStringColumns = columns.filter((col) => typeof col !== 'string');
		if (nonStringColumns.length > 0) {
			throw new Error('All columns must be strings');
		}

		// Validate no empty strings
		const emptyColumns = columns.filter((col) => col.trim() === '');
		if (emptyColumns.length > 0) {
			throw new Error('Column names cannot be empty strings');
		}

		// Ensure timestamp column is included (unless using SELECT *)
		if (!columns.includes(timestampColumn)) {
			throw new Error(
				`Timestamp column '${timestampColumn}' must be included in columns list. ` +
					`Add it to the array or use "*" to select all columns.`
			);
		}

		// Return trimmed columns
		return columns.map((col) => col.trim());
	}

	// Invalid type
	throw new Error(`Invalid columns type: ${typeof columns}. Expected array of strings, "*", or undefined.`);
}

/**
 * Validates sync configuration
 * @param {Object} syncConfig - The sync configuration object
 * @throws {Error} If sync configuration is invalid
 */
export function validateSyncConfig(syncConfig) {
	if (!syncConfig) {
		throw new Error('Sync configuration is required');
	}

	// Validate batch sizes are positive integers
	const batchSizeFields = ['initialBatchSize', 'catchupBatchSize', 'steadyBatchSize'];
	for (const field of batchSizeFields) {
		if (syncConfig[field] !== undefined) {
			if (!Number.isInteger(syncConfig[field]) || syncConfig[field] <= 0) {
				throw new Error(`${field} must be a positive integer`);
			}
		}
	}

	// Validate thresholds are positive numbers
	const thresholdFields = ['catchupThreshold', 'steadyThreshold'];
	for (const field of thresholdFields) {
		if (syncConfig[field] !== undefined) {
			if (typeof syncConfig[field] !== 'number' || syncConfig[field] <= 0) {
				throw new Error(`${field} must be a positive number`);
			}
		}
	}

	// Validate poll interval
	if (syncConfig.pollInterval !== undefined) {
		if (!Number.isInteger(syncConfig.pollInterval) || syncConfig.pollInterval <= 0) {
			throw new Error('pollInterval must be a positive integer');
		}
	}

	return true;
}

/**
 * Validates retry configuration
 * @param {Object} retryConfig - The retry configuration object
 * @throws {Error} If retry configuration is invalid
 */
export function validateRetryConfig(retryConfig) {
	if (!retryConfig) {
		return true; // Retry config is optional
	}

	if (retryConfig.maxAttempts !== undefined) {
		if (!Number.isInteger(retryConfig.maxAttempts) || retryConfig.maxAttempts < 0) {
			throw new Error('maxAttempts must be a non-negative integer');
		}
	}

	if (retryConfig.backoffMultiplier !== undefined) {
		if (typeof retryConfig.backoffMultiplier !== 'number' || retryConfig.backoffMultiplier <= 0) {
			throw new Error('backoffMultiplier must be a positive number');
		}
	}

	if (retryConfig.initialDelay !== undefined) {
		if (!Number.isInteger(retryConfig.initialDelay) || retryConfig.initialDelay < 0) {
			throw new Error('initialDelay must be a non-negative integer');
		}
	}

	return true;
}

/**
 * Validates the entire configuration object
 * @param {Object} config - The full configuration object
 * @throws {Error} If any part of the configuration is invalid
 */
export function validateFullConfig(config) {
	if (!config) {
		throw new Error('Configuration object is required');
	}

	// Validate BigQuery config
	validateBigQueryConfig(config.bigquery);

	// Validate and normalize columns
	const normalizedColumns = validateAndNormalizeColumns(config.bigquery.columns, config.bigquery.timestampColumn);

	// Validate sync config
	if (config.sync) {
		validateSyncConfig(config.sync);
	}

	// Validate retry config
	if (config.retry) {
		validateRetryConfig(config.retry);
	}

	return {
		isValid: true,
		normalizedColumns,
	};
}

export default {
	validateBigQueryConfig,
	validateAndNormalizeColumns,
	validateSyncConfig,
	validateRetryConfig,
	validateFullConfig,
};
