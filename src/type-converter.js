/**
 * Type Converter
 * Converts BigQuery-specific types to JavaScript primitives and Date objects
 */

/**
 * Checks if a value is a BigQuery timestamp type
 * @param {*} value - Value to check
 * @returns {boolean} True if value is a BigQuery timestamp type
 * @private
 */
function isBigQueryTimestamp(value) {
	if (!value || typeof value !== 'object') return false;

	const constructorName = value.constructor?.name;
	return ['BigQueryTimestamp', 'BigQueryDatetime', 'BigQueryDate'].includes(constructorName);
}

/**
 * Checks if a string matches ISO 8601 date format
 * @param {string} str - String to check
 * @returns {boolean} True if string looks like an ISO date
 * @private
 */
function looksLikeISODate(str) {
	return typeof str === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(str);
}

/**
 * Converts a BigQuery timestamp object to a JavaScript Date
 * @param {Object} value - BigQuery timestamp object
 * @returns {Date|*} Date object if conversion succeeds, original value otherwise
 */
export function convertBigQueryTimestamp(value) {
	// Try .value property (contains ISO string)
	if (value.value) {
		return new Date(value.value);
	}

	// Try .toJSON() method
	if (typeof value.toJSON === 'function') {
		const jsonValue = value.toJSON();
		return new Date(jsonValue);
	}

	// Unable to convert
	return value;
}

/**
 * Converts a BigInt to Number or String based on size
 * @param {BigInt} value - BigInt value to convert
 * @returns {number|string} Number if within safe integer range, String otherwise
 */
export function convertBigInt(value) {
	if (value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER) {
		return Number(value);
	}
	return value.toString();
}

/**
 * Converts a single value from BigQuery format to JavaScript format
 * @param {*} value - Value to convert
 * @returns {*} Converted value
 */
export function convertValue(value) {
	// Handle null/undefined
	if (value === null || value === undefined) {
		return value;
	}

	// Handle BigInt
	if (typeof value === 'bigint') {
		return convertBigInt(value);
	}

	// Handle objects
	if (typeof value === 'object') {
		// BigQuery timestamp types
		if (isBigQueryTimestamp(value)) {
			return convertBigQueryTimestamp(value);
		}

		// Already a Date object
		if (value instanceof Date) {
			return value;
		}

		// Object with toJSON method
		if (typeof value.toJSON === 'function') {
			const jsonValue = value.toJSON();

			// If it looks like an ISO date, convert to Date
			if (looksLikeISODate(jsonValue)) {
				return new Date(jsonValue);
			}

			return jsonValue;
		}

		// Other objects - keep as-is
		return value;
	}

	// Primitive types - keep as-is
	return value;
}

/**
 * Converts BigQuery record types to JavaScript primitives
 * All timestamp/datetime types are converted to Date objects for Harper's timestamp type
 * @param {Object} record - Record with BigQuery types
 * @returns {Object} Record with converted types
 */
export function convertBigQueryTypes(record) {
	if (!record || typeof record !== 'object') {
		throw new Error('Record must be an object');
	}

	const converted = {};

	for (const [key, value] of Object.entries(record)) {
		converted[key] = convertValue(value);
	}

	return converted;
}

/**
 * Converts an array of BigQuery records
 * @param {Array<Object>} records - Array of records to convert
 * @returns {Array<Object>} Array of converted records
 */
export function convertBigQueryRecords(records) {
	if (!Array.isArray(records)) {
		throw new Error('Records must be an array');
	}

	return records.map((record) => convertBigQueryTypes(record));
}

export default {
	convertBigQueryTypes,
	convertBigQueryRecords,
	convertValue,
	convertBigInt,
	convertBigQueryTimestamp,
};
