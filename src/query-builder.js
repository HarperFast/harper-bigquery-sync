/**
 * Query Builder
 * Constructs SQL queries for BigQuery operations with column selection support
 */

/**
 * Formats a column list for SQL SELECT statement
 * @param {Array<string>} columns - Array of column names (or ['*'])
 * @returns {string} Formatted column list for SQL
 * @example
 * formatColumnList(['*']) // returns "*"
 * formatColumnList(['id', 'name', 'timestamp']) // returns "id, name, timestamp"
 */
export function formatColumnList(columns) {
	if (!Array.isArray(columns)) {
		logger.error('[formatColumnList] Invalid input: columns must be an array');
		throw new Error('columns must be an array');
	}

	if (columns.length === 0) {
		logger.error('[formatColumnList] Invalid input: columns array cannot be empty');
		throw new Error('columns array cannot be empty');
	}

	// Special case: ['*'] means SELECT *
	if (columns.length === 1 && columns[0] === '*') {
		logger.debug('[formatColumnList] Using wildcard SELECT *');
		return '*';
	}

	// Format as comma-separated list with proper spacing
	const formatted = columns.join(', ');
	logger.debug(`[formatColumnList] Formatted ${columns.length} columns: ${formatted}`);
	return formatted;
}

/**
 * Builds a SQL query to pull a partition of data from BigQuery
 * Uses modulo-based partitioning for distributed workload
 * @param {Object} options - Query options
 * @param {string} options.dataset - BigQuery dataset name
 * @param {string} options.table - BigQuery table name
 * @param {string} options.timestampColumn - Name of the timestamp column
 * @param {Array<string>} options.columns - Columns to select (or ['*'])
 * @returns {string} SQL query string
 */
export function buildPullPartitionQuery({ dataset, table, timestampColumn, columns }) {
	if (!dataset || !table || !timestampColumn) {
		logger.error(
			'[buildPullPartitionQuery] Missing required parameters: dataset, table, and timestampColumn are required'
		);
		throw new Error('dataset, table, and timestampColumn are required');
	}

	if (!columns || !Array.isArray(columns)) {
		logger.error('[buildPullPartitionQuery] Invalid columns parameter: must be a non-empty array');
		throw new Error('columns must be a non-empty array');
	}

	logger.info(
		`[buildPullPartitionQuery] Building pull query for ${dataset}.${table} with ${columns.length === 1 && columns[0] === '*' ? 'all columns' : `${columns.length} columns`}`
	);

	const columnList = formatColumnList(columns);

	const query = `
    SELECT ${columnList}
    FROM \`${dataset}.${table}\`
    WHERE
      -- guard + normalize types
      CAST(@clusterSize AS INT64) > 0
      AND CAST(@nodeId AS INT64) BETWEEN 0 AND CAST(@clusterSize AS INT64) - 1
      -- sharding
      AND MOD(UNIX_MICROS(${timestampColumn}), CAST(@clusterSize AS INT64)) = CAST(@nodeId AS INT64)
      -- time filter
      AND ${timestampColumn} > TIMESTAMP(@lastTimestamp)
    ORDER BY ${timestampColumn} ASC
    LIMIT CAST(@batchSize AS INT64)
  `;

	logger.debug('[buildPullPartitionQuery] Query construction complete');
	return query;
}

/**
 * Builds a SQL query to count records in a partition
 * @param {Object} options - Query options
 * @param {string} options.dataset - BigQuery dataset name
 * @param {string} options.table - BigQuery table name
 * @param {string} options.timestampColumn - Name of the timestamp column
 * @returns {string} SQL query string
 */
export function buildCountPartitionQuery({ dataset, table, timestampColumn }) {
	if (!dataset || !table || !timestampColumn) {
		throw new Error('dataset, table, and timestampColumn are required');
	}

	return `
    SELECT COUNT(*) as count
    FROM \`${dataset}.${table}\`
    WHERE MOD(
      ABS(FARM_FINGERPRINT(CAST(${timestampColumn} AS STRING))),
      @clusterSize
    ) = @nodeId
  `;
}

/**
 * Builds a SQL query to verify a specific record exists
 * @param {Object} options - Query options
 * @param {string} options.dataset - BigQuery dataset name
 * @param {string} options.table - BigQuery table name
 * @param {string} options.timestampColumn - Name of the timestamp column
 * @returns {string} SQL query string
 */
export function buildVerifyRecordQuery({ dataset, table, timestampColumn }) {
	if (!dataset || !table || !timestampColumn) {
		throw new Error('dataset, table, and timestampColumn are required');
	}

	return `
    SELECT 1
    FROM \`${dataset}.${table}\`
    WHERE ${timestampColumn} = @timestamp
      AND id = @recordId
    LIMIT 1
  `;
}

/**
 * Query Builder class for creating BigQuery SQL queries
 * Encapsulates query construction logic with column selection support
 */
export class QueryBuilder {
	/**
	 * Creates a new QueryBuilder instance
	 * @param {Object} config - BigQuery configuration
	 * @param {string} config.dataset - BigQuery dataset name
	 * @param {string} config.table - BigQuery table name
	 * @param {string} config.timestampColumn - Name of the timestamp column
	 * @param {Array<string>} config.columns - Columns to select (defaults to ['*'])
	 */
	constructor({ dataset, table, timestampColumn, columns = ['*'] }) {
		if (!dataset || !table || !timestampColumn) {
			logger.error('[QueryBuilder] Missing required parameters: dataset, table, and timestampColumn are required');
			throw new Error('dataset, table, and timestampColumn are required');
		}

		this.dataset = dataset;
		this.table = table;
		this.timestampColumn = timestampColumn;
		this.columns = columns;

		logger.info(
			`[QueryBuilder] Initialized for ${dataset}.${table} with timestamp column '${timestampColumn}' and ${columns.length === 1 && columns[0] === '*' ? 'all columns' : `${columns.length} columns`}`
		);
	}

	/**
	 * Builds query to pull a partition of data
	 * @returns {string} SQL query string
	 */
	buildPullPartitionQuery() {
		return buildPullPartitionQuery({
			dataset: this.dataset,
			table: this.table,
			timestampColumn: this.timestampColumn,
			columns: this.columns,
		});
	}

	/**
	 * Builds query to count records in a partition
	 * @returns {string} SQL query string
	 */
	buildCountPartitionQuery() {
		return buildCountPartitionQuery({
			dataset: this.dataset,
			table: this.table,
			timestampColumn: this.timestampColumn,
		});
	}

	/**
	 * Builds query to verify a specific record exists
	 * @returns {string} SQL query string
	 */
	buildVerifyRecordQuery() {
		return buildVerifyRecordQuery({
			dataset: this.dataset,
			table: this.table,
			timestampColumn: this.timestampColumn,
		});
	}

	/**
	 * Gets the formatted column list for logging/debugging
	 * @returns {string} Formatted column list
	 */
	getColumnList() {
		return formatColumnList(this.columns);
	}
}

export default QueryBuilder;
