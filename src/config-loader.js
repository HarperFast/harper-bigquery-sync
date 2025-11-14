/**
 * Configuration Loader
 * Loads and parses the config.yaml file for both the plugin and synthesizer
 */

import { readFileSync } from 'fs';
import { parse } from 'yaml';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { validateFullConfig as _validateFullConfig, validateAndNormalizeColumns } from './validators.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Load configuration from config.yaml or accept a config object
 * @param {string|Object|null} configPath - Path to config file or config object or options object
 * @returns {Object} Parsed and normalized configuration object
 * @throws {Error} If config file cannot be read or parsed
 */
export function loadConfig(configPath = null) {
	try {
		let config;
		let source;

		// Handle different input types
		if (configPath === null || configPath === undefined) {
			// Default to config.yaml in project root
			const path = join(__dirname, '..', 'config.yaml');
			logger.debug(`[ConfigLoader.loadConfig] Loading config from default path: ${path}`);
			const fileContent = readFileSync(path, 'utf8');
			config = parse(fileContent);
			source = path;
		} else if (typeof configPath === 'string') {
			// Path to config file
			logger.debug(`[ConfigLoader.loadConfig] Loading config from: ${configPath}`);
			const fileContent = readFileSync(configPath, 'utf8');
			config = parse(fileContent);
			source = configPath;
		} else if (typeof configPath === 'object') {
			// Config object passed directly (for testing)
			logger.debug('[ConfigLoader.loadConfig] Using config object passed directly');
			// Check if it's an options object with 'config' property
			if (configPath.config) {
				config = configPath.config;
			} else {
				config = configPath;
			}
			source = 'object';
		} else {
			throw new Error('configPath must be a string, object, or null');
		}

		if (!config) {
			logger.error('[ConfigLoader.loadConfig] Failed to parse configuration');
			throw new Error('Failed to parse configuration');
		}

		logger.info(`[ConfigLoader.loadConfig] Successfully loaded config from: ${source}`);

		// Normalize to multi-table format if needed
		return normalizeConfig(config);
	} catch (error) {
		logger.error(`[ConfigLoader.loadConfig] Configuration loading failed: ${error.message}`);
		throw new Error(`Failed to load configuration: ${error.message}`);
	}
}

/**
 * Normalizes configuration to multi-table format
 * Converts legacy single-table configs to multi-table format
 * @param {Object} config - Raw configuration object
 * @returns {Object} Normalized configuration
 * @private
 */
function normalizeConfig(config) {
	if (!config.bigquery) {
		logger.error('[ConfigLoader.normalizeConfig] bigquery section missing in configuration');
		throw new Error('bigquery section missing in configuration');
	}

	// Check if already in multi-table format
	if (config.bigquery.tables && Array.isArray(config.bigquery.tables)) {
		logger.info(
			`[ConfigLoader.normalizeConfig] Config already in multi-table format with ${config.bigquery.tables.length} tables`
		);
		// Validate multi-table configuration
		validateMultiTableConfig(config);
		return config;
	}

	// Legacy single-table format - wrap in tables array
	logger.info('[ConfigLoader.normalizeConfig] Converting legacy single-table config to multi-table format');
	const legacyBigQueryConfig = config.bigquery;

	// Extract table-specific config
	const tableConfig = {
		id: 'default',
		dataset: legacyBigQueryConfig.dataset,
		table: legacyBigQueryConfig.table,
		timestampColumn: legacyBigQueryConfig.timestampColumn,
		columns: legacyBigQueryConfig.columns || ['*'],
		targetTable: 'VesselPositions', // Default Harper table name
		sync: {
			initialBatchSize: config.sync?.initialBatchSize,
			catchupBatchSize: config.sync?.catchupBatchSize,
			steadyBatchSize: config.sync?.steadyBatchSize,
		},
	};

	logger.debug(
		`[ConfigLoader.normalizeConfig] Created table config: ${tableConfig.dataset}.${tableConfig.table} -> ${tableConfig.targetTable}`
	);

	// Create normalized multi-table config
	const normalizedConfig = {
		operations: config.operations, // Preserve operations config if present
		bigquery: {
			projectId: legacyBigQueryConfig.projectId,
			credentials: legacyBigQueryConfig.credentials,
			location: legacyBigQueryConfig.location,
			tables: [tableConfig],
		},
		sync: {
			pollInterval: config.sync?.pollInterval,
			catchupThreshold: config.sync?.catchupThreshold,
			steadyThreshold: config.sync?.steadyThreshold,
		},
	};

	logger.info('[ConfigLoader.normalizeConfig] Successfully normalized config to multi-table format');
	return normalizedConfig;
}

/**
 * Validates multi-table configuration
 * @param {Object} config - Configuration to validate
 * @throws {Error} If configuration is invalid
 * @private
 */
function validateMultiTableConfig(config) {
	logger.debug('[ConfigLoader.validateMultiTableConfig] Validating multi-table configuration');

	if (!config.bigquery.tables || !Array.isArray(config.bigquery.tables)) {
		logger.error('[ConfigLoader.validateMultiTableConfig] bigquery.tables must be an array');
		throw new Error('bigquery.tables must be an array');
	}

	if (config.bigquery.tables.length === 0) {
		logger.error('[ConfigLoader.validateMultiTableConfig] bigquery.tables array cannot be empty');
		throw new Error('bigquery.tables array cannot be empty');
	}

	const tableIds = new Set();
	const targetTables = new Set();

	for (const table of config.bigquery.tables) {
		// Check required fields
		if (!table.id) {
			logger.error('[ConfigLoader.validateMultiTableConfig] Missing required field: table.id');
			throw new Error('Missing required field: table.id');
		}
		if (!table.dataset) {
			logger.error(`[ConfigLoader.validateMultiTableConfig] Missing 'dataset' for table: ${table.id}`);
			throw new Error(`Missing required field 'dataset' for table: ${table.id}`);
		}
		if (!table.table) {
			logger.error(`[ConfigLoader.validateMultiTableConfig] Missing 'table' for table: ${table.id}`);
			throw new Error(`Missing required field 'table' for table: ${table.id}`);
		}
		if (!table.timestampColumn) {
			logger.error(`[ConfigLoader.validateMultiTableConfig] Missing 'timestampColumn' for table: ${table.id}`);
			throw new Error(`Missing required field 'timestampColumn' for table: ${table.id}`);
		}
		if (!table.targetTable) {
			logger.error(`[ConfigLoader.validateMultiTableConfig] Missing 'targetTable' for table: ${table.id}`);
			throw new Error(`Missing required field 'targetTable' for table: ${table.id}`);
		}

		// Check for duplicate IDs
		if (tableIds.has(table.id)) {
			logger.error(`[ConfigLoader.validateMultiTableConfig] Duplicate table ID: ${table.id}`);
			throw new Error(`Duplicate table ID: ${table.id}`);
		}
		tableIds.add(table.id);

		// Check for duplicate target Harper tables
		if (targetTables.has(table.targetTable)) {
			logger.error(
				`[ConfigLoader.validateMultiTableConfig] Duplicate targetTable '${table.targetTable}' for: ${table.id}`
			);
			throw new Error(
				`Duplicate targetTable '${table.targetTable}' for table: ${table.id}. ` +
					`Each BigQuery table must sync to a DIFFERENT Harper table. ` +
					`Multiple BigQuery tables syncing to the same targetTable will cause record ID collisions, ` +
					`validation failures, and checkpoint confusion. If you need combined data, sync to separate ` +
					`tables and join at query time.`
			);
		}
		targetTables.add(table.targetTable);

		logger.debug(
			`[ConfigLoader.validateMultiTableConfig] Validated table: ${table.id} (${table.dataset}.${table.table} -> ${table.targetTable})`
		);
	}

	logger.info(
		`[ConfigLoader.validateMultiTableConfig] Successfully validated ${config.bigquery.tables.length} table configurations`
	);
}

/**
 * Get BigQuery configuration for the synthesizer
 * Uses bigquery section as primary config, with optional synthesizer overrides
 * @param {Object|null} config - Optional pre-loaded configuration
 * @returns {Object} BigQuery configuration for the synthesizer
 * @throws {Error} If bigquery section is missing
 */
export function getSynthesizerConfig(config = null) {
	const fullConfig = config || loadConfig();

	if (!fullConfig.bigquery) {
		throw new Error('bigquery section missing in config.yaml');
	}

	// Use bigquery settings as defaults, with optional synthesizer overrides
	const synthConfig = {
		// BigQuery connection (from bigquery section)
		projectId: fullConfig.bigquery.projectId,
		credentials: fullConfig.bigquery.credentials,
		location: fullConfig.bigquery.location || 'US',

		// Target dataset/table: Use bigquery settings by default, synthesizer overrides if present
		datasetId: fullConfig.synthesizer?.dataset || fullConfig.bigquery.dataset,
		tableId: fullConfig.synthesizer?.table || fullConfig.bigquery.table,

		// Data generation settings (from synthesizer section with defaults)
		totalVessels: fullConfig.synthesizer?.totalVessels || 100000,
		batchSize: fullConfig.synthesizer?.batchSize || 100,
		generationIntervalMs: fullConfig.synthesizer?.generationIntervalMs || 60000,

		// Data retention (from synthesizer section with defaults)
		retentionDays: fullConfig.synthesizer?.retentionDays || 30,
		cleanupIntervalHours: fullConfig.synthesizer?.cleanupIntervalHours || 24,
	};

	// Include multi-table config if available (for CLI to detect mode)
	if (fullConfig.bigquery.tables && Array.isArray(fullConfig.bigquery.tables)) {
		synthConfig.multiTableConfig = fullConfig.bigquery.tables;
	}

	return synthConfig;
}

/**
 * Get BigQuery configuration for the plugin
 * Returns multi-table configuration with validated and normalized columns
 * @param {Object|null} config - Optional pre-loaded configuration
 * @returns {Object} Validated multi-table BigQuery configuration
 * @throws {Error} If configuration is invalid
 */
export function getPluginConfig(config = null) {
	const fullConfig = config || loadConfig();

	if (!fullConfig || !fullConfig.bigquery) {
		throw new Error(
			'BigQuery configuration missing. Please ensure your config.yaml has a "bigquery" section ' +
				'with required fields: projectId, credentials, dataset, table, timestampColumn. ' +
				'See documentation for configuration examples.'
		);
	}

	// If tables is not present, the config needs to be normalized first
	if (!fullConfig.bigquery.tables) {
		// Run normalization to convert legacy format to multi-table
		try {
			const normalizedConfig = normalizeConfig(fullConfig);
			return getPluginConfig(normalizedConfig);
		} catch (error) {
			throw new Error(
				`Failed to normalize configuration: ${error.message}. ` +
					'Please check that your config has required fields: dataset, table, timestampColumn, columns.'
			);
		}
	}

	// Config is already normalized to multi-table format by loadConfig
	// Validate and normalize columns for each table
	const tablesWithNormalizedColumns = fullConfig.bigquery.tables.map((table) => {
		const normalizedColumns = validateAndNormalizeColumns(table.columns, table.timestampColumn);

		return {
			...table,
			columns: normalizedColumns,
		};
	});

	return {
		bigquery: {
			projectId: fullConfig.bigquery.projectId,
			credentials: fullConfig.bigquery.credentials,
			location: fullConfig.bigquery.location || 'US',
			tables: tablesWithNormalizedColumns,
		},
		sync: fullConfig.sync,
	};
}

/**
 * Get configuration for a specific table
 * @param {string} tableId - Table ID to get config for
 * @param {Object|null} config - Optional pre-loaded configuration
 * @returns {Object} Table-specific configuration
 * @throws {Error} If table not found
 */
export function getTableConfig(tableId, config = null) {
	const fullConfig = getPluginConfig(config);

	const tableConfig = fullConfig.bigquery.tables.find((t) => t.id === tableId);

	if (!tableConfig) {
		throw new Error(`Table configuration not found for ID: ${tableId}`);
	}

	return {
		bigquery: {
			projectId: fullConfig.bigquery.projectId,
			dataset: tableConfig.dataset,
			table: tableConfig.table,
			timestampColumn: tableConfig.timestampColumn,
			columns: tableConfig.columns,
			credentials: fullConfig.bigquery.credentials,
			location: fullConfig.bigquery.location,
		},
		sync: {
			...fullConfig.sync,
			...tableConfig.sync, // Table-specific sync settings override global
		},
		tableId: tableConfig.id,
		targetTable: tableConfig.targetTable,
	};
}

export default {
	loadConfig,
	getSynthesizerConfig,
	getPluginConfig,
	getTableConfig,
};
