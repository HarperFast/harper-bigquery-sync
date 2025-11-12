// my plugin entry point
import { globals } from './globals.js';

import { SyncEngine } from './sync-engine.js';
import { getPluginConfig, getTableConfig } from './config-loader.js';
import { ValidationService } from './validation.js';
import { logger } from '@google-cloud/bigquery/build/src/logger.js';

export async function handleApplication(scope) {
    const logger = scope.logger;
    const options = scope.options.getAll();

    // Load and normalize configuration (converts legacy single-table to multi-table format)
    const fullConfig = getPluginConfig(options);

    // Create a SyncEngine for each table
    // NOTE: This is a simple sequential loop for now. In the future, this can easily be
    // refactored to create parallel SyncEngines (one-line change to SyncOrchestrator pattern)
    const syncEngines = [];

    logger.info(`[handleApplication] Initializing sync for ${fullConfig.bigquery.tables.length} tables`);

    for (const tableConfig of fullConfig.bigquery.tables) {
        logger.info(`[handleApplication] Creating SyncEngine for table: ${tableConfig.id} (${tableConfig.table}) -> ${tableConfig.targetTable}`);

        // Get table-specific configuration
        const tableSpecificConfig = getTableConfig(tableConfig.id, fullConfig);

        // Create and initialize SyncEngine for this table
        const syncEngine = new SyncEngine(tableSpecificConfig);
        await syncEngine.initialize();

        syncEngines.push(syncEngine);

        logger.info(`[handleApplication] SyncEngine initialized for table: ${tableConfig.id}`);
    }

    // Store all sync engines in globals
    globals.set('syncEngines', syncEngines);

    // For backward compatibility, also store the first engine as 'syncEngine'
    if (syncEngines.length > 0) {
        globals.set('syncEngine', syncEngines[0]);
    }

    logger.info(`[handleApplication] All SyncEngines initialized (${syncEngines.length} tables)`);

    // Initialize ValidationService with full config (optional - only if config is complete)
    try {
        if (fullConfig.bigquery && fullConfig.bigquery.tables && fullConfig.bigquery.tables.length > 0) {
            const validationService = new ValidationService(fullConfig);
            globals.set('validator', validationService);
            logger.info('[handleApplication] ValidationService initialized');
        } else {
            logger.warn('[handleApplication] ValidationService not initialized - no tables configured');
        }
    } catch (error) {
        logger.warn(`[handleApplication] ValidationService initialization failed: ${error.message}. Validation will be disabled.`);
    }
}