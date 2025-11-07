/**
 * Configuration Loader
 * Loads and parses the config.yaml file for both the plugin and synthesizer
 */

import { readFileSync } from 'fs';
import { parse } from 'yaml';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Load configuration from config.yaml
 */
export function loadConfig(configPath = null) {
  try {
    // Default to config.yaml in project root
    const path = configPath || join(__dirname, '..', 'config.yaml');
    const fileContent = readFileSync(path, 'utf8');
    const config = parse(fileContent);

    if (!config) {
      throw new Error('Failed to parse config.yaml');
    }

    return config;
  } catch (error) {
    throw new Error(`Failed to load configuration: ${error.message}`);
  }
}

/**
 * Get BigQuery configuration for the synthesizer
 * Uses bigquery section as primary config, with optional synthesizer overrides
 */
export function getSynthesizerConfig(config = null) {
  const fullConfig = config || loadConfig();

  if (!fullConfig.bigquery) {
    throw new Error('bigquery section missing in config.yaml');
  }

  // Use bigquery settings as defaults, with optional synthesizer overrides
  return {
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
    cleanupIntervalHours: fullConfig.synthesizer?.cleanupIntervalHours || 24
  };
}

/**
 * Get BigQuery configuration for the plugin (for reference)
 */
export function getPluginConfig(config = null) {
  const fullConfig = config || loadConfig();

  if (!fullConfig.bigquery) {
    throw new Error('bigquery section missing in config.yaml');
  }

  return {
    projectId: fullConfig.bigquery.projectId,
    dataset: fullConfig.bigquery.dataset,
    table: fullConfig.bigquery.table,
    timestampColumn: fullConfig.bigquery.timestampColumn,
    credentials: fullConfig.bigquery.credentials,
    location: fullConfig.bigquery.location || 'US'
  };
}

export default {
  loadConfig,
  getSynthesizerConfig,
  getPluginConfig
};
