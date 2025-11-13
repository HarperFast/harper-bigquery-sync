// ============================================================================
// Harper BigQuery Sync Component - Production Implementation
// Learn more: https://harperdb.io | https://docs.harperdb.io
// Deploy easily on Fabric: https://fabric.harper.fast
// ============================================================================

// File: resources.js
// Component entry point with resource definitions

/* global tables, Resource */
import { globals } from './globals.js';

// Main data table resource
export class BigQueryData extends tables.BigQueryData {
	async get(id) {
		logger.debug(`[BigQueryData.get] Fetching record with id: ${id}`);
		const result = await super.get(id);
		logger.debug(`[BigQueryData.get] Record ${result ? 'found' : 'not found'}`);
		return result;
	}

	async search(params) {
		// This allows us to search on dynamic attributes.
		params.allowConditionsOnDynamicAttributes = true;
		logger.debug(`[BigQueryData.search] Searching with params: ${JSON.stringify(params).substring(0, 200)}`);
		const results = await super.search(params);
		logger.info(`[BigQueryData.search] Search returned ${results.length} records`);
		return results;
	}
}

// Checkpoint resource
export class SyncCheckpoint extends tables.SyncCheckpoint {
	async getForNode(nodeId) {
		logger.debug(`[SyncCheckpoint.getForNode] Fetching checkpoint for nodeId: ${nodeId}`);
		const checkpoint = await super.get(nodeId);
		logger.debug(`[SyncCheckpoint.getForNode] Checkpoint ${checkpoint ? 'found' : 'not found'}`);
		return checkpoint;
	}

	async updateCheckpoint(nodeId, data) {
		logger.info(`[SyncCheckpoint.updateCheckpoint] Updating checkpoint for nodeId: ${nodeId}`);
		logger.debug(`[SyncCheckpoint.updateCheckpoint] Data: ${JSON.stringify(data).substring(0, 200)}`);
		const result = await super.put({ nodeId, ...data });
		logger.info(`[SyncCheckpoint.updateCheckpoint] Checkpoint updated successfully`);
		return result;
	}
}

// Audit resource
export class SyncAudit extends tables.SyncAudit {
	async getRecent(hours = 24) {
		logger.debug(`[SyncAudit.getRecent] Fetching audit records from last ${hours} hours`);
		const cutoff = new Date(Date.now() - hours * 3600000).toISOString();
		logger.debug(`[SyncAudit.getRecent] Cutoff timestamp: ${cutoff}`);
		const results = await super.search({
			conditions: [{ timestamp: { $gt: cutoff } }],
			orderBy: 'timestamp DESC',
		});
		logger.info(`[SyncAudit.getRecent] Retrieved ${results.length} audit records`);
		return results;
	}
}

// Control endpoint
export class SyncControl extends Resource {
	async get() {
		logger.debug('[SyncControl.get] Status request received');
		const status = await globals.get('syncEngine').getStatus();
		const response = {
			status,
			uptime: process.uptime(),
			version: '1.0.0',
		};
		logger.info(`[SyncControl.get] Returning status - running: ${status.running}, phase: ${status.phase}`);
		return response;
	}

	async post({ action }) {
		logger.info(`[SyncControl.post] Control action received: ${action}`);
		switch (action) {
			case 'start':
				logger.info('[SyncControl.post] Starting sync engine');
				await globals.get('syncEngine').start();
				logger.info('[SyncControl.post] Sync engine started successfully');
				return { message: 'Sync started' };
			case 'stop':
				logger.info('[SyncControl.post] Stopping sync engine');
				await globals.get('syncEngine').stop();
				logger.info('[SyncControl.post] Sync engine stopped successfully');
				return { message: 'Sync stopped' };
			// TODO: Validation not yet implemented - requires additional testing
			// case 'validate':
			//   logger.info('[SyncControl.post] Triggering validation');
			//   await globals.get('validator').runValidation();
			//   logger.info('[SyncControl.post] Validation completed');
			//   return { message: 'Validation triggered' };
			default:
				logger.warn(`[SyncControl.post] Unknown action requested: ${action}`);
				throw new Error(`Unknown action: ${action}`);
		}
	}
}
