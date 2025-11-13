// ============================================================================
// File: validation.js
// Validation service for data integrity checks
// NOTE: Avoids count-based validation since Harper counts are estimates
//
// ⚠️  WARNING: This validation service is not yet fully implemented and tested.
// ⚠️  It is currently disabled in the plugin. See TODO comments in:
// ⚠️  - src/index.js
// ⚠️  - src/resources.js
// ⚠️  - src/sync-engine.js

/* global harperCluster, tables */

import { BigQueryClient } from './bigquery-client.js';
import { createHash } from 'node:crypto';

export class ValidationService {
	constructor(config) {
		this.config = config;
		logger.info('[ValidationService] Constructor called - initializing validation service');
		this.bigqueryClient = new BigQueryClient(config);
		logger.debug('[ValidationService] BigQuery client initialized for validation');
	}

	async runValidation() {
		logger.info('[ValidationService.runValidation] Starting validation suite');

		const results = {
			timestamp: new Date().toISOString(),
			checks: {},
		};

		try {
			// 1. Checkpoint progress monitoring
			logger.debug('[ValidationService.runValidation] Running checkpoint progress validation');
			results.checks.progress = await this.validateProgress();
			logger.info(`[ValidationService.runValidation] Progress check complete: ${results.checks.progress.status}`);

			// 2. Smoke test - can we query recent data?
			logger.debug('[ValidationService.runValidation] Running smoke test');
			results.checks.smokeTest = await this.smokeTest();
			logger.info(`[ValidationService.runValidation] Smoke test complete: ${results.checks.smokeTest.status}`);

			// 3. Spot check random records
			logger.debug('[ValidationService.runValidation] Running spot check');
			results.checks.spotCheck = await this.spotCheckRecords();
			logger.info(`[ValidationService.runValidation] Spot check complete: ${results.checks.spotCheck.status}`);

			// Determine overall status
			const allHealthy = Object.values(results.checks).every(
				(check) => check.status === 'healthy' || check.status === 'ok'
			);

			results.overallStatus = allHealthy ? 'healthy' : 'issues_detected';
			logger.info(`[ValidationService.runValidation] Overall validation status: ${results.overallStatus}`);

			// Log to audit table
			logger.debug('[ValidationService.runValidation] Logging validation results to audit table');
			await this.logAudit(results);

			return results;
		} catch (error) {
			logger.error(`[ValidationService.runValidation] Validation failed: ${error.message}`, error);
			results.overallStatus = 'error';
			results.error = error.message;
			await this.logAudit(results);
			throw error;
		}
	}

	async validateProgress() {
		logger.debug('[ValidationService.validateProgress] Validating checkpoint progress');
		const clusterInfo = await this.discoverCluster();
		logger.debug(
			`[ValidationService.validateProgress] Cluster info - nodeId: ${clusterInfo.nodeId}, clusterSize: ${clusterInfo.clusterSize}`
		);

		const checkpoint = await tables.SyncCheckpoint.get(clusterInfo.nodeId);

		if (!checkpoint) {
			logger.warn('[ValidationService.validateProgress] No checkpoint found - node may not have started');
			return {
				status: 'no_checkpoint',
				message: 'No checkpoint found - node may not have started',
			};
		}

		logger.debug(
			`[ValidationService.validateProgress] Checkpoint found - lastTimestamp: ${checkpoint.lastTimestamp}, recordsIngested: ${checkpoint.recordsIngested}`
		);

		const timeSinceLastSync = Date.now() - new Date(checkpoint.lastSyncTime).getTime();
		const lagSeconds = (Date.now() - new Date(checkpoint.lastTimestamp).getTime()) / 1000;

		logger.debug(
			`[ValidationService.validateProgress] Time since last sync: ${timeSinceLastSync}ms, lag: ${lagSeconds.toFixed(2)}s`
		);

		// Alert if no progress in 10 minutes
		if (timeSinceLastSync > 600000) {
			logger.warn(
				`[ValidationService.validateProgress] Sync appears STALLED - no progress in ${(timeSinceLastSync / 1000 / 60).toFixed(2)} minutes`
			);
			return {
				status: 'stalled',
				message: 'No ingestion progress in 10+ minutes',
				timeSinceLastSync,
				lastTimestamp: checkpoint.lastTimestamp,
			};
		}

		// Check lag
		let lagStatus = 'healthy';
		if (lagSeconds > 3600) lagStatus = 'severely_lagging';
		else if (lagSeconds > 300) lagStatus = 'lagging';

		logger.info(
			`[ValidationService.validateProgress] Progress validation complete - status: ${lagStatus}, lag: ${lagSeconds.toFixed(2)}s`
		);

		return {
			status: lagStatus,
			lagSeconds,
			recordsIngested: checkpoint.recordsIngested,
			phase: checkpoint.phase,
			lastTimestamp: checkpoint.lastTimestamp,
		};
	}

	async smokeTest() {
		logger.debug('[ValidationService.smokeTest] Running smoke test - checking for recent data');
		const fiveMinutesAgo = new Date(Date.now() - 300000).toISOString();
		logger.debug(`[ValidationService.smokeTest] Looking for records after ${fiveMinutesAgo}`);

		try {
			// Can we query recent data?
			logger.debug('[ValidationService.smokeTest] Querying BigQueryData table for recent records');
			const recentRecords = await tables.BigQueryData.search({
				conditions: [{ timestamp: { $gt: fiveMinutesAgo } }],
				limit: 1,
				orderBy: 'timestamp DESC',
			});

			logger.debug(`[ValidationService.smokeTest] Query returned ${recentRecords.length} records`);

			if (recentRecords.length === 0) {
				logger.warn('[ValidationService.smokeTest] No recent data found in last 5 minutes');
				return {
					status: 'no_recent_data',
					message: 'No records found in last 5 minutes',
				};
			}

			const latestRecord = recentRecords[0];
			const recordLagSeconds = (Date.now() - new Date(latestRecord.timestamp).getTime()) / 1000;

			logger.info(
				`[ValidationService.smokeTest] Smoke test passed - latest record is ${Math.round(recordLagSeconds)}s old (timestamp: ${latestRecord.timestamp})`
			);

			return {
				status: 'healthy',
				latestTimestamp: latestRecord.timestamp,
				lagSeconds: recordLagSeconds,
				message: `Latest record is ${Math.round(recordLagSeconds)}s old`,
			};
		} catch (error) {
			logger.error(`[ValidationService.smokeTest] Query failed: ${error.message}`, error);
			return {
				status: 'query_failed',
				message: 'Failed to query Harper',
				error: error.message,
			};
		}
	}

	async spotCheckRecords() {
		logger.debug('[ValidationService.spotCheckRecords] Starting spot check validation');
		const clusterInfo = await this.discoverCluster();
		logger.debug(
			`[ValidationService.spotCheckRecords] Using nodeId: ${clusterInfo.nodeId}, clusterSize: ${clusterInfo.clusterSize}`
		);
		const issues = [];

		try {
			// Get 5 recent records from Harper
			logger.debug('[ValidationService.spotCheckRecords] Fetching 5 recent records from Harper');
			const harperSample = await tables.BigQueryData.search({
				limit: 5,
				orderBy: 'timestamp DESC',
			});

			logger.debug(`[ValidationService.spotCheckRecords] Retrieved ${harperSample.length} records from Harper`);

			if (harperSample.length === 0) {
				logger.warn('[ValidationService.spotCheckRecords] No records found in Harper for validation');
				return {
					status: 'no_data',
					message: 'No records in Harper to validate',
				};
			}

			// Verify each exists in BigQuery
			logger.debug(
				`[ValidationService.spotCheckRecords] Verifying ${harperSample.length} Harper records exist in BigQuery`
			);
			for (const record of harperSample) {
				logger.trace(
					`[ValidationService.spotCheckRecords] Verifying Harper record: id=${record.id}, timestamp=${record.timestamp}`
				);
				const exists = await this.bigqueryClient.verifyRecord(record);
				if (!exists) {
					logger.warn(
						`[ValidationService.spotCheckRecords] Phantom record found - exists in Harper but not BigQuery: ${record.id}`
					);
					issues.push({
						type: 'phantom_record',
						timestamp: record.timestamp,
						id: record.id,
						message: 'Record exists in Harper but not in BigQuery',
					});
				}
			}

			// Reverse check: verify recent BigQuery records exist in Harper
			const oneHourAgo = new Date(Date.now() - 3600000).toISOString();
			logger.debug(`[ValidationService.spotCheckRecords] Fetching recent BigQuery records (after ${oneHourAgo})`);
			const bqSample = await this.bigqueryClient.pullPartition({
				nodeId: clusterInfo.nodeId,
				clusterSize: clusterInfo.clusterSize,
				lastTimestamp: oneHourAgo, // Last hour
				batchSize: 5,
			});

			logger.debug(
				`[ValidationService.spotCheckRecords] Retrieved ${bqSample.length} records from BigQuery for reverse check`
			);

			for (const record of bqSample) {
				const id = this.generateRecordId(record);
				logger.trace(`[ValidationService.spotCheckRecords] Checking if BigQuery record exists in Harper: id=${id}`);
				const exists = await tables.BigQueryData.get(id);
				if (!exists) {
					logger.warn(`[ValidationService.spotCheckRecords] Missing record - exists in BigQuery but not Harper: ${id}`);
					issues.push({
						type: 'missing_record',
						timestamp: record.timestamp,
						id,
						message: 'Record exists in BigQuery but not in Harper',
					});
				}
			}

			const totalChecked = harperSample.length + bqSample.length;
			const status = issues.length === 0 ? 'healthy' : 'issues_found';
			logger.info(
				`[ValidationService.spotCheckRecords] Spot check complete - status: ${status}, checked: ${totalChecked} records, issues: ${issues.length}`
			);

			return {
				status,
				samplesChecked: totalChecked,
				issues,
				message:
					issues.length === 0 ? `Checked ${totalChecked} records, all match` : `Found ${issues.length} mismatches`,
			};
		} catch (error) {
			logger.error(`[ValidationService.spotCheckRecords] Spot check failed: ${error.message}`, error);
			return {
				status: 'check_failed',
				message: 'Spot check failed',
				error: error.message,
			};
		}
	}

	generateRecordId(record) {
		logger.trace(`[ValidationService.generateRecordId] Generating ID for validation - timestamp: ${record.timestamp}`);
		// Match the ID generation in sync-engine.js
		// Note: Adapt this to match your record's unique identifier strategy
		const hash = createHash('sha256')
			.update(`${record.timestamp}-${record.id || ''}`)
			.digest('hex');
		const id = hash.substring(0, 16);
		logger.trace(`[ValidationService.generateRecordId] Generated ID: ${id}`);
		return id;
	}

	async logAudit(results) {
		logger.debug('[ValidationService.logAudit] Logging validation audit results');
		const auditEntry = {
			id: `validation-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
			timestamp: results.timestamp,
			nodeId: (await this.discoverCluster()).nodeId,
			status: results.overallStatus,
			checkResults: JSON.stringify(results.checks),
			message: results.error || 'Validation completed',
		};
		logger.debug(`[ValidationService.logAudit] Audit entry: ${JSON.stringify(auditEntry).substring(0, 200)}...`);
		await tables.SyncAudit.put(auditEntry);
		logger.info('[ValidationService.logAudit] Validation audit logged to SyncAudit table');
	}

	async discoverCluster() {
		logger.trace('[ValidationService.discoverCluster] Discovering cluster topology for validation');
		const nodes = await harperCluster.getNodes();
		logger.trace(`[ValidationService.discoverCluster] Found ${nodes.length} nodes`);
		const sortedNodes = nodes.sort((a, b) => a.id.localeCompare(b.id));
		const currentNodeId = harperCluster.currentNode.id;
		const nodeIndex = sortedNodes.findIndex((n) => n.id === currentNodeId);

		logger.trace(
			`[ValidationService.discoverCluster] Current node: ${currentNodeId}, index: ${nodeIndex}, clusterSize: ${sortedNodes.length}`
		);

		return {
			nodeId: nodeIndex,
			clusterSize: sortedNodes.length,
		};
	}
}
