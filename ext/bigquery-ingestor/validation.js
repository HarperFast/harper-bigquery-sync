// ============================================================================
// File: validation.js
// Validation service for data integrity checks
// NOTE: Avoids count-based validation since Harper counts are estimates

/* global harperCluster, tables */

import { BigQueryClient } from './bigquery-client.js';
import { createHash } from 'node:crypto';

export class ValidationService {
  constructor() {
    this.bigqueryClient = new BigQueryClient();
  }

  async runValidation() {
    console.log('Running validation suite...');

    const results = {
      timestamp: new Date().toISOString(),
      checks: {}
    };

    try {
      // 1. Checkpoint progress monitoring
      results.checks.progress = await this.validateProgress();

      // 2. Smoke test - can we query recent data?
      results.checks.smokeTest = await this.smokeTest();

      // 3. Spot check random records
      results.checks.spotCheck = await this.spotCheckRecords();

      // Determine overall status
      const allHealthy = Object.values(results.checks).every(
        check => check.status === 'healthy' || check.status === 'ok'
      );

      results.overallStatus = allHealthy ? 'healthy' : 'issues_detected';

      // Log to audit table
      await this.logAudit(results);

      console.log(`Validation complete: ${results.overallStatus}`);
      return results;

    } catch (error) {
      console.error('Validation failed:', error);
      results.overallStatus = 'error';
      results.error = error.message;
      await this.logAudit(results);
      throw error;
    }
  }

  async validateProgress() {
    const clusterInfo = await this.discoverCluster();
    const checkpoint = await tables.SyncCheckpoint.get(clusterInfo.nodeId);

    if (!checkpoint) {
      return {
        status: 'no_checkpoint',
        message: 'No checkpoint found - node may not have started'
      };
    }

    const timeSinceLastSync = Date.now() - new Date(checkpoint.lastSyncTime).getTime();
    const lagSeconds = (Date.now() - new Date(checkpoint.lastTimestamp).getTime()) / 1000;

    // Alert if no progress in 10 minutes
    if (timeSinceLastSync > 600000) {
      return {
        status: 'stalled',
        message: 'No ingestion progress in 10+ minutes',
        timeSinceLastSync,
        lastTimestamp: checkpoint.lastTimestamp
      };
    }

    // Check lag
    let lagStatus = 'healthy';
    if (lagSeconds > 3600) lagStatus = 'severely_lagging';
    else if (lagSeconds > 300) lagStatus = 'lagging';

    return {
      status: lagStatus,
      lagSeconds,
      recordsIngested: checkpoint.recordsIngested,
      phase: checkpoint.phase,
      lastTimestamp: checkpoint.lastTimestamp
    };
  }

  async smokeTest() {
    const fiveMinutesAgo = new Date(Date.now() - 300000).toISOString();

    try {
      // Can we query recent data?
      const recentRecords = await tables.BigQueryData.search({
        conditions: [{ timestamp: { $gt: fiveMinutesAgo } }],
        limit: 1,
        orderBy: 'timestamp DESC'
      });

      if (recentRecords.length === 0) {
        return {
          status: 'no_recent_data',
          message: 'No records found in last 5 minutes'
        };
      }

      const latestRecord = recentRecords[0];
      const recordLagSeconds = (Date.now() - new Date(latestRecord.timestamp).getTime()) / 1000;

      return {
        status: 'healthy',
        latestTimestamp: latestRecord.timestamp,
        lagSeconds: recordLagSeconds,
        message: `Latest record is ${Math.round(recordLagSeconds)}s old`
      };

    } catch (error) {
      return {
        status: 'query_failed',
        message: 'Failed to query Harper',
        error: error.message
      };
    }
  }

  async spotCheckRecords() {
    const clusterInfo = await this.discoverCluster();
    const issues = [];

    try {
      // Get 5 recent records from Harper
      const harperSample = await tables.BigQueryData.search({
        limit: 5,
        orderBy: 'timestamp DESC'
      });

      if (harperSample.length === 0) {
        return {
          status: 'no_data',
          message: 'No records in Harper to validate'
        };
      }

      // Verify each exists in BigQuery
      for (const record of harperSample) {
        const exists = await this.bigqueryClient.verifyRecord(record);
        if (!exists) {
          issues.push({
            type: 'phantom_record',
            timestamp: record.timestamp,
            id: record.id,
            message: 'Record exists in Harper but not in BigQuery'
          });
        }
      }

      // Reverse check: verify recent BigQuery records exist in Harper
      const bqSample = await this.bigqueryClient.pullPartition({
        nodeId: clusterInfo.nodeId,
        clusterSize: clusterInfo.clusterSize,
        lastTimestamp: new Date(Date.now() - 3600000).toISOString(), // Last hour
        batchSize: 5
      });

      for (const record of bqSample) {
        const id = this.generateRecordId(record);
        const exists = await tables.BigQueryData.get(id);
        if (!exists) {
          issues.push({
            type: 'missing_record',
            timestamp: record.timestamp,
            id,
            message: 'Record exists in BigQuery but not in Harper'
          });
        }
      }

      return {
        status: issues.length === 0 ? 'healthy' : 'issues_found',
        samplesChecked: harperSample.length + bqSample.length,
        issues,
        message: issues.length === 0
          ? `Checked ${harperSample.length + bqSample.length} records, all match`
          : `Found ${issues.length} mismatches`
      };

    } catch (error) {
      return {
        status: 'check_failed',
        message: 'Spot check failed',
        error: error.message
      };
    }
  }

  generateRecordId(record) {
    // Match the ID generation in sync-engine.js
    const hash = createHash('sha256')
      .update(`${record.timestamp}-${record.device_id || ''}`)
      .digest('hex');
    return hash.substring(0, 16);
  }

  async logAudit(results) {
    await tables.SyncAudit.put({
      id: `validation-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      timestamp: results.timestamp,
      nodeId: (await this.discoverCluster()).nodeId,
      status: results.overallStatus,
      checkResults: JSON.stringify(results.checks),
      message: results.error || 'Validation completed'
    });
  }

  async discoverCluster() {
    const nodes = await harperCluster.getNodes();
    const sortedNodes = nodes.sort((a, b) => a.id.localeCompare(b.id));
    const currentNodeId = harperCluster.currentNode.id;
    const nodeIndex = sortedNodes.findIndex(n => n.id === currentNodeId);

    return {
      nodeId: nodeIndex,
      clusterSize: sortedNodes.length
    };
  }
}