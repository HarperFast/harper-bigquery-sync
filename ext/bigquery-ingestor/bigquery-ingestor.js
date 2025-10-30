// ============================================================================
// File: sync-engine.js
// Core synchronization engine with modulo-based partitioning

/* global config, harperCluster, tables */

import { BigQueryClient } from './bigquery-client.js';
import { createHash } from 'node:crypto';

export class BigQueryIngestor {
  constructor() {
    this.client = new BigQueryClient();
    this.running = false;
    this.nodeId = null;
    this.clusterSize = null;
    this.currentPhase = 'initial';
    this.lastCheckpoint = null;
    this.pollTimer = null;
  }
  
  async initialize() {
    // Discover cluster topology using Harper's native clustering
    const clusterInfo = await this.discoverCluster();
    this.nodeId = clusterInfo.nodeId;
    this.clusterSize = clusterInfo.clusterSize;
    
    console.log(`Node initialized: ID=${this.nodeId}, ClusterSize=${this.clusterSize}`);
    
    // Load last checkpoint
    this.lastCheckpoint = await this.loadCheckpoint();
    
    if (this.lastCheckpoint) {
      this.currentPhase = this.lastCheckpoint.phase || 'initial';
      console.log(`Resuming from checkpoint: ${this.lastCheckpoint.lastTimestamp}`);
    } else {
      // First run - start from beginning or configurable start time
      this.lastCheckpoint = {
        nodeId: this.nodeId,
        lastTimestamp: config.sync.startTimestamp || '1970-01-01T00:00:00Z',
        recordsIngested: 0,
        phase: 'initial'
      };
    }
  }
  
  async discoverCluster() {
    // Use Harper's native clustering API
    const nodes = await harperCluster.getNodes();
    
    // Sort deterministically (lexicographic)
    const sortedNodes = nodes.sort((a, b) => a.id.localeCompare(b.id));
    
    // Find our position
    const currentNodeId = harperCluster.currentNode.id;
    const nodeIndex = sortedNodes.findIndex(n => n.id === currentNodeId);
    
    if (nodeIndex === -1) {
      throw new Error('Current node not found in cluster');
    }
    
    return {
      nodeId: nodeIndex,
      clusterSize: sortedNodes.length,
      nodes: sortedNodes
    };
  }
  
  async loadCheckpoint() {
    try {
      return await tables.SyncCheckpoint.get(this.nodeId);
    } catch (error) {
      // If checkpoint not found, return null; otherwise log and rethrow so callers can handle it.
      if (error && (error.code === 'NOT_FOUND' || /not\s*found/i.test(error.message || ''))) {
        return null;
      }
      console.error('Error loading checkpoint:', error);
      throw error;
    }
  }
  
  async start() {
    if (this.running) {
      console.log('Sync already running');
      return;
    }
    
    this.running = true;
    console.log('Starting sync loop');
    this.schedulePoll();
  }
  
  async stop() {
    this.running = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    console.log('Sync stopped');
  }
  
  schedulePoll() {
    if (!this.running) return;
    
    const interval = this.calculatePollInterval();
    this.pollTimer = setTimeout(() => this.runSyncCycle(), interval);
  }
  
  calculatePollInterval() {
    // Adaptive polling based on phase
    switch(this.currentPhase) {
      case 'initial':
        return 1000; // 1 second - aggressive catch-up
      case 'catchup':
        return 5000; // 5 seconds
      case 'steady':
        return config.sync.pollInterval || 30000; // 30 seconds
      default:
        return 10000;
    }
  }
  
  async runSyncCycle() {
    try {
      console.log(`[Node ${this.nodeId}] Starting sync cycle, phase: ${this.currentPhase}`);
      
      const batchSize = this.calculateBatchSize();
      
      // Pull records for this node's partition
      const records = await this.client.pullPartition({
        nodeId: this.nodeId,
        clusterSize: this.clusterSize,
        lastTimestamp: this.lastCheckpoint.lastTimestamp,
        batchSize
      });
      
      if (records.length === 0) {
        console.log(`[Node ${this.nodeId}] No new records, transitioning to steady state`);
        this.currentPhase = 'steady';
      } else {
        console.log(`[Node ${this.nodeId}] Ingested ${records.length} records`);
        
        // Write to Harper
        await this.ingestRecords(records);
        
        // Update checkpoint
        await this.updateCheckpoint(records);
        
        // Update phase based on lag
        await this.updatePhase();
      }
      
    } catch (error) {
      console.error(`[Node ${this.nodeId}] Sync cycle error:`, error);
      // Continue despite errors - don't crash the component
    } finally {
      // Schedule next poll
      this.schedulePoll();
    }
  }
  
  calculateBatchSize() {
    switch(this.currentPhase) {
      case 'initial':
        return config.sync.initialBatchSize || 10000;
      case 'catchup':
        return config.sync.catchupBatchSize || 1000;
      case 'steady':
        return config.sync.steadyBatchSize || 500;
      default:
        return 1000;
    }
  }
  
  async ingestRecords(records) {
    const validRecords = [];
    
    for (const record of records) {
      // Validate required fields
      if (!record.timestamp) {
        console.warn('Missing timestamp, skipping record:', record);
        await this.logSkippedRecord(record, 'missing_timestamp');
        continue;
      }
      
      // Generate primary key
      const id = this.generateRecordId(record);
      
      validRecords.push({
        id,
        timestamp: record.timestamp,
        deviceId: record.device_id,
        data: record,
        _syncedAt: new Date().toISOString()
      });
    }
    
    // Batch write to Harper using internal API
    if (validRecords.length > 0) {
      await tables.BigQueryData.putBatch(validRecords);
    }
  }
  
  generateRecordId(record) {
    // Create deterministic ID from timestamp + device_id
    const hash = createHash('sha256')
      .update(`${record.timestamp}-${record.device_id || ''}`)
      .digest('hex');
    return hash.substring(0, 16);
  }
  
  async updateCheckpoint(records) {
    const lastRecord = records.at(-1);
    
    this.lastCheckpoint = {
      nodeId: this.nodeId,
      lastTimestamp: lastRecord.timestamp,
      recordsIngested: this.lastCheckpoint.recordsIngested + records.length,
      lastSyncTime: new Date().toISOString(),
      phase: this.currentPhase,
      batchSize: this.calculateBatchSize()
    };
    
    await tables.SyncCheckpoint.put(this.lastCheckpoint);
  }
  
  async updatePhase() {
    // Calculate lag in seconds
    const now = Date.now();
    const lastTimestamp = new Date(this.lastCheckpoint.lastTimestamp).getTime();
    const lagSeconds = (now - lastTimestamp) / 1000;
    
    // Update phase based on lag thresholds
    if (lagSeconds > (config.sync.catchupThreshold || 3600)) {
      this.currentPhase = 'initial';
    } else if (lagSeconds > (config.sync.steadyThreshold || 300)) {
      this.currentPhase = 'catchup';
    } else {
      this.currentPhase = 'steady';
    }
  }
  
  async logSkippedRecord(record, reason) {
    // Log to audit table for monitoring
    await tables.SyncAudit.put({
      id: `skip-${Date.now()}-${Math.random()}`,
      timestamp: new Date().toISOString(),
      nodeId: this.nodeId,
      status: 'skipped',
      reason,
      recordSample: JSON.stringify(record).substring(0, 500)
    });
  }
  
  async getStatus() {
    return {
      nodeId: this.nodeId,
      clusterSize: this.clusterSize,
      running: this.running,
      phase: this.currentPhase,
      lastCheckpoint: this.lastCheckpoint
    };
  }
}

// Export additional classes for use in resources.js
export { ValidationService } from './validation.js';
export { BigQueryClient } from './bigquery-client.js';