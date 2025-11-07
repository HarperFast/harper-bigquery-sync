// ============================================================================
// File: sync-engine.js
// Core synchronization engine with modulo-based partitioning

/* global config, harperCluster, tables */

import { BigQueryClient } from './bigquery-client.js';
import { globals } from './globals.js';

export class SyncEngine {
  constructor(config) {
    logger.info('[SyncEngine] Constructor called - initializing sync engine');

    logger.info("Hostname: " + server.hostname);
    logger.info("Worker Id: " + server.workerIndex);
    logger.info("Nodes: " + server.nodes);

    this.initialized = false;
    this.config = config;
    this.client = new BigQueryClient(this.config);
    this.running = false;
    this.nodeId = null;
    this.clusterSize = null;
    this.currentPhase = 'initial';
    this.lastCheckpoint = null;
    this.pollTimer = null;
    logger.debug('[SyncEngine] Constructor complete - initial state set');
  }
  
  async initialize() {
    if (! this.initialized) {
      logger.info('[SyncEngine.initialize] Starting initialization process');

      // Discover cluster topology using Harper's native clustering
      logger.debug('[SyncEngine.initialize] Discovering cluster topology');
      const clusterInfo = await this.discoverCluster();
      logger.debug(clusterInfo);
      this.nodeId = clusterInfo.nodeId;
      this.clusterSize = clusterInfo.clusterSize;

      logger.info(`[SyncEngine.initialize] Node initialized: ID=${this.nodeId}, ClusterSize=${this.clusterSize}`);

      // Load last checkpoint
      logger.debug('[SyncEngine.initialize] Loading checkpoint from database');
      this.lastCheckpoint = await this.loadCheckpoint();

      if (this.lastCheckpoint) {
        this.currentPhase = this.lastCheckpoint.phase || 'initial';
        logger.info(`[SyncEngine.initialize] Resuming from checkpoint: ${this.lastCheckpoint.lastTimestamp}, phase: ${this.currentPhase}`);
      } else {
        logger.info('[SyncEngine.initialize] No checkpoint found - starting fresh');
        // First run - start from beginning or configurable start time
        this.lastCheckpoint = {
          nodeId: this.nodeId,
          lastTimestamp: this.config.sync.startTimestamp || '1970-01-01T00:00:00Z',
          recordsIngested: 0,
          phase: 'initial'
        };
        logger.debug(`[SyncEngine.initialize] Created new checkpoint starting at ${this.lastCheckpoint.lastTimestamp}`);
      }
      logger.info('[SyncEngine.initialize] Initialization complete');
      this.initialized = true;
    } else {
      logger.info('[SyncEngine.initialize] Object already initialized');
    }
  }
  
  async discoverCluster() {
    logger.debug('[SyncEngine.discoverCluster] Querying Harper cluster API');
    const currentNodeId = [server.hostname, server.workerIndex].join('-');
    logger.info(`[SyncEngine.discoverCluster] Current node ID: ${currentNodeId}`);

    // Get cluster nodes from server.nodes if available
    let nodes;
    if (server.nodes && Array.isArray(server.nodes) && server.nodes.length > 0) {
      nodes = server.nodes.map(node => `${node.hostname}-${node.workerIndex || 0}`);
      logger.info(`[SyncEngine.discoverCluster] Found ${nodes.length} nodes from server.nodes`);
    } else {
      logger.info('[SyncEngine.discoverCluster] No cluster nodes found, running in single-node mode');
      nodes = [currentNodeId];
    }

    // Sort deterministically (lexicographic)
    nodes.sort();
    logger.info(`[SyncEngine.discoverCluster] Sorted nodes: ${nodes.join(', ')}`);

    // Find our position
    const nodeIndex = nodes.findIndex(n => n === currentNodeId);

    if (nodeIndex === -1) {
      logger.error(`[SyncEngine.discoverCluster] Current node '${currentNodeId}' not found in cluster nodes: ${nodes.join(', ')}`);
      throw new Error(`Current node ${currentNodeId} not found in cluster`);
    }

    logger.info(`[SyncEngine.discoverCluster] Node position determined: index=${nodeIndex}, clusterSize=${nodes.length}`);
    return {
      nodeId: nodeIndex,
      clusterSize: nodes.length,
      nodes: nodes
    };
  }
  
  async loadCheckpoint() {
    logger.debug(`[SyncEngine.loadCheckpoint] Attempting to load checkpoint for nodeId=${this.nodeId}`);
    try {
      const checkpoint = await tables.SyncCheckpoint.get(this.nodeId);
      logger.debug(`[SyncEngine.loadCheckpoint] Checkpoint found: ${JSON.stringify(checkpoint)}`);
      return checkpoint;
    } catch (error) {
      // If checkpoint not found, return null; otherwise log and rethrow so callers can handle it.
      if (error && (error.code === 'NOT_FOUND' || /not\s*found/i.test(error.message || ''))) {
        logger.debug('[SyncEngine.loadCheckpoint] No checkpoint found in database (first run)');
        return null;
      }
      logger.error(`[SyncEngine.loadCheckpoint] Error loading checkpoint: ${error.message}`, error);
      throw error;
    }
  }
  
  async start() {
    logger.info('[SyncEngine.start] Start method called');
    if (this.running) {
      logger.warn('[SyncEngine.start] Sync already running - ignoring duplicate start request');
      return;
    }
    
    this.running = true;
    logger.info('[SyncEngine.start] Starting sync loop');
    this.schedulePoll();
    logger.debug('[SyncEngine.start] First poll scheduled');
  }
  
  async stop() {
    logger.info('[SyncEngine.stop] Stop method called');
    this.running = false;
    if (this.pollTimer) {
      logger.debug('[SyncEngine.stop] Clearing poll timer');
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    logger.info('[SyncEngine.stop] Sync stopped');
  }
  
  schedulePoll() {
    if (!this.running) {
      logger.debug('[SyncEngine.schedulePoll] Not running - skipping poll schedule');
      return;
    }
    
    const interval = this.calculatePollInterval();
    logger.debug(`[SyncEngine.schedulePoll] Scheduling next poll in ${interval}ms (phase: ${this.currentPhase})`);
    this.pollTimer = setTimeout(() => this.runSyncCycle(), interval);
  }
  
  calculatePollInterval() {
    logger.debug(`[SyncEngine.calculatePollInterval] Calculating interval for phase: ${this.currentPhase}`);
    // Adaptive polling based on phase
    let interval;
    switch(this.currentPhase) {
      case 'initial':
      interval = 1000; // 1 second - aggressive catch-up
      break;
      case 'catchup':
      interval = 5000; // 5 seconds
      break;
      case 'steady':
      interval = this.config.sync.pollInterval || 30000; // 30 seconds
      break;
      default:
      interval = 10000;
    }
    logger.debug(`[SyncEngine.calculatePollInterval] Interval calculated: ${interval}ms`);
    return interval;
  }
  
  async runSyncCycle() {
    logger.info(`[SyncEngine.runSyncCycle] Starting sync cycle for node ${this.nodeId}, phase: ${this.currentPhase}`);
    try {
      const batchSize = this.calculateBatchSize();
      logger.debug(`[SyncEngine.runSyncCycle] Batch size: ${batchSize}`);
      
      // Pull records for this node's partition
      logger.debug(`[SyncEngine.runSyncCycle] Pulling partition data from BigQuery - nodeId: ${this.nodeId}, clusterSize: ${this.clusterSize}, lastTimestamp: ${this.lastCheckpoint.lastTimestamp}`);
      const records = await this.client.pullPartition({
        nodeId: this.nodeId,
        clusterSize: this.clusterSize,
        lastTimestamp: this.lastCheckpoint.lastTimestamp,
        batchSize
      });
      logger.info(`[SyncEngine.runSyncCycle] Received ${records.length} records from BigQuery`);
      
      if (records.length === 0) {
        logger.info(`[SyncEngine.runSyncCycle] No new records found - transitioning to steady state`);
        this.currentPhase = 'steady';
      } else {
        
        // Write to Harper
        logger.debug(`[SyncEngine.runSyncCycle] Ingesting ${records.length} records into Harper`);
        await this.ingestRecords(records);
        logger.debug('[SyncEngine.runSyncCycle] Ingest complete');
        
        // Update checkpoint
        logger.debug('[SyncEngine.runSyncCycle] Updating checkpoint');
        await this.updateCheckpoint(records);
        logger.debug('[SyncEngine.runSyncCycle] Checkpoint updated');
        
        // Update phase based on lag
        logger.debug('[SyncEngine.runSyncCycle] Updating phase based on lag');
        await this.updatePhase();
        logger.debug(`[SyncEngine.runSyncCycle] Phase after update: ${this.currentPhase}`);
      }
      
      logger.info(`[SyncEngine.runSyncCycle] Sync cycle complete`);
    } catch (error) {
      logger.error(`[SyncEngine.runSyncCycle] Sync cycle error: ${error.message}`, error);
      // Continue despite errors - don't crash the component
    } finally {
      // Schedule next poll
      logger.debug('[SyncEngine.runSyncCycle] Scheduling next poll');
      this.schedulePoll();
    }
  }
  
  calculateBatchSize() {
    logger.debug(`[SyncEngine.calculateBatchSize] Calculating batch size for phase: ${this.currentPhase}`);
    let batchSize;
    switch(this.currentPhase) {
      case 'initial':
      batchSize = this.config.sync.initialBatchSize || 10000;
      break;
      case 'catchup':
      batchSize = this.config.sync.catchupBatchSize || 1000;
      break;
      case 'steady':
      batchSize = this.config.sync.steadyBatchSize || 500;
      break;
      default:
      batchSize = 1000;
    }
    logger.debug(`[SyncEngine.calculateBatchSize] Batch size: ${batchSize}`);
    return batchSize;
  }
  
  convertBigQueryTypes(record) {
    // Convert BigQuery types to JavaScript primitives
    // All timestamp/datetime types are converted to Date objects for Harper's timestamp type
    const converted = {};
    for (const [key, value] of Object.entries(record)) {
      if (value === null || value === undefined) {
        converted[key] = value;
      } else if (typeof value === 'bigint') {
        // Convert BigInt to number or string depending on size
        converted[key] = value <= Number.MAX_SAFE_INTEGER ? Number(value) : value.toString();
      } else if (value && typeof value === 'object') {
        // Handle various BigQuery object types
        const constructorName = value.constructor?.name;

        // BigQuery Timestamp/DateTime objects
        if (constructorName === 'BigQueryTimestamp' || constructorName === 'BigQueryDatetime' || constructorName === 'BigQueryDate') {
          // Convert to Date object - Harper's timestamp type expects Date objects
          if (value.value) {
            // value.value contains the ISO string
            const dateObj = new Date(value.value);
            logger.trace(`[SyncEngine.convertBigQueryTypes] Converted ${constructorName} '${key}': ${value.value} -> Date(${dateObj.toISOString()})`);
            converted[key] = dateObj;
          } else if (typeof value.toJSON === 'function') {
            const jsonValue = value.toJSON();
            const dateObj = new Date(jsonValue);
            logger.trace(`[SyncEngine.convertBigQueryTypes] Converted ${constructorName} '${key}' via toJSON: ${jsonValue} -> Date(${dateObj.toISOString()})`);
            converted[key] = dateObj;
          } else {
            logger.warn(`[SyncEngine.convertBigQueryTypes] Unable to convert ${constructorName} for key ${key}`);
            converted[key] = value;
          }
        } else if (typeof value.toISOString === 'function') {
          // Already a Date object - keep as-is
          converted[key] = value;
        } else if (typeof value.toJSON === 'function') {
          // Object with toJSON method - convert
          const jsonValue = value.toJSON();
          // If it looks like an ISO date string, convert to Date
          if (typeof jsonValue === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(jsonValue)) {
            const dateObj = new Date(jsonValue);
            logger.trace(`[SyncEngine.convertBigQueryTypes] Converted generic timestamp '${key}': ${jsonValue} -> Date(${dateObj.toISOString()})`);
            converted[key] = dateObj;
          } else {
            converted[key] = jsonValue;
          }
        } else {
          converted[key] = value;
        }
      } else {
        converted[key] = value;
      }
    }
    return converted;
  }

  async ingestRecords(records) {

    logger.trace(`[SyncEngine.ingestRecords] Processing records: ${JSON.stringify(records)} records for ingestion`);
    logger.debug(`[SyncEngine.ingestRecords] Processing ${records.length} records for ingestion`);
    const validRecords = [];
    const timestampColumn = this.config.bigquery.timestampColumn;

    for (const record of records) {
      try {
        // Convert BigQuery types to JavaScript primitives
        const convertedRecord = this.convertBigQueryTypes(record);
        logger.trace(`[SyncEngine.ingestRecords] Converted record: ${JSON.stringify(convertedRecord)}`);

        // Validate timestamp exists
        if (!convertedRecord[timestampColumn]) {
          logger.warn(`[SyncEngine.ingestRecords] Missing timestamp column '${timestampColumn}', skipping record: ${JSON.stringify(convertedRecord).substring(0, 100)}`);
          await this.logSkippedRecord(convertedRecord, `missing_${timestampColumn}`);
          continue;
        }

        // Remove 'id' field from BigQuery data if it exists (not needed since transaction_date is the primary key)
        const { id: _unusedId, ...cleanedRecord } = convertedRecord;

        // Store BigQuery record as-is with metadata
        // transaction_date is the primary key (defined in schema)
        const mappedRecord = {
          ...cleanedRecord,  // All BigQuery fields at top level (timestamps converted to Date objects)
          _syncedAt: new Date()  // Add sync timestamp as Date object
        };

        validRecords.push(mappedRecord);
      } catch (error) {
        logger.error(`[SyncEngine.ingestRecords] Error processing record: ${error.message}`, error);
        logger.error(`[SyncEngine.ingestRecords] Error stack: ${error.stack}`);
        await this.logSkippedRecord(record, `processing_error: ${error.message}`);
      }
    }

    logger.info(`[SyncEngine.ingestRecords] Validated ${validRecords.length}/${records.length} records`);
    // logger.debug(`[SyncEngine.ingestRecords] Cleaned Records: ` + validRecords);

    // Batch write to Harper using internal API
    if (validRecords.length > 0) {
      logger.info(`[SyncEngine.ingestRecords] Writing ${validRecords.length} records to Harper`);

      // Debug: Log first record to see exact structure
      const firstRecord = validRecords[0];
      logger.info(`[SyncEngine.ingestRecords] First record keys: ${Object.keys(firstRecord).join(', ')}`);
      logger.info(`[SyncEngine.ingestRecords] First record sample: ${JSON.stringify(firstRecord).substring(0, 500)}`);

      // Check for undefined values
      for (const [key, value] of Object.entries(firstRecord)) {
        if (value === undefined) {
          logger.error(`[SyncEngine.ingestRecords] Field '${key}' is undefined!`);
        }
      }

      let lastResult; 
      transaction((txn) => {
        logger.info(`[SyncEngine.ingestRecords] Cleaned Records[0]: ${JSON.stringify(validRecords[0]).substring(0, 500)}`);
        try {
          // logger.error(`[SyncEngine.ingestRecords] Records to create ${JSON.stringify(validRecords, null, 2)}`);
          for (const rec of validRecords) {
            lastResult = tables.BigQueryData.create(rec);
          }
        } catch (error) {
          // Always log full error detail
          logger.error('[SyncEngine.ingestRecords] Harper create failed');
          logger.error(`Error name: ${error.name}`);
          logger.error(`Error message: ${error.message}`);
          logger.error(`Error stack: ${error.stack}`);

          // BigQuery often includes structured info
          if (error.errors) {
            for (const e of error.errors) {
              logger.error(`BigQuery error reason: ${e.reason}`);
              logger.error(`BigQuery error location: ${e.location}`);
              logger.error(`BigQuery error message: ${e.message}`);
            }
          }
        }
      });
      logger.info('[SyncEngine.ingestRecords] Created validRecords in database/table, result:' + lastResult);

      logger.info(`[SyncEngine.ingestRecords] Successfully wrote ${validRecords.length} records to BigQueryData table`);
    } else {
      logger.warn('[SyncEngine.ingestRecords] No valid records to write');
    }
  }

  async updateCheckpoint(records) {
    logger.debug(`[SyncEngine.updateCheckpoint] Updating checkpoint with ${records.length} records`);
    const lastRecord = records.at(-1);
    const timestampColumn = this.config.bigquery.timestampColumn;
    const lastTimestamp = lastRecord[timestampColumn];

    if (!lastTimestamp) {
      logger.error(`[SyncEngine.updateCheckpoint] Last record missing timestamp column '${timestampColumn}'`);
      throw new Error(`Missing timestamp column in last record: ${timestampColumn}`);
    }

    // Convert Date object to ISO string for storage in checkpoint
    const lastTimestampString = lastTimestamp instanceof Date ? lastTimestamp.toISOString() : String(lastTimestamp);
    logger.debug(`[SyncEngine.updateCheckpoint] Last record timestamp: ${lastTimestampString}`);

    this.lastCheckpoint = {
      nodeId: this.nodeId,
      lastTimestamp: lastTimestampString,
      recordsIngested: this.lastCheckpoint.recordsIngested + records.length,
      lastSyncTime: new Date().toISOString(),
      phase: this.currentPhase,
      batchSize: this.calculateBatchSize()
    };

    logger.debug(`[SyncEngine.updateCheckpoint] New checkpoint: ${JSON.stringify(this.lastCheckpoint)}`);
    await tables.SyncCheckpoint.put(this.lastCheckpoint);
    logger.info(`[SyncEngine.updateCheckpoint] Checkpoint saved - total records ingested: ${this.lastCheckpoint.recordsIngested}`);
  }
  
  async updatePhase() {
    logger.debug('[SyncEngine.updatePhase] Calculating sync lag and updating phase');
    // Calculate lag in seconds
    const now = Date.now();
    const lastTimestamp = new Date(this.lastCheckpoint.lastTimestamp).getTime();
    const lagSeconds = (now - lastTimestamp) / 1000;
    
    logger.debug(`[SyncEngine.updatePhase] Current lag: ${lagSeconds.toFixed(2)} seconds`);
    const oldPhase = this.currentPhase;
    
    // Update phase based on lag thresholds
    if (lagSeconds > (this.config.sync.catchupThreshold || 3600)) {
      this.currentPhase = 'initial';
    } else if (lagSeconds > (this.config.sync.steadyThreshold || 300)) {
      this.currentPhase = 'catchup';
    } else {
      this.currentPhase = 'steady';
    }
    
    if (oldPhase !== this.currentPhase) {
      logger.info(`[SyncEngine.updatePhase] Phase transition: ${oldPhase} -> ${this.currentPhase} (lag: ${lagSeconds.toFixed(2)}s)`);
    } else {
      logger.debug(`[SyncEngine.updatePhase] Phase unchanged: ${this.currentPhase}`);
    }
  }
  
  async logSkippedRecord(record, reason) {
    logger.warn(`[SyncEngine.logSkippedRecord] Logging skipped record - reason: ${reason}`);
    // Log to audit table for monitoring
    const auditEntry = {
      id: `skip-${Date.now()}-${Math.random()}`,
      timestamp: new Date().toISOString(),
      nodeId: this.nodeId,
      status: 'skipped',
      reason,
      recordSample: JSON.stringify(record).substring(0, 500)
    };
    logger.debug(`[SyncEngine.logSkippedRecord] Audit entry: ${JSON.stringify(auditEntry)}`);
    await tables.SyncAudit.put(auditEntry);
    logger.debug('[SyncEngine.logSkippedRecord] Skipped record logged to audit table');
  }
  
  async getStatus() {
    logger.debug('[SyncEngine.getStatus] Status requested');
    const status = {
      nodeId: this.nodeId,
      clusterSize: this.clusterSize,
      running: this.running,
      phase: this.currentPhase,
      lastCheckpoint: this.lastCheckpoint
    };
    logger.debug(`[SyncEngine.getStatus] Returning status: ${JSON.stringify(status)}`);
    return status;
  }
}

// Export additional classes for use in resources.js
// TODO: Validation not yet implemented - requires additional testing
// export { ValidationService } from './validation.js';
export { BigQueryClient } from './bigquery-client.js';