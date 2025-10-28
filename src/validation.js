// ============================================================================
// File: validation.js
// Validation service for data completeness checks

/* global harperCluster, tables */

import { BigQueryClient } from './bigquery-client.js';

export class ValidationService {
  constructor() {
    this.bigqueryClient = new BigQueryClient();
  }
  
  async runValidation() {
    console.log('Running validation...');
    
    // Get current node info
    const clusterInfo = await this.discoverCluster();
    const nodeId = clusterInfo.nodeId;
    const clusterSize = clusterInfo.clusterSize;
    
    // Count in BigQuery
    const bqCount = await this.bigqueryClient.countPartition({
      nodeId,
      clusterSize
    });
    
    // Count in Harper
    const harperCount = await tables.BigQueryData.count();
    
    const delta = bqCount - harperCount;
    const status = Math.abs(delta) < 100 ? 'ok' : 'drift';
    
    // Log audit record
    await tables.SyncAudit.put({
      id: `audit-${Date.now()}`,
      timestamp: new Date().toISOString(),
      nodeId,
      bigQueryCount: bqCount,
      harperCount: harperCount,
      delta,
      status
    });
    
    console.log(`Validation complete: BQ=${bqCount}, Harper=${harperCount}, Delta=${delta}, Status=${status}`);
    
    return { bqCount, harperCount, delta, status };
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