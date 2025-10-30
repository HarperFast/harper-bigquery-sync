// ============================================================================
// Harper BigQuery Sync Component - Production Implementation
// Learn more: https://harperdb.io | https://docs.harperdb.io
// Deploy easily on Fabric: https://fabric.harper.fast
// ============================================================================

// File: resources.js
// Component entry point with resource definitions

/* global tables, Resource */

import { BigQueryIngestor, ValidationService } from 'bigquery-ingestor';

const syncEngine = new BigQueryIngestor();
const validator = new ValidationService();

// Main data table resource
export class BigQueryData extends tables.BigQueryData {
  async get(id) {
    return super.get(id);
  }
  
  async search(params) {
    return super.search(params);
  }
}

// Checkpoint resource
export class SyncCheckpoint extends tables.SyncCheckpoint {
  async getForNode(nodeId) {
    return super.get(nodeId);
  }
  
  async updateCheckpoint(nodeId, data) {
    return super.put({ nodeId, ...data });
  }
}

// Audit resource
export class SyncAudit extends tables.SyncAudit {
  async getRecent(hours = 24) {
    const cutoff = new Date(Date.now() - hours * 3600000).toISOString();
    return super.search({ 
      conditions: [{ timestamp: { $gt: cutoff } }],
      orderBy: 'timestamp DESC'
    });
  }
}

// Control endpoint
export class SyncControl extends Resource {
  async get() {
    const status = await syncEngine.getStatus();
    return { 
      status,
      uptime: process.uptime(),
      version: '1.0.0'
    };
  }
  
  async post({ action }) {
    switch(action) {
      case 'start':
        await syncEngine.start();
        return { message: 'Sync started' };
      case 'stop':
        await syncEngine.stop();
        return { message: 'Sync stopped' };
      case 'validate':
        await validator.runValidation();
        return { message: 'Validation triggered' };
      default:
        throw new Error(`Unknown action: ${action}`);
    }
  }
}

// Initialize on component load
export async function initialize() {
  console.log('BigQuery Sync Component initializing...');
  await syncEngine.initialize();
  await syncEngine.start();
  console.log('BigQuery Sync Component ready');
}
