// ============================================================================
// File: bigquery-client.js
// BigQuery API client with partition-aware queries

/* global config */

import { BigQuery } from '@google-cloud/bigquery';

export class BigQueryClient {
  constructor() {
    this.client = new BigQuery({
      projectId: config.bigquery.projectId,
      keyFilename: config.bigquery.credentials
    });
    
    this.dataset = config.bigquery.dataset;
    this.table = config.bigquery.table;
    this.timestampColumn = config.bigquery.timestampColumn;
  }
  
  async pullPartition({ nodeId, clusterSize, lastTimestamp, batchSize }) {
    const query = `
      SELECT *
      FROM \`${this.dataset}.${this.table}\`
      WHERE MOD(
        ABS(FARM_FINGERPRINT(CAST(${this.timestampColumn} AS STRING))), 
        @clusterSize
      ) = @nodeId
      AND ${this.timestampColumn} > @lastTimestamp
      ORDER BY ${this.timestampColumn} ASC
      LIMIT @batchSize
    `;
    
    const options = {
      query,
      params: {
        clusterSize,
        nodeId,
        lastTimestamp,
        batchSize
      }
    };
    
    try {
      const [rows] = await this.client.query(options);
      return rows;
    } catch (error) {
      console.error('BigQuery query error:', error);
      throw error;
    }
  }
  
  async countPartition({ nodeId, clusterSize }) {
    const query = `
      SELECT COUNT(*) as count
      FROM \`${this.dataset}.${this.table}\`
      WHERE MOD(
        ABS(FARM_FINGERPRINT(CAST(${this.timestampColumn} AS STRING))), 
        @clusterSize
      ) = @nodeId
    `;
    
    const options = {
      query,
      params: { clusterSize, nodeId }
    };
    
    const [rows] = await this.client.query(options);
    return rows[0].count;
  }
}