# Multi-Table Support - TDD Implementation Design

**Date:** 2025-11-12
**Approach:** Test-Driven Development
**Goal:** Enable end-to-end testing with multiple BigQuery tables while preparing for future parallel SyncEngine architecture

## Overview

Extend the BigQuery sync plugin to support multiple tables using a test-first approach. Build the multi-table data synthesizer first, create comprehensive tests, then implement plugin changes to make tests pass.

## Design Principles

1. **Test-First:** Write tests before implementation
2. **Future-Proof:** Design for easy refactoring to parallel SyncEngines
3. **Backward Compatible:** Legacy single-table configs continue to work
4. **Clean Separation:** Each table is independent (mirrors future architecture)
5. **Minimal Changes:** Keep current SyncEngine mostly intact

## Multi-Table Schema

### BigQuery Tables

**1. `vessel_positions`** (existing, enhanced)
- High volume: ~144K records/day
- Primary tracking data: location, speed, heading
- Timestamp column: `timestamp`

**2. `port_events`** (new)
- Medium volume: ~5-10K events/day
- Vessel arrivals/departures at ports
- Timestamp column: `event_time`
- Relationships: Links to vessels via `mmsi`, ports via `port_id`

**3. `vessel_metadata`** (new)
- Low volume: ~100K vessels, rare updates
- Vessel static information: name, type, specs
- Timestamp column: `last_updated`
- Purpose: Slow-changing dimension pattern

### Schema Benefits

- **Different sync patterns:** High-frequency, event-driven, slow-changing
- **Realistic relationships:** Tests join scenarios downstream
- **Volume diversity:** Tests performance across different scales
- **Clean separation:** Each table independently valuable
- **Future-proof:** Natural mapping to parallel SyncEngine instances

## Data Synthesizer Architecture

### Generator Structure

```
src/generators/
  vessel-positions-generator.js   (existing, refactored)
  port-events-generator.js        (new)
  vessel-metadata-generator.js    (new)
  multi-table-orchestrator.js     (new)
```

### Generator Interface (Standard Contract)

```javascript
class TableGenerator {
  async initialize(config)      // Setup BigQuery client, validate table
  async generate(timeRange)      // Generate records for time range
  async write(records)           // Write to BigQuery
  async clear()                  // Clear all data
  getStats()                     // Return generation statistics
}
```

### Multi-Table Orchestrator

```javascript
class MultiTableOrchestrator {
  constructor(config) {
    this.generators = {
      vessel_positions: new VesselPositionsGenerator(config),
      port_events: new PortEventsGenerator(config),
      vessel_metadata: new VesselMetadataGenerator(config)
    };
  }

  async generateAll(timeRange, options = {}) {
    // Generate all tables for the same time window
    // Can enable/disable specific tables
    const enabledGenerators = options.tables
      ? options.tables.map(t => this.generators[t])
      : Object.values(this.generators);

    await Promise.all(
      enabledGenerators.map(g => g.generate(timeRange))
    );
  }

  async clearAll() {
    await Promise.all(
      Object.values(this.generators).map(g => g.clear())
    );
  }
}
```

### Key Design Decisions

- **Shared Vessel Registry:** All generators reference same 100K vessel fleet
- **Temporal Consistency:** Events coordinated (vessel arrives → port event fires)
- **Independent Tables:** Each generator writes to its own BigQuery table
- **Configurable Volume:** Enable/disable tables, adjust generation rates
- **Parallel Ready:** Structure mirrors future parallel SyncEngines

## Configuration Format

### Multi-Table Configuration

```yaml
bigquery:
  projectId: your-project
  credentials: service-account-key.json
  location: US

  # Array of tables to sync (NEW)
  tables:
    - id: vessel_positions           # Unique identifier
      dataset: maritime_tracking
      table: vessel_positions
      timestampColumn: timestamp
      columns:
        - timestamp
        - mmsi
        - latitude
        - longitude
        - speed_knots
      targetTable: VesselPositions   # Harper table name
      sync:
        initialBatchSize: 10000
        catchupBatchSize: 1000
        steadyBatchSize: 500

    - id: port_events
      dataset: maritime_tracking
      table: port_events
      timestampColumn: event_time
      columns:
        - event_time
        - port_id
        - vessel_mmsi
        - event_type
        - status
      targetTable: PortEvents
      sync:
        initialBatchSize: 5000
        catchupBatchSize: 500
        steadyBatchSize: 100

    - id: vessel_metadata
      dataset: maritime_tracking
      table: vessel_metadata
      timestampColumn: last_updated
      columns: "*"  # All vessel details
      targetTable: VesselMetadata
      sync:
        initialBatchSize: 1000
        catchupBatchSize: 100
        steadyBatchSize: 10

# Global sync defaults (optional, for tables that don't override)
sync:
  pollInterval: 30000
  catchupThreshold: 3600
  steadyThreshold: 300
```

### Backward Compatible Legacy Format

```yaml
# Single table (still works, no changes needed)
bigquery:
  projectId: your-project
  dataset: maritime_tracking
  table: vessel_positions
  timestampColumn: timestamp
  columns: [timestamp, mmsi, latitude, longitude]
```

### Configuration Detection

```javascript
export function getPluginConfig(fullConfig) {
  // Detect multi-table vs legacy
  if (fullConfig.bigquery.tables && Array.isArray(fullConfig.bigquery.tables)) {
    return getMultiTableConfig(fullConfig);  // Returns { tables: [...] }
  }

  // Legacy single-table - wrap in tables array for unified handling
  return {
    tables: [getSingleTableConfig(fullConfig)]
  };
}
```

## Plugin Implementation Changes

### Minimal, Future-Proof Changes

**Current Architecture:**
```
service.js → SyncEngine (single table)
```

**TDD Implementation (temporary, clean):**
```
service.js → Loop over tables → SyncEngine (one per table)
```

**Future Architecture (later refactoring):**
```
service.js → SyncOrchestrator → TableSyncEngine[] (parallel)
```

### 1. Config Loader Changes

**File:** `src/config-loader.js`

```javascript
// NEW: Multi-table config parser
export function getMultiTableConfig(fullConfig) {
  const { bigquery, sync } = fullConfig;

  return {
    projectId: bigquery.projectId,
    credentials: bigquery.credentials,
    location: bigquery.location || 'US',
    tables: bigquery.tables.map(tableConfig => ({
      id: tableConfig.id,
      dataset: tableConfig.dataset,
      table: tableConfig.table,
      timestampColumn: tableConfig.timestampColumn,
      columns: validateAndNormalizeColumns(
        tableConfig.columns,
        tableConfig.timestampColumn
      ),
      targetTable: tableConfig.targetTable,
      sync: {
        ...sync,  // Global defaults
        ...tableConfig.sync  // Table-specific overrides
      }
    }))
  };
}

// Wrapper for backward compatibility
export function getSingleTableConfig(fullConfig) {
  // Existing single-table logic, returns one table config
  const config = /* existing logic */;

  // Add fields needed for multi-table structure
  return {
    id: 'default',  // Legacy identifier
    targetTable: 'BigQueryData',  // Legacy table name
    ...config
  };
}
```

### 2. Service Entry Point Changes

**File:** `src/service.js`

```javascript
// Before: Single engine
// const syncEngine = new SyncEngine(config);

// After: Multiple engines (one per table)
const config = getPluginConfig();

// Create one SyncEngine per table
const syncEngines = config.tables.map(tableConfig => {
  return new SyncEngine(tableConfig);
});

// Initialize all engines
await Promise.all(syncEngines.map(engine => engine.initialize()));

// Start all sync loops
syncEngines.forEach(engine => engine.start());

// Store for cleanup
globals.syncEngines = syncEngines;
```

### 3. SyncEngine Changes

**File:** `src/sync-engine.js`

```javascript
constructor(tableConfig) {  // Changed from: config
  logger.info('[SyncEngine] Constructor called - initializing sync engine');

  // NEW: Table identification
  this.tableId = tableConfig.id;                    // e.g., "vessel_positions"
  this.targetTable = tableConfig.targetTable;       // e.g., "VesselPositions"

  this.initialized = false;
  this.config = tableConfig;
  this.client = new BigQueryClient({ bigquery: tableConfig });
  this.running = false;
  this.nodeId = null;
  this.clusterSize = null;
  this.currentPhase = 'initial';
  this.lastCheckpoint = null;
  this.pollTimer = null;
}

// Checkpoint ID becomes composite
async loadCheckpoint() {
  const checkpointId = `${this.tableId}_${this.nodeId}`;  // NEW: composite key
  logger.debug(`[SyncEngine.loadCheckpoint] Loading checkpoint: ${checkpointId}`);

  try {
    const checkpoint = await tables.SyncCheckpoint.get(checkpointId);
    return checkpoint;
  } catch (error) {
    if (error && (error.code === 'NOT_FOUND' || /not\s*found/i.test(error.message || ''))) {
      logger.debug('[SyncEngine.loadCheckpoint] No checkpoint found (first run)');
      return null;
    }
    throw error;
  }
}

async updateCheckpoint(records) {
  // ... extract timestamp logic ...

  this.lastCheckpoint = {
    id: `${this.tableId}_${this.nodeId}`,  // NEW: composite ID
    tableId: this.tableId,                  // NEW: for querying
    nodeId: this.nodeId,
    lastTimestamp: lastTimestampString,
    recordsIngested: this.lastCheckpoint.recordsIngested + records.length,
    lastSyncTime: new Date().toISOString(),
    phase: this.currentPhase,
    batchSize: this.calculateBatchSize()
  };

  await tables.SyncCheckpoint.put(this.lastCheckpoint);
}

// Write to dynamic Harper table
async ingestRecords(records) {
  logger.debug(`[SyncEngine.ingestRecords] Ingesting to table: ${this.targetTable}`);

  const validRecords = [];
  const timestampColumn = this.config.timestampColumn;

  for (const record of records) {
    try {
      const convertedRecord = convertBigQueryTypes(record);

      if (!convertedRecord[timestampColumn]) {
        logger.warn(`[SyncEngine.ingestRecords] Missing timestamp, skipping`);
        continue;
      }

      const { id: _unusedId, ...cleanedRecord } = convertedRecord;

      const mappedRecord = {
        ...cleanedRecord,
        _syncedAt: new Date().toISOString()
      };

      validRecords.push(mappedRecord);
    } catch (error) {
      logger.error(`[SyncEngine.ingestRecords] Error processing record: ${error.message}`);
    }
  }

  if (validRecords.length > 0) {
    // NEW: Dynamic table access
    const targetTable = tables[this.targetTable];

    if (!targetTable) {
      throw new Error(`Target table '${this.targetTable}' not found in Harper schema`);
    }

    transaction((txn) => {
      for (const rec of validRecords) {
        targetTable.create(rec);
      }
    });

    logger.info(`[SyncEngine.ingestRecords] Wrote ${validRecords.length} records to ${this.targetTable}`);
  }
}
```

### 4. Schema Changes

**File:** `schema/harper-bigquery-sync.graphql`

```graphql
# Updated checkpoint with composite ID and tableId index
type SyncCheckpoint @table {
  id: ID @primaryKey              # Format: "{tableId}_{nodeId}"
  tableId: String! @indexed       # For querying by table
  nodeId: Int!
  lastTimestamp: String!
  recordsIngested: Long!
  lastSyncTime: String!
  phase: String!
  batchSize: Int!
}

# Audit log with table tracking
type SyncAudit @table {
  id: ID! @primaryKey
  timestamp: String! @indexed
  tableId: String @indexed        # NEW: track which table
  nodeId: Int
  bigQueryCount: Long
  harperCount: Long
  delta: Long
  status: String!
  reason: String
  recordSample: String
}

# Target tables (user defines these in their schema)
# Example:

type VesselPositions @table {
  id: ID @primaryKey
  timestamp: String @indexed
  mmsi: String @indexed
  vessel_name: String
  latitude: Float
  longitude: Float
  speed_knots: Float
  heading: Float
  _syncedAt: String
}

type PortEvents @table {
  id: ID @primaryKey
  event_time: String @indexed
  port_id: String @indexed
  vessel_mmsi: String @indexed
  event_type: String
  status: String
  _syncedAt: String
}

type VesselMetadata @table {
  id: ID @primaryKey
  mmsi: String! @primaryKey
  last_updated: String @indexed
  vessel_name: String
  imo: String
  vessel_type: String
  flag: String
  callsign: String
  length: Float
  beam: Float
  draft: Float
  _syncedAt: String
}
```

## Integration Tests (TDD)

### Test Structure

**Level 1: Unit Tests** (generators)
- Test each generator independently
- Verify data quality and relationships
- Fast, no BigQuery needed

**Level 2: Synthesizer Integration Tests**
- Generate multi-table data to BigQuery
- Verify tables populated correctly
- Verify relationships

**Level 3: End-to-End Plugin Tests**
- Configure plugin for multi-table sync
- Run synthesizer to populate BigQuery
- Sync all tables to Harper
- Verify data isolation and checkpoint independence

### Key Test Scenarios

```javascript
describe('Multi-Table End-to-End', () => {

  it('should sync 3 tables independently', async () => {
    // 1. Generate test data in BigQuery
    await synthesizer.generateAll({
      tables: ['vessel_positions', 'port_events', 'vessel_metadata'],
      timeRange: last24Hours,
      recordCounts: { positions: 1000, events: 50, metadata: 100 }
    });

    // 2. Configure multi-table sync
    const config = {
      tables: [
        { id: 'vessel_positions', dataset: 'test', table: 'vessel_positions', ... },
        { id: 'port_events', dataset: 'test', table: 'port_events', ... },
        { id: 'vessel_metadata', dataset: 'test', table: 'vessel_metadata', ... }
      ]
    };

    // 3. Run sync
    await plugin.syncAll(config);

    // 4. Verify each Harper table
    const positions = await harperDB.query('SELECT COUNT(*) FROM VesselPositions');
    const events = await harperDB.query('SELECT COUNT(*) FROM PortEvents');
    const metadata = await harperDB.query('SELECT COUNT(*) FROM VesselMetadata');

    assert.equal(positions.count, 1000);
    assert.equal(events.count, 50);
    assert.equal(metadata.count, 100);
  });

  it('should maintain separate checkpoints per table', async () => {
    const checkpoints = await harperDB.query(`
      SELECT * FROM SyncCheckpoint
      WHERE tableId IN ('vessel_positions', 'port_events', 'vessel_metadata')
    `);

    assert.equal(checkpoints.length, 3);

    // Each has different lastTimestamp
    assert.notEqual(
      checkpoints.find(c => c.tableId === 'vessel_positions').lastTimestamp,
      checkpoints.find(c => c.tableId === 'port_events').lastTimestamp
    );
  });

  it('should handle one table failing without affecting others', async () => {
    // Simulate port_events table issues
    await bigquery.deleteTable('port_events');

    const results = await plugin.syncAll(config);

    assert.equal(results.vessel_positions.status, 'success');
    assert.equal(results.port_events.status, 'failed');
    assert.equal(results.vessel_metadata.status, 'success');
  });

  it('should sync tables at different rates', async () => {
    const config = {
      tables: [
        { id: 'vessel_positions', sync: { steadyBatchSize: 500 } },
        { id: 'port_events', sync: { steadyBatchSize: 100 } }
      ]
    };

    await plugin.syncAll(config);

    const checkpoints = await getCheckpoints();
    assert.equal(checkpoints.vessel_positions.batchSize, 500);
    assert.equal(checkpoints.port_events.batchSize, 100);
  });

  it('should support legacy single-table config', async () => {
    // Old config format should still work
    const legacyConfig = {
      bigquery: {
        projectId: 'test',
        dataset: 'maritime_tracking',
        table: 'vessel_positions',
        timestampColumn: 'timestamp',
        columns: ['*']
      }
    };

    const parsed = getPluginConfig(legacyConfig);

    // Should be wrapped in tables array
    assert.equal(parsed.tables.length, 1);
    assert.equal(parsed.tables[0].id, 'default');
    assert.equal(parsed.tables[0].targetTable, 'BigQueryData');
  });
});
```

### Test Data Fixtures

```javascript
// test/fixtures/multi-table-test-data.js
export const TEST_SCENARIOS = {
  small: {
    vessel_positions: 100,
    port_events: 10,
    vessel_metadata: 20,
    duration: '1 hour'
  },
  realistic: {
    vessel_positions: 10000,
    port_events: 500,
    vessel_metadata: 100,
    duration: '24 hours'
  },
  stress: {
    vessel_positions: 100000,
    port_events: 5000,
    vessel_metadata: 1000,
    duration: '7 days'
  }
};
```

## Implementation Timeline (TDD Approach)

### Phase 1: Tests & Synthesizer (Days 1-2)

**Day 1: Test Infrastructure**
- [ ] Write multi-table integration test suite (failing tests)
- [ ] Create test fixtures and data scenarios
- [ ] Set up test BigQuery dataset

**Day 2: Data Synthesizer**
- [ ] Build port-events-generator.js
- [ ] Build vessel-metadata-generator.js
- [ ] Build multi-table-orchestrator.js
- [ ] Make generator unit tests pass

### Phase 2: Plugin Implementation (Days 3-4)

**Day 3: Config & Schema**
- [ ] Extend config-loader.js for multi-table
- [ ] Update schema with composite checkpoint IDs
- [ ] Add backward compatibility detection
- [ ] Make config parsing tests pass

**Day 4: SyncEngine Updates**
- [ ] Add tableId and targetTable to SyncEngine
- [ ] Update checkpoint methods with composite IDs
- [ ] Update ingestRecords for dynamic tables
- [ ] Update service.js to loop over tables
- [ ] Make sync tests pass

### Phase 3: Validation (Day 5)

**Day 5: End-to-End Testing**
- [ ] Run full integration test suite
- [ ] Verify data isolation between tables
- [ ] Verify checkpoint independence
- [ ] Test backward compatibility
- [ ] Performance testing with 3 tables
- [ ] All tests green ✅

## Migration to Parallel SyncEngines (Future)

When you're ready to refactor to parallel SyncEngines, the changes are minimal:

### Step 1: Rename SyncEngine → TableSyncEngine
```javascript
// Just rename the class
export class TableSyncEngine {
  // Everything stays the same
}
```

### Step 2: Create SyncOrchestrator
```javascript
export class SyncOrchestrator {
  constructor(config) {
    this.engines = config.tables.map(tableConfig =>
      new TableSyncEngine(tableConfig)
    );
  }

  async initialize() {
    await Promise.all(this.engines.map(e => e.initialize()));
  }

  async startAll() {
    // Was: syncEngines.forEach(e => e.start())
    // Now: this.engines.forEach(e => e.start())
    this.engines.forEach(e => e.start());
  }

  async stopAll() {
    await Promise.all(this.engines.map(e => e.stop()));
  }

  getStatus() {
    return this.engines.map(e => ({
      tableId: e.tableId,
      status: e.getStatus()
    }));
  }
}
```

### Step 3: Update service.js
```javascript
// Before (TDD implementation):
const syncEngines = config.tables.map(t => new SyncEngine(t));
await Promise.all(syncEngines.map(e => e.initialize()));
syncEngines.forEach(e => e.start());

// After (parallel architecture):
const orchestrator = new SyncOrchestrator(config);
await orchestrator.initialize();
await orchestrator.startAll();
```

**That's it!** No changes to TableSyncEngine internals needed.

## Success Criteria

- ✅ Synthesizer generates 3 related BigQuery tables with realistic data
- ✅ Plugin syncs all 3 tables to separate Harper tables
- ✅ Each table has independent checkpoint
- ✅ Tables sync at different rates (different batch sizes)
- ✅ One table failure doesn't affect others
- ✅ Backward compatible with single-table configs
- ✅ All 105+ existing tests still pass
- ✅ New multi-table integration tests pass
- ✅ Easy refactoring path to parallel SyncEngines

## Open Questions

1. **Table Schema Definition:** Should users manually define Harper table schemas, or should we auto-generate from BigQuery schema?
   - **Decision:** Manual for now (simpler, more control)

2. **Error Handling:** If one table sync fails, should others continue?
   - **Decision:** Yes, independent failures (each table has own error handling)

3. **Startup Order:** Should tables start sequentially or in parallel?
   - **Decision:** Parallel (Promise.all for initialization and start)

4. **Resource Limits:** Maximum number of tables per instance?
   - **Decision:** No hard limit initially, monitor in production

## References

- Multi-Table Roadmap: `docs/MULTI-TABLE-ROADMAP.md`
- Current Implementation: `src/sync-engine.js`
- Query Builder: `src/query-builder.js`
- Config Loader: `src/config-loader.js`

---

**Document Status:** Approved Design
**Ready for Implementation:** Yes
**Estimated Timeline:** 5 days (TDD approach)
