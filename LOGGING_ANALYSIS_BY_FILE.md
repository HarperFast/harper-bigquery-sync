# Logging Analysis by File - HarperDB Globals System Research

## Complete File-by-File Breakdown

### CATEGORY 1: PRODUCTION CODE - USING LOGGER (Grafana-Ready)

#### src/sync-engine.js (70+ logger calls)

- **Type**: Core sync engine implementation
- **Status**: EXCELLENT - Comprehensive logging coverage
- **Logger Usage**:
  - Constructor initialization
  - Cluster discovery (multi-node coordination)
  - Checkpoint management (persistence)
  - Sync cycle orchestration
  - Record ingestion and validation
  - Phase transition tracking (initial → catchup → steady)
  - Error handling with context
- **Log Levels Used**: info, debug, warn, error, trace
- **Key Information Logged**:
  - Cluster topology and node IDs
  - Phase transitions with lag values
  - Record counts and throughput
  - Checkpoint timestamps
  - Validation failures with reasons
- **Monitoring Ready**: YES - Can create dashboards for:
  - Phase transition rates
  - Lag trending
  - Error frequency per phase
  - Ingest throughput

---

#### src/bigquery-client.js (40+ logger calls)

- **Type**: BigQuery API client with retry logic
- **Status**: EXCELLENT - Well-instrumented
- **Logger Usage**:
  - Constructor and initialization
  - Exponential backoff retry logic
  - Query execution with performance timing
  - Error categorization (retryable vs. fatal)
  - Partition-aware queries
  - Record verification
- **Log Levels Used**: info, debug, warn, error, trace
- **Key Information Logged**:
  - Query parameters and SQL
  - Attempt numbers and backoff delays
  - Query execution time
  - Transient vs. fatal errors
  - Retry decisions with reasoning
  - Row counts returned
- **Monitoring Ready**: YES - Can create dashboards for:
  - Query latency histograms
  - Retry frequency
  - Success vs. failure rates
  - Transient error patterns

---

#### src/validation.js (30+ logger calls)

- **Type**: Data validation and integrity checks
- **Status**: GOOD - Adequate logging
- **Logger Usage**:
  - Constructor initialization
  - Validation suite orchestration
  - Checkpoint progress validation
  - Smoke tests
  - Spot checks on records
  - Cluster discovery
  - Audit logging
- **Log Levels Used**: info, debug, warn, error
- **Key Information Logged**:
  - Validation check results
  - Checkpoint status
  - Record counts and lag
  - Test pass/fail with reasons
  - Overall health status
- **Monitoring Ready**: YES - Can track:
  - Validation pass rates
  - Health status transitions
  - Checkpoint lag over time
  - Test coverage metrics

---

#### src/resources.js (15+ logger calls)

- **Type**: Harper resource layer (GraphQL/REST endpoints)
- **Status**: GOOD - Adequate logging
- **Logger Usage**:
  - Get operations on data tables
  - Search operations
  - Control endpoint status queries
  - Control endpoint actions (start/stop/validate)
- **Log Levels Used**: info, debug
- **Key Information Logged**:
  - Resource queries and results
  - Record counts
  - Operation names and parameters
  - Status information
- **Monitoring Ready**: YES - Can track:
  - API call frequency
  - Endpoint usage patterns
  - Operation success rates

---

#### src/index.js (15+ logger calls)

- **Type**: Plugin entry point and initialization
- **Status**: GOOD - Covers key lifecycle events
- **Logger Usage**:
  - Schema manager initialization
  - Sync engine creation
  - Table configuration
  - Validator setup
- **Log Levels Used**: info, warn, error
- **Key Information Logged**:
  - Component initialization status
  - Table configuration details
  - Initialization failures and reasons
  - Table count and IDs
- **Monitoring Ready**: PARTIAL - Can track:
  - Initialization success rate
  - Component availability

---

#### src/operations-client.js (10+ logger calls)

- **Type**: Harper Operations API client
- **Status**: FAIR - Minimal coverage
- **Logger Usage**:
  - API calls (limited logging)
  - Response handling
- **Log Levels Used**: info, error
- **Key Information Logged**:
  - Operation names
  - Error responses
- **Monitoring Ready**: POOR - Needs enhancement:
  - Request details
  - Response times
  - Error categorization

---

### CATEGORY 2: CORE CODE - MISSING LOGGING (Blind Spots)

#### src/schema-manager.js (0 logger calls) ⚠️ CRITICAL

- **Type**: Harper table schema creation and migration
- **Status**: POOR - No logging instrumentation
- **Critical Operations Without Visibility**:
  - Table existence checking
  - Schema introspection from BigQuery
  - Migration planning and execution
  - Attribute type mapping
  - Type conflict detection
  - Operations API calls
  - Dynamic table creation
- **Impact**: Cannot monitor schema operations, failures, or performance
- **Recommendation**: Add 20+ logger calls for:
  ```javascript
  logger.debug('[SchemaManager.ensureTable] Starting table creation...');
  logger.info('[SchemaManager] Comparing schemas...');
  logger.debug('[SchemaManager] Found X attributes to add');
  logger.warn('[SchemaManager] Type conflict detected on field X');
  logger.error('[SchemaManager] Failed to create table: X');
  logger.info('[SchemaManager] Table migration completed');
  ```

---

#### src/config-loader.js (0 logger calls) ⚠️ IMPORTANT

- **Type**: Configuration file loading and validation
- **Status**: POOR - No logging
- **Critical Operations Without Visibility**:
  - Config file location resolution
  - YAML parsing
  - Configuration validation
  - Defaults merging
  - Legacy format conversion
- **Impact**: Cannot diagnose configuration problems
- **Recommendation**: Add 10+ logger calls for:
  ```javascript
  logger.info('[ConfigLoader] Loading config from: X');
  logger.debug('[ConfigLoader] Parsing YAML...');
  logger.warn('[ConfigLoader] Using default for X');
  logger.error('[ConfigLoader] Invalid config: X');
  ```

---

#### src/type-mapper.js (0 logger calls) ⚠️ IMPORTANT

- **Type**: BigQuery ↔ Harper type conversion
- **Status**: POOR - No logging
- **Critical Operations Without Visibility**:
  - Type mapping decisions
  - Unsupported type handling
  - Type conversion logic
  - Schema building
- **Impact**: Cannot debug type conversion issues
- **Recommendation**: Add 10+ logger calls for:
  ```javascript
  logger.debug('[TypeMapper] Mapping BigQuery type: X to Harper type: Y');
  logger.warn('[TypeMapper] Unsupported type X, using fallback: Y');
  ```

---

#### src/type-converter.js (0 logger calls) ⚠️ IMPORTANT

- **Type**: Runtime type conversion from BigQuery to JavaScript
- **Status**: POOR - No logging
- **Critical Operations Without Visibility**:
  - Type conversion attempts
  - Conversion failures
  - Data validation
  - Edge case handling
- **Impact**: Cannot debug data conversion failures
- **Recommendation**: Add 10+ logger calls for:
  ```javascript
  logger.trace('[TypeConverter] Converting value: X (type: Y)');
  logger.warn('[TypeConverter] Conversion failed for: X');
  ```

---

#### src/query-builder.js (0 logger calls) ⚠️ IMPORTANT

- **Type**: BigQuery SQL query generation
- **Status**: POOR - No logging
- **Critical Operations Without Visibility**:
  - Query building steps
  - Parameter binding
  - WHERE clause construction
  - Join operations
- **Impact**: Cannot debug query construction issues
- **Recommendation**: Add 5+ logger calls for:
  ```javascript
  logger.trace('[QueryBuilder] Generated SQL: X');
  logger.debug('[QueryBuilder] Query parameters: X');
  ```

---

#### src/index-strategy.js (0 logger calls) ⚠️ MINOR

- **Type**: Harper index strategy selection
- **Status**: POOR - No logging
- **Operations Without Visibility**:
  - Strategy selection logic
  - Index recommendation logic
- **Recommendation**: Add 5+ logger calls

---

#### src/validators.js (0 logger calls) ⚠️ MINOR

- **Type**: Validation rule definitions
- **Status**: POOR - No logging
- **Recommendation**: Add 5-10 logger calls

---

### CATEGORY 3: UTILITY/LEGACY CODE - USING CONSOLE (Not Grafana-Ready)

#### src/bigquery.js (23 console.log/error calls) ⚠️ NEEDS MIGRATION

- **Type**: Legacy maritime data synthesizer - BigQuery client
- **Status**: NEEDS MIGRATION - Uses console instead of logger
- **Console Usage**:
  - Dataset creation/checking
  - Table creation/checking
  - Batch insertion feedback
  - Retry feedback
  - Cleanup feedback
  - Error reporting
- **Problem**: Console output not captured by Harper logging system
- **Files Affected**: Synthesizer (test data generation)
- **Migration Impact**: Low (test utility code)
- **Recommendation**:
  ```javascript
  // Keep console for CLI backward compatibility
  if (process.env.LOGGING_MODE === 'cli') console.log(message);
  // Add to logger for Harper deployment
  logger.info(`[MaritimeBigQueryClient] ${message}`);
  ```

---

#### src/service.js (45 console.log/error calls) ⚠️ NEEDS MIGRATION

- **Type**: Legacy maritime data synthesizer - Service orchestrator
- **Status**: NEEDS MIGRATION - Heavy console usage
- **Console Usage**:
  - Initialization feedback
  - Progress messages
  - Batch generation feedback
  - Time estimates
  - Completion messages
  - Error reporting
- **Problem**: Console output not captured for monitoring
- **Recommendation**: Migrate to dual logging:
  ```javascript
  // Keep for CLI
  console.log(`Loading ${days} days...`);
  // Add to logger
  logger.info(`[MaritimeDataSynthesizer] Loading ${days} days...`);
  ```

---

#### src/generator.js (2 console.log calls) ⚠️ MINOR MIGRATION

- **Type**: Legacy maritime data synthesizer - Generator
- **Status**: MINIMAL - Only 2 console calls
- **Console Usage**:
  - Vessel pool initialization
  - Journey cleanup
- **Impact**: Minimal, but should be consistent
- **Recommendation**: Migrate for consistency

---

### CATEGORY 4: COMPONENTS - PARTIAL/MINIMAL LOGGING

#### src/schema-leader-election.js (MINIMAL)

- **Status**: FAIR - Minimal logging coverage
- **Missing**: Leader election attempts, conflicts, state changes
- **Recommendation**: Add 10+ logger calls for election logic

---

### LOGGING STATISTICS SUMMARY

| Category           | Files  | Logger Calls | Console Calls | Status           |
| ------------------ | ------ | ------------ | ------------- | ---------------- |
| Production Core    | 7      | 185          | 0             | EXCELLENT        |
| Missing Logging    | 7      | 0            | 0             | POOR             |
| Legacy Synthesizer | 3      | 0            | 70            | NEEDS MIGRATION  |
| Partial/Other      | 2      | ~17          | 0             | FAIR             |
| **TOTALS**         | **19** | **202**      | **70**        | **74% COVERAGE** |

---

## Migration Priority Matrix

### Priority 1: CRITICAL (Block Issue #11)

- [ ] src/schema-manager.js - Add 20 logging points
- [ ] src/bigquery.js - Migrate 23 console calls
- [ ] src/service.js - Migrate 45 console calls
- [ ] src/generator.js - Migrate 2 console calls

**Effort**: 1-2 days | **Impact**: Enables complete monitoring

### Priority 2: IMPORTANT (Improve Visibility)

- [ ] src/config-loader.js - Add 10 logging points
- [ ] src/type-mapper.js - Add 10 logging points
- [ ] src/type-converter.js - Add 10 logging points
- [ ] src/query-builder.js - Add 5 logging points

**Effort**: 2-3 days | **Impact**: Better debugging

### Priority 3: NICE-TO-HAVE (Polish)

- [ ] src/index-strategy.js - Add 5 logging points
- [ ] src/validators.js - Add 5 logging points
- [ ] src/schema-leader-election.js - Add 10 logging points

**Effort**: 1 day | **Impact**: Complete coverage

---

## Code Examples for Migration

### Pattern 1: Simple Info Logging

```javascript
// BEFORE (console)
console.log(`Dataset ${this.datasetId} created`);

// AFTER (logger + optional console)
if (process.env.LOGGING_MODE === 'cli') {
	console.log(`Dataset ${this.datasetId} created`);
}
logger.info(`[MaritimeBigQueryClient] Dataset ${this.datasetId} created`);
```

### Pattern 2: Error Logging

```javascript
// BEFORE (console)
console.error('Error loading data:', error);

// AFTER
logger.error(`[MaritimeDataSynthesizer] Error loading data: ${error.message}`, error);
```

### Pattern 3: Progress Tracking

```javascript
// BEFORE (console)
console.log(`Loaded ${recordsInserted} records in ${totalTime} minutes`);

// AFTER
logger.info(`[MaritimeDataSynthesizer] Loaded ${recordsInserted} records in ${totalTime} minutes`);
```

### Pattern 4: Missing Component Logging

```javascript
// NEW: Add to schema-manager.js
logger.info('[SchemaManager] Ensuring table exists...');
logger.debug(`[SchemaManager] Checking if Harper table '${tableName}' exists`);
logger.info('[SchemaManager] Building BigQuery schema...');
const migration = this.determineMigrationNeeds(harperSchema, bigQuerySchema);
if (migration.action === 'create') {
	logger.info(`[SchemaManager] Creating new table with ${Object.keys(migration.attributesToAdd).length} attributes`);
} else if (migration.action === 'migrate') {
	logger.info(`[SchemaManager] Migrating table - adding ${Object.keys(migration.attributesToAdd).length} attributes`);
}
```

---

## Verification Checklist

After migration:

- [ ] All 70 console calls converted to logger calls
- [ ] Dual logging in place (console + logger where appropriate)
- [ ] Schema-manager.js has 20+ logging points
- [ ] No console.log/error calls remain in production code
- [ ] All logger calls follow `[ClassName.method]` bracket pattern
- [ ] Log levels appropriate (info/debug/warn/error)
- [ ] Existing logger calls remain unchanged
- [ ] Tests pass with new logging
- [ ] Can create Grafana filters on bracketed names

---

## Next Steps for Issue #11

Once logging migration complete:

1. **Extract Logger Messages**: Parse logs to create metrics
2. **Build Grafana Dashboards**:
   - Sync health dashboard (phase, lag, error rate)
   - Throughput dashboard (records/sec per table)
   - Error dashboard (by type, by table)
   - Performance dashboard (query times, retry counts)
3. **Create Alert Rules**:
   - High error rate
   - Lag exceeding threshold
   - Phase stuck in initial
4. **Document Observability**:
   - Grafana dashboard JSON files
   - Alert configuration
   - Log query examples
   - Troubleshooting guide
