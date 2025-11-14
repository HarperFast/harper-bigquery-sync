# HarperDB Globals Logging System - Research Report for Issue #11

## Executive Summary

This research investigates the HarperDB globals logging system as it relates to issue #11 (Enhanced monitoring and observability). The current codebase has a **mixed logging approach** where:

- **202 logger.\* calls** properly use HarperDB's global logger (for Grafana integration)
- **70 console.\* calls** bypass the logging system entirely
- Only 3 files use console logging directly (bigquery.js, generator.js, service.js)

The **globals.js implementation uses a singleton pattern**, which is appropriate for this use case and aligns with HarperDB's architecture.

---

## 1. HarperDB Globals System Overview

### What is Globals?

The `globals` system in HarperDB is a key-value store that persists data at the application level, surviving across requests. It's commonly used for:

- Caching shared state
- Maintaining singleton instances
- Storing application configuration
- Passing state between different parts of the application

### Logger Integration with Grafana

HarperDB provides a **global `logger` object** that is automatically available in plugin code:

```javascript
logger.trace(); // Detailed diagnostic information
logger.debug(); // Debug-level messages
logger.info(); // General informational messages
logger.warn(); // Warning messages
logger.error(); // Error messages
logger.fatal(); // Fatal error messages
logger.notify(); // Notifications
```

**Key Point:** The `logger` global writes to Harper's centralized logging system, which can be **integrated with Grafana** for monitoring and observability. Console.log output bypasses this system entirely and goes to stdout/stderr.

---

## 2. Current Globals Implementation

### File: `/Users/ivan/Projects/harper-bigquery-sync/src/globals.js`

```javascript
class Globals {
	constructor() {
		if (Globals.instance) {
			return Globals.instance; // Singleton pattern
		}
		this.data = {};
		Globals.instance = this;
	}
	set(key, value) {
		this.data[key] = value;
	}
	get(key) {
		return this.data[key];
	}
}

const globals = new Globals();
export { globals, Globals };
export default Globals;
```

### Analysis: Singleton Pattern

**Is the singleton pattern necessary here?** Yes, for several reasons:

1. **Deterministic instance sharing**: Guarantees only one instance exists across module imports
2. **Multi-module consistency**: When multiple modules import globals, they all access the same object
3. **Thread safety in context**: Within Harper's execution model, the singleton is safe
4. **Application state durability**: A single instance persists data across requests

### Current Usage

Files using globals:

1. **`src/index.js`** - Entry point stores SyncEngines:

   ```javascript
   globals.set('syncEngines', syncEngines);
   globals.set('syncEngine', syncEngines[0]);
   globals.set('schemaManager', schemaManager);
   globals.set('validator', validationService);
   ```

2. **`src/resources.js`** - Resource layer retrieves engines:

   ```javascript
   await globals.get('syncEngine').getStatus();
   await globals.get('validator').runValidation();
   ```

3. **`src/sync-engine.js`** - Imports globals but doesn't currently use it

---

## 3. Current Logging Patterns in Codebase

### Statistics

| Metric                              | Count |
| ----------------------------------- | ----- |
| Total lines of src/ JavaScript code | 4,517 |
| Logger.\* calls                     | 202   |
| Console.\* calls                    | 70    |
| Files using console directly        | 3     |
| Files using logger                  | 11+   |

### Files Using Console (Logging Not via Grafana)

1. **`src/bigquery.js`** - MaritimeBigQueryClient (legacy synthesizer)
   - 23 console.log/error statements
   - Example: `console.log(\`Creating dataset: ${this.datasetId}\`)`

2. **`src/generator.js`** - MaritimeVesselGenerator (legacy synthesizer)
   - 2 console.log statements
   - Used for test data generation

3. **`src/service.js`** - MaritimeDataSynthesizer (legacy synthesizer)
   - 45 console.log/error statements
   - Used for CLI output during data initialization

### Files Using Logger (Grafana-Integrated)

1. **`src/sync-engine.js`** - 70+ logger.\* calls
   - Constructor, initialization, sync cycles
   - Cluster discovery, checkpoint management
   - Record ingestion and error handling

2. **`src/bigquery-client.js`** - 40+ logger.\* calls
   - Retry logic with exponential backoff
   - Query execution and performance tracking
   - Error categorization (retryable vs. fatal)

3. **`src/validation.js`** - 30+ logger.\* calls
   - Validation suite progress
   - Checkpoint validation
   - Smoke tests and spot checks

4. **`src/resources.js`** - 15+ logger.\* calls
   - Resource layer operations
   - Sync control endpoints
   - Data retrieval operations

5. **`src/index.js`** - 15+ logger.\* calls
   - Application initialization
   - Schema manager setup
   - Engine lifecycle

6. **`src/schema-manager.js`** - No logger calls currently
   - Good opportunity for enhancement

7. **`src/operations-client.js`** - Uses logger
   - API operations

---

## 4. Logging Content Analysis

### Types of Information Being Logged

#### Info Level (Progress & State Changes)

- Component initialization
- Cluster topology discovery
- Sync cycle start/end
- Checkpoint updates
- Phase transitions
- BigQuery operations completion

Example:

```javascript
logger.info(`[SyncEngine.initialize] Sync started - initializing with ${tableConfig.id}`);
logger.info(`[SyncEngine.runSyncCycle] Received ${records.length} records from BigQuery`);
logger.info(`[SyncEngine.updatePhase] Phase transition: ${oldPhase} -> ${this.currentPhase}`);
```

#### Debug Level (Detailed Tracing)

- Method entry/exit
- Parameter values
- Intermediate calculations
- Query execution details
- Retry attempt tracking

Example:

```javascript
logger.debug(`[SyncEngine.initialize] Discovering cluster topology`);
logger.debug(`[BigQueryClient.pullPartition] Query parameters - lastTimestamp: ${lastTimestamp}`);
```

#### Warn Level (Recoverable Issues)

- Missing or skipped records
- Transient errors being retried
- Deprecated configurations
- Non-fatal validation failures

Example:

```javascript
logger.warn(`[SyncEngine.ingestRecords] Missing timestamp column '${timestampColumn}', skipping record`);
logger.warn(`[BigQueryClient.pullPartition] Transient error - retrying in ${delay}ms`);
```

#### Error Level (Failures Requiring Attention)

- Unrecoverable failures
- Invalid data
- Missing configuration
- Operation failures

Example:

```javascript
logger.error(`[SyncEngine.runSyncCycle] Sync cycle error: ${error.message}`);
logger.error(`[SyncEngine.loadCheckpoint] Invalid timestamp: ${checkpoint.lastTimestamp}`);
```

---

## 5. Areas Missing Logging Instrumentation

### High Priority (Should Definitely Have Logging)

1. **`src/schema-manager.js`**
   - Currently no logger calls
   - Critical operations: table creation, schema migration
   - Missing: migration planning, attribute additions, type conflicts

2. **`src/operations-client.js`**
   - Minimal logging for API interactions
   - Missing: request details, response times, failure scenarios

3. **`src/query-builder.js`**
   - Complex query generation logic
   - Missing: query parameter logging, validation steps

4. **`src/type-converter.js`** & **`src/type-mapper.js`**
   - Data transformation logic
   - Missing: type conversion attempts, edge cases, failures

5. **`src/config-loader.js`**
   - Configuration loading and validation
   - Missing: config file location, parsing steps, validation results

6. **`src/index-strategy.js`**
   - Index strategy selection for Harper tables
   - Missing: strategy calculation logic, selected indexes

### Medium Priority (Would Benefit from Logging)

1. **`src/schema-leader-election.js`**
   - Leader election logic
   - Missing: election attempts, leader changes, conflicts

2. **`src/validators.js`**
   - Validation logic
   - Missing: validation rule execution, results

3. **Synthesizer components** (src/service.js, src/generator.js, src/bigquery.js)
   - These are test/utility components
   - Currently use console.log for CLI output
   - Could migrate to logger for production use

---

## 6. Multi-Threading Considerations

### Current Architecture

The codebase currently implements **distributed multi-node ingestion**, not true multi-threading:

1. **Cluster Discovery** (`src/sync-engine.js`):

   ```javascript
   const currentNodeId = [server.hostname, server.workerIndex].join('-');
   ```

   - Uses `server.workerIndex` from HarperDB's worker context
   - Multiple nodes discovered via `server.nodes` array

2. **Deterministic Partitioning**:
   - Modulo-based partition assignment using nodeId
   - Each node gets its own deterministic partition
   - No shared state conflicts between nodes

3. **Local Checkpoints**:
   - Each node maintains its own checkpoint
   - Checkpoint ID: `{tableId}_{nodeId}`
   - No coordination needed between nodes

### Threading Analysis

**Current usage of `server.workerIndex`**:

- Combines with hostname for unique node identity
- Enables multiple workers on same host to have different IDs
- No actual worker threads are created in the code
- HarperDB manages worker distribution

**Future Threading Consideration (Issue #9)**:
If multi-threaded ingestion is added in the future:

- Singleton globals would remain thread-safe within Harper's concurrency model
- Per-thread checkpoints may be needed: `{tableId}_{nodeId}_{threadId}`
- Logger calls should remain as-is (HarperDB logger handles concurrency)
- Would need thread-local storage for per-thread state

---

## 7. Singleton Pattern Necessity Assessment

### Why Singleton IS Necessary

1. **Shared State Persistence**: Multiple modules need access to the same engine instances
2. **Module Independence**: Avoids circular dependencies - any module can import globals
3. **Request Handling**: Persists data across multiple request handlers
4. **Configuration Sharing**: Single point of truth for application state

### When Singleton Could Be Problematic

1. **True Worker Threads**: If code runs in separate threads, each needs isolated state
2. **Testing**: Can cause state pollution between tests (can use before/after hooks)
3. **Multiple Application Instances**: Would share state incorrectly

### Assessment for This Codebase

**Verdict**: Singleton pattern is **appropriate** for:

- Single Harper plugin instance context
- Cluster-distributed (not thread-distributed) workloads
- Current architecture with one engine per instance

**Minor Enhancement Opportunity**:

```javascript
class Globals {
	constructor() {
		if (Globals.instance) return Globals.instance;
		this.data = {};
		this.version = '1.0.0';
		this.createdAt = new Date();
		Globals.instance = this;
	}

	set(key, value) {
		this.data[key] = value;
		logger.debug(`[Globals] Set ${key} = ${typeof value}`);
	}

	get(key) {
		const value = this.data[key];
		if (!value) logger.warn(`[Globals] Key '${key}' not found`);
		return value;
	}
}
```

---

## 8. Files Requiring Console → Logger Migration

### High Priority

| File               | console calls | Type               | Priority |
| ------------------ | ------------- | ------------------ | -------- |
| `src/bigquery.js`  | 23            | Legacy synthesizer | HIGH     |
| `src/service.js`   | 45            | Legacy synthesizer | HIGH     |
| `src/generator.js` | 2             | Legacy synthesizer | HIGH     |

### Notes on Synthesizer Files

These files (bigquery.js, service.js, generator.js) are the **legacy maritime data synthesizer** used for test data generation. They output to console for CLI feedback.

**Migration Strategy**:

1. Keep console output for backward compatibility with CLI
2. Add logger.\* calls for production deployment within Harper
3. Use environment variable to control output level

Example pattern:

```javascript
const ENV_LOGGING_MODE = process.env.LOGGING_MODE || 'cli';

function logProgress(message) {
	if (ENV_LOGGING_MODE === 'cli') {
		console.log(message);
	}
	logger.info(`[Synthesizer] ${message}`);
}
```

---

## 9. Grafana Integration Points

### How Logger Enables Grafana Integration

1. **Centralized Log Collection**:
   - HarperDB logger writes to structured logs
   - These can be exported to log aggregation (Loki, DataDog, etc.)
   - Grafana reads from these sources

2. **Structured Logging Benefits**:
   - Method names in brackets `[ClassName.method]` for easy filtering
   - Consistent log levels (INFO, DEBUG, WARN, ERROR)
   - Timestamped entries with context
   - Can create metrics and alerts

3. **Monitoring Dashboards** (Issue #11 Goal):

   ```sql
   -- Count ingestion errors
   | filter [SyncEngine] and level="ERROR"
   | stats count by tableId

   -- Track phase transitions
   | filter "Phase transition"
   | stats count by currentPhase

   -- Monitor checkpoint lag
   | filter [SyncEngine.updatePhase]
   | extract lag:number
   | stats avg(lag) by nodeId
   ```

### Required Logging for Grafana Monitoring

To build effective dashboards (Issue #11), need to log:

1. **Performance metrics**: Query times, batch sizes, throughput
2. **Health indicators**: Phase transitions, checkpoint progress, lag
3. **Error patterns**: Failure types, retry attempts, recovery actions
4. **Resource usage**: Records processed, memory operations, cleanup events
5. **Cluster state**: Node discovery, topology changes, partition distribution

---

## 10. Recommendations Summary

### Immediate Actions (Before Issue #11 Implementation)

1. **Migrate synthesizer console logging** (3 files, 70 calls)
   - Allows test data generation to work within Harper logging system
   - Enables monitoring of test data pipeline

2. **Add logging to schema components**
   - schema-manager.js: ~20 strategic logging points
   - operations-client.js: ~10 logging points
   - Covers critical data transformation and API operations

3. **Enhance globals.js with logging**
   - Add debug logging for get/set operations
   - Aids in troubleshooting state management issues

### For Issue #11 Implementation (Monitoring & Observability)

1. **Maintain current logging patterns**
   - Already structured for Grafana consumption
   - Consistent use of method names in brackets
   - Clear log levels and messages

2. **Add monitoring-specific logging**
   - Query execution times: `logger.info(\`Query executed in ${duration}ms\`)`
   - Record throughput: `logger.info(\`Processed ${batchSize} records in ${time}ms\`)`
   - Lag tracking: `logger.info(\`Current lag: ${lag}s, phase: ${phase}\`)`

3. **Create Grafana dashboard queries**
   - Aggregate by log method names for per-component metrics
   - Track phase transitions for health visualization
   - Monitor lag trending for alerting

### Code Quality

- **No refactoring needed**: Current structure is sound
- **Logging coverage**: 75% of codebase uses logger (202/272 logging calls)
- **Singleton pattern**: Appropriate for current architecture

---

## 11. Documentation References

### HarperDB Official Documentation

- **Globals Reference**: https://docs.harperdb.io/docs/technical-details/reference/globals
- **Debugging Applications**: https://docs.harperdb.io/docs/developers/applications/debugging
- **Standard Logging**: https://docs.harperdb.io/docs/administration/logging/logging

### Available Logger Methods

- `logger.trace()` - Most detailed diagnostic information
- `logger.debug()` - Debugging information
- `logger.info()` - General informational messages
- `logger.warn()` - Warning messages (recoverable issues)
- `logger.error()` - Error messages (unrecoverable issues)
- `logger.fatal()` - Fatal error messages
- `logger.notify()` - Special notification messages

### Project Issue References

- **#11** (THIS): Enhanced monitoring and observability with Grafana
- **#9**: Multi-threaded ingestion per node
- **#10**: Dynamic rebalancing for autoscaling

---

## Conclusion

The codebase is **well-structured for Grafana integration** via HarperDB's global logger:

1. ✅ **Singleton globals pattern is appropriate** for this architecture
2. ✅ **High coverage of logger usage** (202 structured log calls)
3. ✅ **Consistent logging patterns** enable Grafana dashboard creation
4. ⚠️ **3 files still use console** (legacy synthesizer - can be migrated)
5. ⚠️ **Some modules lack instrumentation** (schema-manager, type-mapper)
6. 🎯 **Ready for Issue #11 implementation** with minor additions

The main work for issue #11 will be creating the Grafana dashboards and alert configurations, not modifying the logging system itself. The groundwork is already in place.
