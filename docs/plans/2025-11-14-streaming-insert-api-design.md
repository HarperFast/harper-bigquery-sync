# Streaming Insert API Support for BigQuery

**Date:** 2025-11-14
**Status:** Planning
**Issue:** [#8](https://github.com/HarperFast/harper-bigquery-sync/issues/8)
**Approach:** Test-Driven Development

## Overview

Add opt-in streaming insert API for BigQuery alongside the current load job API. Streaming inserts provide lower latency for real-time use cases but have a different cost model.

## Current Implementation

**File:** `src/bigquery.js`
**Method:** `insertBatch(records, maxRetries)`
**API Used:** Load Job API (`table.load()`)

**Current Behavior:**
1. Writes records to temporary NDJSON file
2. Uses `table.load()` to upload file
3. Waits for job completion
4. Cleans up temporary file
5. Retry logic with exponential backoff

**Benefits of Load Job API:**
- ✅ Free tier compatible (no per-row costs)
- ✅ Efficient for large batches
- ✅ Good for batch workloads

**Limitations:**
- ❌ Higher latency (seconds to minutes)
- ❌ Requires temporary file I/O
- ❌ Not suitable for real-time requirements

## Proposed: Streaming Insert API

**API:** `table.insert(rows)` method from @google-cloud/bigquery
**When to use:** Real-time use cases, lower latency requirements, production deployments

**Benefits:**
- ✅ Lower latency (sub-second to few seconds)
- ✅ No temporary files needed
- ✅ Direct insert to table
- ✅ Simpler code path

**Tradeoffs:**
- ❌ Costs money ($.01 per 200 MB, minimum $0.01/day)
- ❌ Row size limits (1 MB per row, 10 MB per request)
- ❌ Best-effort deduplication (not exact-once guarantees)

## Design

### Configuration

Add `useStreamingAPIs` flag to BigQuery config:

**config.yaml:**
```yaml
bigquery:
  projectId: my-project
  dataset: maritime_tracking
  table: vessel_positions
  timestampColumn: timestamp
  credentials: service-account-key.json
  location: US

  # Streaming insert API (off by default)
  useStreamingAPIs: false  # Set to true for lower latency
```

**Per-table configuration:**
```yaml
bigquery:
  # ... shared config ...
  tables:
    - id: vessel_positions
      dataset: maritime_tracking
      table: vessel_positions
      timestampColumn: timestamp
      columns: ['*']
      useStreamingAPIs: false  # High volume, batch is fine

    - id: port_events
      dataset: maritime_tracking
      table: port_events
      timestampColumn: event_time
      columns: ['*']
      useStreamingAPIs: true   # Real-time events, use streaming
```

### Implementation Strategy

**Option 1: Single method with conditional logic**
```javascript
async insertBatch(records, maxRetries = 5) {
  if (this.useStreamingAPIs) {
    return await this._insertStreaming(records);
  } else {
    return await this._insertLoadJob(records, maxRetries);
  }
}
```

**Option 2: Strategy pattern (cleaner, testable)**
```javascript
class LoadJobStrategy {
  async insert(table, records, schema) { /* current implementation */ }
}

class StreamingInsertStrategy {
  async insert(table, records) { /* streaming implementation */ }
}

class MaritimeBigQueryClient {
  constructor(config) {
    this.insertStrategy = config.useStreamingAPIs
      ? new StreamingInsertStrategy()
      : new LoadJobStrategy();
  }

  async insertBatch(records, maxRetries) {
    return await this.insertStrategy.insert(this.table, records, this.getSchema());
  }
}
```

**Recommendation:** Start with Option 1 (simpler), refactor to Option 2 if needed.

### Streaming Insert Implementation

```javascript
async _insertStreaming(records) {
  if (!records || records.length === 0) {
    throw new Error('No records to insert');
  }

  try {
    // BigQuery streaming insert API
    const response = await this.table.insert(records, {
      // Skip invalid rows (log and continue)
      skipInvalidRows: false,
      // Don't ignore unknown values (fail on schema mismatch)
      ignoreUnknownValues: false,
      // Template suffix for best-effort deduplication (optional)
      // templateSuffix: '_streaming'
    });

    return {
      success: true,
      recordCount: records.length,
      method: 'streaming'
    };
  } catch (error) {
    // Handle partial failures
    if (error.name === 'PartialFailureError') {
      console.error('Some rows failed to insert:', error.errors);

      // Log failed rows for debugging
      error.errors.forEach((err, index) => {
        console.error(`Row ${index} failed:`, err);
      });

      throw new Error(`Partial failure: ${error.errors.length} rows failed`);
    }

    throw error;
  }
}
```

### Error Handling

**Load Job API errors:**
- Network timeouts → Retry
- Rate limits (429) → Retry with backoff
- Server errors (5xx) → Retry
- Schema errors → Fail immediately
- Permissions errors → Fail immediately

**Streaming Insert API errors:**
- Partial failures → Log failed rows, throw error
- Quota exceeded → Retry with backoff
- Invalid schema → Fail immediately
- Row size too large → Fail immediately (log which row)

### Retry Logic

**Load Job API:** Already has retry logic with exponential backoff (up to 5 attempts)

**Streaming Insert API:** Add retry for transient errors
```javascript
async _insertStreaming(records, maxRetries = 3) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await this.table.insert(records, options);
    } catch (error) {
      lastError = error;

      // Check if retryable
      const isRetryable =
        error.code === 429 ||  // Quota exceeded
        error.code === 503 ||  // Service unavailable
        (error.code >= 500 && error.code < 600);  // Server errors

      if (!isRetryable || attempt === maxRetries) {
        throw error;
      }

      // Exponential backoff: 1s, 2s, 4s
      const backoffMs = Math.pow(2, attempt - 1) * 1000;
      console.log(`Streaming insert failed (attempt ${attempt}/${maxRetries}): ${error.message}`);
      console.log(`Retrying in ${backoffMs / 1000}s...`);

      await new Promise(resolve => setTimeout(resolve, backoffMs));
    }
  }

  throw lastError;
}
```

## Testing Strategy (TDD)

### Unit Tests

**Test file:** `test/bigquery-streaming.test.js`

```javascript
describe('BigQuery Streaming Inserts', () => {
  describe('Configuration', () => {
    it('should default to load job API when useStreamingAPIs is false');
    it('should use streaming API when useStreamingAPIs is true');
    it('should validate streaming config at initialization');
  });

  describe('Streaming insert method', () => {
    it('should successfully insert records using streaming API');
    it('should handle empty record array');
    it('should include record count in success response');
    it('should indicate method used (streaming vs load job)');
  });

  describe('Error handling', () => {
    it('should handle partial failures with detailed logging');
    it('should retry on quota exceeded (429)');
    it('should retry on service unavailable (503)');
    it('should not retry on schema errors');
    it('should not retry on invalid row data');
    it('should respect maxRetries limit');
  });

  describe('Performance', () => {
    it('should be faster than load job API for small batches');
    it('should handle row size limits gracefully');
    it('should handle request size limits (10 MB)');
  });

  describe('Backward compatibility', () => {
    it('should use load job API by default (existing behavior)');
    it('should maintain retry logic for load jobs');
    it('should clean up temp files with load jobs');
  });
});
```

### Integration Tests

**Test file:** `test/integration/bigquery-streaming.integration.test.js`

```javascript
describe('BigQuery Streaming Integration', () => {
  // Only run if BIGQUERY_INTEGRATION_TESTS=true
  before(function() {
    if (!process.env.BIGQUERY_INTEGRATION_TESTS) {
      this.skip();
    }
  });

  it('should insert records using streaming API against real BigQuery');
  it('should verify records are queryable immediately');
  it('should compare latency: streaming vs load job');
  it('should handle concurrent streaming inserts');
});
```

## Implementation Checklist

### Phase 1: Tests (Write First)
- [ ] Create `test/bigquery-streaming.test.js`
- [ ] Write unit tests for configuration
- [ ] Write unit tests for streaming insert method
- [ ] Write unit tests for error handling
- [ ] Write unit tests for backward compatibility
- [ ] Run tests (should all fail - RED phase)

### Phase 2: Implementation (Make Tests Pass)
- [ ] Add `useStreamingAPIs` config option
- [ ] Extract current load job logic to `_insertLoadJob()` method
- [ ] Implement `_insertStreaming()` method
- [ ] Update `insertBatch()` to dispatch based on config
- [ ] Add retry logic for streaming inserts
- [ ] Handle partial failure errors
- [ ] Run tests (should all pass - GREEN phase)

### Phase 3: Refactoring (Clean Up)
- [ ] Extract common error handling logic
- [ ] Add JSDoc documentation
- [ ] Consider strategy pattern if code gets complex
- [ ] Optimize retry backoff timing
- [ ] Run tests (should still pass - REFACTOR phase)

### Phase 4: Documentation
- [ ] Update config.yaml with examples
- [ ] Update README with streaming insert option
- [ ] Document cost implications
- [ ] Add decision guide: when to use streaming vs load jobs
- [ ] Update multi-table configuration examples

### Phase 5: Integration Testing
- [ ] Create integration test with real BigQuery (gated by env var)
- [ ] Test with maritime synthesizer
- [ ] Verify latency improvements
- [ ] Test error scenarios

## Cost Estimation

**Streaming Insert Costs (as of 2024):**
- $0.01 per 200 MB (compressed)
- Minimum $0.01 per day if any streaming inserts used
- No charge for load job API

**Example:**
- 144K records/day × 1 KB/record = 144 MB/day
- Cost: $0.01/day = $0.30/month
- Load job API: $0/month (free)

**Decision Guide:**
- **Use Load Job API (default):** Development, testing, batch workloads, cost-sensitive
- **Use Streaming API:** Production, real-time dashboards, low-latency requirements

## Migration Path

**Existing deployments:** No changes required (defaults to load job API)

**Enable streaming for a table:**
```yaml
tables:
  - id: vessel_positions
    useStreamingAPIs: true  # Add this line
```

**Test in development:**
1. Enable streaming for one table
2. Monitor latency and costs
3. Roll out to other tables if beneficial

## Success Metrics

- ✅ All tests pass (unit + integration)
- ✅ Backward compatible (default behavior unchanged)
- ✅ Configurable per table
- ✅ Lower latency verified (< 5s vs > 30s for load jobs)
- ✅ Error handling comprehensive
- ✅ Retry logic functional
- ✅ Documentation complete

## References

- [BigQuery Streaming Insert API](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery)
- [BigQuery Load Jobs](https://cloud.google.com/bigquery/docs/loading-data)
- [@google-cloud/bigquery Node.js SDK](https://googleapis.dev/nodejs/bigquery/latest/Table.html#insert)
- [Issue #8](https://github.com/HarperFast/harper-bigquery-sync/issues/8)
