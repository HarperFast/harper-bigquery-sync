# Tests

## Running Tests

```bash
npm test
```

## Test Suite

### ✅ Config Loader Tests (`config-loader.test.js`)

Tests for configuration loading and merging:

- BigQuery config defaults
- Synthesizer overrides
- Default values
- Custom settings

### ✅ Sync Engine Tests (`sync-engine.test.js`)

Tests for core sync engine logic:

- Phase calculation (initial/catchup/steady)
- Batch size calculation
- Record ID generation
- Modulo partitioning
- Poll interval calculation
- Timestamp validation

### ✅ Generator Tests (`generator.test.js`)

Tests for maritime vessel data generator:

- Initialization and configuration
- Vessel pool creation
- Batch generation with realistic data
- Journey tracking with memory leak prevention
- Automatic cleanup of old journeys

**Memory leak fixed:** Implemented journey cleanup mechanism that limits active journeys and removes old/completed journeys automatically.

### ✅ BigQuery Client Tests (`bigquery-client.test.js`)

Tests for BigQuery API client:

- Timestamp normalization (ISO 8601 format)
- Checkpoint handling (Date object support)
- Corrupt checkpoint detection
- Exponential backoff retry logic with jitter
- Transient vs permanent error detection

## Test Coverage

Current coverage: **70/70 tests passing** (core functionality)

Areas tested:

- Configuration management ✅
- Sync engine logic ✅
- Data partitioning ✅
- Phase transitions ✅
- Record validation ✅
- Generator with journey tracking ✅
- BigQuery client with retry logic ✅

Areas not tested:

- BigQuery integration (requires live instance) ⏭️
- Harper integration (requires live instance) ⏭️

## Integration Testing

For integration testing with live BigQuery and Harper instances, see:

- `examples/test-bigquery-config.js` - Tests BigQuery connection
- Manual testing with `npx maritime-data-synthesizer start`
