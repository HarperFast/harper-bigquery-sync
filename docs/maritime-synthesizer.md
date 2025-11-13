# Maritime Vessel Data Synthesizer 🚢

A production-ready synthetic data generator that creates realistic vessel tracking data at global scale, designed to work seamlessly with the BigQuery plugin.

## What It Does

Generates millions of realistic vessel position records with:
- **100,000+ vessels** in the global fleet
- **6 vessel types** (container ships, bulk carriers, tankers, cargo, passenger, fishing)
- **29 major ports** worldwide with realistic traffic patterns
- **Physics-based movement** between ports with proper speeds and courses
- **Automatic retention** management (30-day rolling window by default)

## Quick Start

### 1. Configure (uses same config as the plugin!)

Edit `config.yaml`:

```yaml
bigquery:
  projectId: your-gcp-project-id
  dataset: your_dataset          # Plugin reads from here
  table: your_table              # Plugin reads from here
  credentials: service-account-key.json
  location: US

# Optional: Override dataset/table for synthesizer
# By default, synthesizer writes to the same dataset/table as the plugin
synthesizer:
  # dataset: maritime_tracking   # Optional: Override to use different dataset
  # table: vessel_positions      # Optional: Override to use different table
  batchSize: 100                 # Optional: Defaults to 100
  generationIntervalMs: 60000    # Optional: Defaults to 60000
```

**That's it!** The synthesizer uses the same BigQuery connection and target as your plugin by default.

### 2. Initialize with Historical Data

```bash
npx maritime-data-synthesizer initialize 30
```

Loads 30 days of historical vessel positions (~4.3M records in ~1 hour).

### 3. Start Generating (Rolling Window)

```bash
npx maritime-data-synthesizer start
```

**Automatic rolling window mode:**
- Checks current data and backfills if needed
- Generates 100 vessel positions every 60 seconds (144K records/day)
- Maintains exactly N days of data automatically
- Cleans up data older than retention window

**Just works!** No need to initialize first - the service handles everything.

## Why Use This?

### For Testing the BigQuery Plugin
- Generate realistic test data without accessing real vessel tracking systems
- Test data pipeline performance at scale
- Validate data transformation and aggregation logic

### For Development
- Local development without production data access
- Reproducible test datasets
- Privacy-compliant synthetic data

### For Analytics & ML
- Train predictive models for vessel arrival times
- Develop anomaly detection algorithms
- Build maritime traffic visualization dashboards
- Test geospatial query performance

## Key Features

### Realistic Data
- Vessels move between actual ports (Singapore, Rotterdam, Los Angeles, etc.)
- Proper speeds for vessel types (8-30 knots)
- Port operations (anchoring, mooring) and ocean transit
- Journey tracking with destinations and ETAs

### Optimized for BigQuery
- Uses load jobs (free tier compatible)
- Partitioned by timestamp
- Clustered by vessel_type, mmsi, report_date
- Automatic cleanup of old data

### Simple Configuration
- **Shares config with the plugin** - no duplicate setup!
- Same project, credentials, location
- Just configure the target dataset/table

### Production-Ready
- Event-driven architecture
- Comprehensive error handling
- Progress tracking and statistics
- Graceful shutdown

## Data Schema

Each position record includes:

```
mmsi                 STRING      9-digit Maritime Mobile Service Identity
imo                  STRING      7-digit IMO number
vessel_name          STRING      "MV OCEAN FORTUNE 42"
vessel_type          STRING      CONTAINER, BULK_CARRIER, TANKER, etc.
flag                 STRING      Two-letter country code
length               INTEGER     Vessel length in meters
beam                 INTEGER     Vessel width in meters
draft                FLOAT       Vessel draft in meters
latitude             FLOAT       Current position (-90 to 90)
longitude            FLOAT       Current position (-180 to 180)
speed_knots          FLOAT       Current speed in knots
course               INTEGER     Direction of travel (0-360°)
heading              INTEGER     Vessel heading (0-360°)
status               STRING      UNDERWAY_USING_ENGINE, AT_ANCHOR, MOORED
destination          STRING      Destination port name
eta                  TIMESTAMP   Estimated time of arrival
timestamp            TIMESTAMP   Record timestamp (ISO 8601)
report_date          STRING      Date in YYYYMMDD format
```

## Example Queries

### Active Vessels by Type
```sql
SELECT
  vessel_type,
  COUNT(DISTINCT mmsi) as vessel_count,
  AVG(speed_knots) as avg_speed
FROM `your-project.maritime_tracking.vessel_positions`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY vessel_type
ORDER BY vessel_count DESC
```

### Vessels in a Region
```sql
SELECT *
FROM `your-project.maritime_tracking.vessel_positions`
WHERE latitude BETWEEN 35.0 AND 45.0
  AND longitude BETWEEN -130.0 AND -115.0
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY timestamp DESC
```

### Port Activity
```sql
SELECT
  destination as port,
  COUNT(*) as vessel_count
FROM `your-project.maritime_tracking.vessel_positions`
WHERE status IN ('AT_ANCHOR', 'MOORED')
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY port
ORDER BY vessel_count DESC
LIMIT 10
```

## CLI Commands

```bash
# Start with rolling window (recommended - auto-backfills)
npx maritime-data-synthesizer start

# Start without backfill (generation only)
npx maritime-data-synthesizer start --no-backfill

# Initialize with historical data (manual approach)
npx maritime-data-synthesizer initialize [days]

# View table statistics
npx maritime-data-synthesizer stats

# Clear all data (keeps table schema)
npx maritime-data-synthesizer clear

# Delete table and all data
npx maritime-data-synthesizer clean

# Reset with new historical data
npx maritime-data-synthesizer reset [days]

# Show help
npx maritime-data-synthesizer help
```

## Configuration Options

In `config.yaml`:

```yaml
bigquery:
  projectId: your-gcp-project-id
  dataset: your_dataset             # Default target for synthesizer
  table: your_table                 # Default target for synthesizer
  credentials: service-account-key.json

synthesizer:
  # BigQuery targets (optional overrides - defaults to bigquery.dataset/table)
  # dataset: maritime_tracking      # Override: Use different dataset
  # table: vessel_positions         # Override: Use different table

  # Data generation (optional - these are the defaults)
  # totalVessels: 100000            # Vessel pool size
  # batchSize: 100                  # Positions per batch
  # generationIntervalMs: 60000     # Batch interval (ms)

  # Data retention (optional - these are the defaults)
  # retentionDays: 30               # Keep 30 days
  # cleanupIntervalHours: 24        # Clean up daily
```

**Records per day** = `(86,400,000 / generationIntervalMs) × batchSize`

Examples:
- Default (100 × 60s): **144,000 records/day**
- High volume (1000 × 60s): **1,440,000 records/day**
- Low volume (10 × 600s): **1,440 records/day**

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  config.yaml                     │
│  ┌────────────────┐    ┌────────────────────┐  │
│  │    bigquery    │    │   synthesizer      │  │
│  │  - projectId   │────│  - dataset         │  │
│  │  - credentials │    │  - table           │  │
│  │  - location    │    │  - batchSize       │  │
│  └────────────────┘    └────────────────────┘  │
└─────────────────────────────────────────────────┘
           │                        │
           ├────────────────────────┤
           │  Shared Connection     │
           │  & Target (default)    │
           ├────────────────────────┤
           ▼                        ▼
┌───────────────────┐    ┌──────────────────────┐
│  BigQuery Plugin  │    │  Maritime Synthesizer│
│  Reads from:      │    │  Writes to:          │
│  maritime_tracking│◄───┤  maritime_tracking   │
│  .vessel_positions│    │  .vessel_positions   │
└───────────────────┘    └──────────────────────┘
```

## Performance

- **Memory**: ~150 MB baseline
- **CPU**: <5% on modern hardware
- **Throughput**: 100-1,000+ records/minute
- **BigQuery**: Free tier compatible (uses load jobs)

## Rolling Window Mode

The synthesizer supports **rolling window mode** - a "set it and forget it" operation that automatically maintains a fixed N-day window of data.

### How It Works

When you run `npx maritime-data-synthesizer start`, the service:

1. **Checks** current data state on startup
2. **Backfills** automatically if data is missing or insufficient
3. **Generates** new data continuously going forward
4. **Cleans up** old data automatically beyond the retention window
5. **Maintains** exactly N days of data indefinitely

### Example Scenarios

#### Fresh Start (No Data)
```
Checking data range (target: 30 days)...
❌ No existing data found

Action: Initializing with 30 days of historical data
  • Will load ~4,320,000 records
  • Estimated time: ~60 minutes

✓ Initialization complete
✓ Starting continuous generation
✓ Rolling window active: 30 days
```

#### Partial Data (7 days)
```
Checking data range (target: 30 days)...
✓ Found 1,008,000 records covering 7 days
⚠️  Data window insufficient (7/30 days)

Action: Backfilling 23 days
  • Will load ~3,312,000 records

✓ Backfill complete
✓ Rolling window active: 30 days
```

#### Sufficient Data (30+ days)
```
Checking data range (target: 30 days)...
✓ Found 4,320,000 records covering 30 days
✓ Data window sufficient (30/30 days)

✓ Starting continuous generation immediately
✓ Rolling window active: 30 days
```

### Benefits

**Zero Manual Intervention** - No need to run `initialize` before `start`. Just start the service and it handles everything.

**Old workflow:**
```bash
npx maritime-data-synthesizer initialize 30  # Manual step
npx maritime-data-synthesizer start          # Then start
```

**New workflow:**
```bash
npx maritime-data-synthesizer start          # That's it!
```

**Graceful Recovery** - Service can be stopped and restarted at any time. It will check current state, backfill if needed, and resume.

**Consistent State** - Always maintains exactly N days of data:
- New data continuously added at the front
- Old data automatically removed from the back
- Window size remains constant

**Production-Ready** - Perfect for long-running deployments with no manual maintenance needed.

### Configuration

Rolling window behavior is controlled by `retentionDays` in `config.yaml`:

```yaml
synthesizer:
  retentionDays: 30           # Target window size
  cleanupIntervalHours: 24    # How often to clean up old data
```

### Skip Backfill

To only generate new data without backfilling:

```bash
npx maritime-data-synthesizer start --no-backfill
```

This mode:
- Skips data range check
- No backfilling
- Only generates new data going forward
- Useful if you want manual control

## Documentation

- **Quick Start**: `docs/quickstart.md` - Get up and running in 5 minutes
- **Config Reference**: See comments in `config.yaml`
- **Rolling Window**: See "Rolling Window Mode" section above

## Use Cases

✅ Test BigQuery data pipelines
✅ Develop maritime tracking applications
✅ Train ML models for vessel predictions
✅ Build visualization dashboards
✅ Load testing and performance validation
✅ Privacy-compliant development data

## Example Output

```
Configuration loaded from config.yaml
  Project: irjudson-demo
  Dataset: maritime_tracking
  Table: vessel_positions

Maritime Data Synthesizer started
  Batch size: 100 vessels
  Generation interval: 60 seconds
  Records per day: ~144,000
  Retention: 30 days

Batch inserted: 100 records | Total: 4,320,100 | Batches: 43,201
```

## Requirements

- Node.js >= 20
- Google Cloud Platform account
- BigQuery API enabled
- Service account with BigQuery permissions

## License

Apache-2.0

---

**Happy sailing!** 🌊⚓
