# Maritime Data Synthesizer - Quick Start Guide

This guide will help you quickly set up and run the maritime vessel data synthesizer.

## Prerequisites

- Node.js >= 20
- Google Cloud Platform account with BigQuery enabled
- Service account key with BigQuery permissions

## Setup (5 minutes)

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure BigQuery Connection

The synthesizer uses the same configuration file (`config.yaml`) as the BigQuery plugin. This keeps everything simple and consistent.

Edit `config.yaml`:

```yaml
bigquery:
  projectId: your-gcp-project-id # Your GCP project
  dataset: maritime_tracking # Plugin reads from here / Synthesizer writes to here (default)
  table: vessel_positions
  timestampColumn: timestamp
  credentials: service-account-key.json # Path to your service account key
  location: US

  # Optional: Column selection (NEW) - fetch only specific columns to reduce costs
  # Omit or use "*" to fetch all columns (default behavior)
  # columns: [timestamp, mmsi, vessel_name, latitude, longitude]  # Must include timestampColumn
  # columns: "*"  # Fetch all columns (default)

# Optional: Override synthesizer settings (defaults shown below)
synthesizer:
  # dataset: maritime_tracking           # Optional: Use different dataset (defaults to bigquery.dataset)
  # table: vessel_positions              # Optional: Use different table (defaults to bigquery.table)

  # totalVessels: 100000                 # Total vessel pool (default: 100000)
  # batchSize: 100                       # Positions per batch (default: 100)
  # generationIntervalMs: 60000          # 60 seconds between batches (default: 60000)

  # retentionDays: 30                    # Keep 30 days of data (default: 30)
  # cleanupIntervalHours: 24             # Clean up daily (default: 24)
```

**Key Points:**

- By default, synthesizer writes to the same dataset/table as the plugin reads from
- The `bigquery` section is shared (same project, credentials, and target)
- The `synthesizer` section is optional - only needed to override defaults or adjust generation settings
- With the config above (all commented out), both plugin and synthesizer use `maritime_tracking.vessel_positions`

### 3. Add Your Service Account Key

Place your GCP service account JSON key in the project root:

```bash
cp ~/Downloads/your-service-account-key.json service-account-key.json
```

Make sure this filename matches the `credentials` field in config.yaml.

## Usage

### Initialize with Historical Data

Load 30 days of historical vessel positions (takes ~30-60 minutes):

```bash
npx maritime-data-synthesizer initialize 30
```

This will:

- Create the `maritime_tracking` dataset (if needed)
- Create the `vessel_positions` table with proper schema
- Load ~4.3 million historical vessel position records
- Show progress updates every 10 batches

**Expected output:**

```
Configuration loaded from config.yaml
  Project: your-project
  Dataset: maritime_tracking
  Table: vessel_positions

Loading 30 days of historical data...
  Records per day: 144,000
  Total records: 4,320,000
  Total batches: 43,200
  Estimated time: 1440 minutes

Progress: 5.0% | Batch 2160/43200 | Records: 216,000/4,320,000 | Rate: 120 records/sec | ETA: 55 min
...
Historical data loaded: 4,320,000 records in 58.3 minutes
```

### Start Continuous Generation (Rolling Window Mode)

Generate new vessel positions in real-time with automatic window maintenance:

```bash
npx maritime-data-synthesizer start
```

**Rolling Window Mode** (default):

- Checks current data range
- Automatically backfills if you have less than the target window (e.g., 30 days)
- Generates new vessel positions continuously (100 every 60 seconds by default)
- Automatically cleans up data older than the retention window
- Keeps running until you press Ctrl+C

**Example scenarios:**

_Fresh start (no data):_

```
$ npx maritime-data-synthesizer start
Checking data range (target: 30 days)...
No existing data found. Initializing with historical data...
Loading 30 days... (takes ~30-60 min)
Starting continuous generation...
```

_Partial data (only 7 days):_

```
$ npx maritime-data-synthesizer start
Checking data range (target: 30 days)...
Found 1,008,000 records covering 7 days
Backfilling 23 days to reach 30-day window...
Starting continuous generation...
```

_Sufficient data (30+ days):_

```
$ npx maritime-data-synthesizer start
Checking data range (target: 30 days)...
Found 4,320,000 records covering 30 days
Data window is sufficient (30/30 days)
Starting continuous generation...
```

**Skip backfill (generation only):**

```bash
npx maritime-data-synthesizer start --no-backfill
```

This will only generate new data going forward without checking or backfilling historical data.

**Expected output:**

```
Configuration loaded from config.yaml
  Project: your-project
  Dataset: maritime_tracking
  Table: vessel_positions

Starting Maritime Data Synthesizer...
  Batch size: 100 vessels
  Generation interval: 60 seconds
  Records per day: ~144000
  Retention: 30 days
  Cleanup interval: 24 hours

Press Ctrl+C to stop

Batch inserted: 100 records | Total: 100 records | Batches: 1
Batch inserted: 100 records | Total: 200 records | Batches: 2
...
```

### View Statistics

Check table statistics (in another terminal while running):

```bash
npx maritime-data-synthesizer stats
```

**Example output:**

```
Configuration loaded from config.yaml
  Project: your-project
  Dataset: maritime_tracking
  Table: vessel_positions

Fetching statistics...

Table Metadata:
  Size: 1,234.56 MB
  Rows: 4,320,000
  Created: 11/7/2025, 10:00:00 AM
  Modified: 11/7/2025, 10:58:23 AM

Data Statistics:
  Total Records: 4,320,000
  Unique Vessels: 10,000
  Vessel Types: 6
  Unique Positions: 3,456,789
  Oldest Record: 2025-10-08T10:00:00.000Z
  Newest Record: 2025-11-07T10:58:00.000Z
```

### Clear or Reset Data

**Clear data (keeps schema):**

```bash
npx maritime-data-synthesizer clear
```

- Removes all data from table
- Preserves table schema and structure
- Useful for quick data refresh

**Reset everything (deletes table):**

```bash
npx maritime-data-synthesizer reset 30
```

- Stops the service if running
- Deletes the entire table
- Reinitializes with 30 days of data

## Configuration Tips

### For Different Data Volumes

**High Volume** (1.44M records/day):

```yaml
synthesizer:
  batchSize: 1000
  generationIntervalMs: 60000
```

**Low Volume** (14.4K records/day):

```yaml
synthesizer:
  batchSize: 100
  generationIntervalMs: 600000 # 10 minutes
```

**Test/Development** (1.44K records/day):

```yaml
synthesizer:
  batchSize: 10
  generationIntervalMs: 600000 # 10 minutes
```

### For Longer/Shorter Retention

**90-day retention:**

```yaml
synthesizer:
  retentionDays: 90
  cleanupIntervalHours: 24
```

**7-day retention (for testing):**

```yaml
synthesizer:
  retentionDays: 7
  cleanupIntervalHours: 6 # Clean up more frequently
```

## Verify It's Working

### Query in BigQuery Console

Go to BigQuery and run:

```sql
SELECT
  vessel_type,
  COUNT(*) as count,
  AVG(speed_knots) as avg_speed
FROM `your-project.maritime_tracking.vessel_positions`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY vessel_type
ORDER BY count DESC
```

### Check Recent Vessels

```sql
SELECT
  mmsi,
  vessel_name,
  vessel_type,
  latitude,
  longitude,
  speed_knots,
  status,
  destination,
  timestamp
FROM `your-project.maritime_tracking.vessel_positions`
ORDER BY timestamp DESC
LIMIT 10
```

## Common Issues

### "Could not load the default credentials"

Make sure:

1. Your service account key file exists
2. The path in config.yaml matches the filename
3. The service account has BigQuery permissions

### "Dataset already exists" but table creation fails

The dataset might exist from a previous run. Either:

- Delete it in BigQuery console and retry
- Or just run with the existing dataset (table will be created)

### Slow historical data loading

This is normal! Loading 30 days takes ~1 hour because:

- 4.3 million records need to be generated
- Each batch is rate-limited to avoid overwhelming BigQuery
- Free tier has load job limits (1,500/day)

To speed up (for testing):

```bash
npx maritime-data-synthesizer initialize 7  # Just 7 days
```

## Next Steps

- Read the full documentation: `docs/maritime-data-synthesizer.md`
- Customize vessel types and ports in: `src/generator.js`
- Add custom queries for your use case
- Integrate with visualization tools (Looker, Data Studio, etc.)

## Stopping the Service

Press `Ctrl+C` in the terminal running the synthesizer. It will:

- Stop generating new data
- Clean up gracefully
- Show final statistics

## Getting Help

- Check logs for error messages
- Verify config.yaml syntax (must be valid YAML)
- Ensure BigQuery permissions are correct
- Look at the full docs in `docs/maritime-data-synthesizer.md`
