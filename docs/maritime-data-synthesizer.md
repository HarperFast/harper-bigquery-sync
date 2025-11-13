# Maritime Vessel Data Synthesizer

A production-grade synthetic data generator for maritime vessel tracking, designed to create realistic vessel movement patterns at global scale with millions of data points.

## Overview

The Maritime Vessel Data Synthesizer generates realistic synthetic tracking data for vessels (ships) moving around the world, emulating real-world maritime traffic patterns. It includes:

- **100,000+ vessels** in the global fleet pool
- **30+ major ports** across all continents with weighted traffic distribution
- **6 vessel types** (container ships, bulk carriers, tankers, cargo, passenger, fishing)
- **Realistic movement patterns** including port stays, ocean crossings, and shipping lanes
- **Physics-based navigation** with accurate distance and bearing calculations
- **Automatic data retention** with configurable rolling windows
- **BigQuery integration** optimized for free tier usage

## Features

### Realistic Maritime Patterns

- **Port Operations**: Vessels anchor or moor at major ports with realistic dwell times
- **Ocean Transit**: Ships follow great circle routes between ports with appropriate speeds
- **Speed Variations**: Different vessel types have realistic speed ranges (8-30 knots)
- **Status Tracking**: UNDERWAY_USING_ENGINE, AT_ANCHOR, MOORED, etc.
- **Global Distribution**: Traffic weighted by actual port volumes (Singapore, Shanghai, Rotterdam, Los Angeles, etc.)

### Data Schema

Each vessel position record includes:

| Field         | Type      | Description                                                |
| ------------- | --------- | ---------------------------------------------------------- |
| `mmsi`        | STRING    | 9-digit Maritime Mobile Service Identity                   |
| `imo`         | STRING    | 7-digit International Maritime Organization number         |
| `vessel_name` | STRING    | Vessel name (e.g., "MV OCEAN FORTUNE 42")                  |
| `vessel_type` | STRING    | CONTAINER, BULK_CARRIER, TANKER, CARGO, PASSENGER, FISHING |
| `flag`        | STRING    | Two-letter country code                                    |
| `length`      | INTEGER   | Vessel length in meters                                    |
| `beam`        | INTEGER   | Vessel width in meters                                     |
| `draft`       | FLOAT     | Vessel draft (depth) in meters                             |
| `latitude`    | FLOAT     | Current latitude (-90 to 90)                               |
| `longitude`   | FLOAT     | Current longitude (-180 to 180)                            |
| `speed_knots` | FLOAT     | Current speed in knots                                     |
| `course`      | INTEGER   | Direction of travel (0-360 degrees)                        |
| `heading`     | INTEGER   | Vessel heading (0-360 degrees)                             |
| `status`      | STRING    | Vessel operational status                                  |
| `destination` | STRING    | Destination port name                                      |
| `eta`         | TIMESTAMP | Estimated time of arrival                                  |
| `timestamp`   | TIMESTAMP | Record timestamp                                           |
| `report_date` | STRING    | Date in YYYYMMDD format                                    |

## Installation

```bash
# Clone or navigate to the project
cd harper-bigquery-sync

# Install dependencies
npm install

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your GCP project ID
```

## Configuration

Create a `.env` file with the following variables:

```bash
# Required
GCP_PROJECT_ID=your-gcp-project-id

# Optional (with defaults)
BIGQUERY_DATASET=maritime_tracking
BIGQUERY_TABLE=vessel_positions
GENERATION_INTERVAL_MS=60000     # 60 seconds between batches
BATCH_SIZE=100                   # 100 vessel positions per batch
TOTAL_VESSELS=100000             # 100,000 vessels in pool
RETENTION_DAYS=30                # Keep 30 days of data
CLEANUP_INTERVAL_HOURS=24        # Clean up old data daily
```

### Configuration Guide

**GENERATION_INTERVAL_MS**: Time between batches

- Lower = more frequent updates, more BigQuery load jobs
- Default: 60000 (1 minute)
- Range: 10000-300000 (10 seconds to 5 minutes)

**BATCH_SIZE**: Records per batch

- Higher = fewer BigQuery jobs, more records per insert
- Default: 100
- Range: 50-1000
- Free tier limit: ~1,500 load jobs per day

**Records per day** = `(86,400,000 / GENERATION_INTERVAL_MS) × BATCH_SIZE`

- Default: `(86400000 / 60000) × 100 = 144,000 records/day`
- At 1000 batch size: 1.44M records/day

## Usage

### CLI Commands

```bash
# Initialize BigQuery and load historical data
npx maritime-data-synthesizer initialize [days]
# Example: Load 30 days of historical data
npx maritime-data-synthesizer initialize 30

# Start continuous data generation
npx maritime-data-synthesizer start

# View statistics
npx maritime-data-synthesizer stats

# Clear all data from table (keeps schema)
npx maritime-data-synthesizer clear

# Delete all data and table
npx maritime-data-synthesizer clean

# Delete and reinitialize with new historical data
npx maritime-data-synthesizer reset [days]
# Example: Reset with 60 days
npx maritime-data-synthesizer reset 60

# Show help
npx maritime-data-synthesizer help
```

### Typical Workflow

1. **Initialize with historical data**:

   ```bash
   npx maritime-data-synthesizer initialize 30
   ```

   This creates the BigQuery table and loads 30 days of historical vessel positions.
   - Time: ~30-60 minutes for 30 days
   - Data: ~4.3M records (144K/day × 30 days)

2. **Start continuous generation**:

   ```bash
   npx maritime-data-synthesizer start
   ```

   This starts generating new vessel positions every minute.
   - Press Ctrl+C to stop

3. **Monitor in another terminal**:
   ```bash
   npx maritime-data-synthesizer stats
   ```

### Programmatic Usage

```javascript
const { MaritimeDataSynthesizer } = require('./src');

// Create synthesizer instance
const synthesizer = new MaritimeDataSynthesizer({
	totalVessels: 100000,
	batchSize: 100,
	generationIntervalMs: 60000,
	retentionDays: 30,
});

// Set up event listeners
synthesizer.on('batch:inserted', (data) => {
	console.log(`Inserted ${data.records} records`);
});

synthesizer.on('batch:error', (data) => {
	console.error('Error:', data.error);
});

// Initialize and start
async function run() {
	await synthesizer.initialize(30); // 30 days of historical data
	await synthesizer.start();
}

run();
```

## Architecture

### Components

1. **MaritimeVesselGenerator** (`src/generator.js`)
   - Generates synthetic vessel position data
   - Maintains vessel pool with persistent identifiers
   - Implements realistic movement patterns
   - Tracks ongoing journeys between ports

2. **MaritimeBigQueryClient** (`src/bigquery.js`)
   - Wraps Google Cloud BigQuery SDK
   - Handles schema creation and table management
   - Performs batch inserts via load jobs (free tier compatible)
   - Manages data retention and cleanup

3. **MaritimeDataSynthesizer** (`src/service.js`)
   - Orchestrates generation and insertion
   - Event-driven architecture with 10+ event types
   - Manages service lifecycle
   - Coordinates initialization, generation, and cleanup loops

### Data Generation Strategy

**Vessel Pool**:

- Pre-generates 10,000 vessels with persistent identifiers
- Each vessel has fixed attributes (MMSI, IMO, type, dimensions)
- Vessels are reused across batches for consistency

**Journey Simulation**:

- Each vessel maintains a journey state (origin → destination)
- 30% of vessels are in port at any time (anchored or moored)
- 70% are at sea, moving toward their destination
- When a vessel reaches its destination, it enters port and eventually starts a new journey

**Movement Calculation**:

- Uses Haversine formula for great circle distances
- Calculates bearing between current position and destination
- Moves vessel based on speed and course
- Accounts for different speeds by vessel type and status

**Geographic Distribution**:

- Major ports weighted by actual traffic volume
- Asia-Pacific: 50% (Singapore, Shanghai, Hong Kong, etc.)
- Europe: 20% (Rotterdam, Antwerp, Hamburg, etc.)
- Americas: 15% (Los Angeles, NY/NJ, Houston, etc.)
- Middle East: 10% (Dubai, Jeddah, etc.)
- Africa: 5% (Cape Town, Durban, Lagos, etc.)

## Performance & Scale

### Throughput

With default settings:

- **144,000 records/day** (100 records × 1,440 batches)
- **1.44M records/day** at 1,000 batch size
- **4.3M records** for 30 days of historical data

### BigQuery Costs (Free Tier)

The synthesizer is optimized for BigQuery free tier:

- **Storage**: 10 GB free (30 days ≈ 2-3 GB)
- **Queries**: 1 TB/month free (plenty for monitoring)
- **Load Jobs**: 1,500/table/day limit (default config uses ~1,440/day)

### Resource Usage

- **Memory**: ~150 MB baseline + 1 MB per 10K vessels
- **CPU**: <5% on modern hardware
- **Network**: Depends on BigQuery API calls (~1-2 KB per record)

## Example Queries

Once data is loaded, you can query it in BigQuery:

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

### Vessels in a Geographic Area

```sql
SELECT
  mmsi,
  vessel_name,
  vessel_type,
  latitude,
  longitude,
  speed_knots,
  status,
  destination
FROM `your-project.maritime_tracking.vessel_positions`
WHERE latitude BETWEEN 35.0 AND 45.0
  AND longitude BETWEEN -130.0 AND -115.0
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY timestamp DESC
```

### Port Activity (vessels at anchor)

```sql
SELECT
  destination as port,
  COUNT(*) as vessel_count,
  AVG(draft) as avg_draft
FROM `your-project.maritime_tracking.vessel_positions`
WHERE status IN ('AT_ANCHOR', 'MOORED')
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY port
ORDER BY vessel_count DESC
LIMIT 10
```

### Vessel Journey History

```sql
SELECT
  mmsi,
  vessel_name,
  vessel_type,
  timestamp,
  latitude,
  longitude,
  speed_knots,
  course,
  status,
  destination
FROM `your-project.maritime_tracking.vessel_positions`
WHERE mmsi = '201123456'
ORDER BY timestamp DESC
LIMIT 100
```

## Use Cases

### Maritime Analytics

- Track vessel movements and patterns
- Analyze port activity and congestion
- Study shipping routes and trade flows
- Monitor vessel speeds and efficiency

### Machine Learning

- Train models for vessel trajectory prediction
- Anomaly detection for unusual vessel behavior
- Port arrival time estimation
- Route optimization algorithms

### Visualization & Dashboards

- Real-time vessel tracking maps
- Port activity heatmaps
- Trade flow visualization
- Fleet management dashboards

### Testing & Development

- Test maritime tracking applications
- Develop AIS (Automatic Identification System) tools
- Validate geospatial queries and analytics
- Load testing for maritime data pipelines

## Event System

The synthesizer emits comprehensive events for monitoring:

### Service Events

- `service:starting` - Service initialization begun
- `service:started` - Service running
- `service:stopping` - Shutdown initiated
- `service:stopped` - Shutdown complete
- `service:error` - Fatal error occurred

### Initialization Events

- `init:starting` - Historical data load beginning
- `init:bigquery-ready` - Schema created
- `init:data-generation-starting` - Batch generation starting
- `init:progress` - Progress update with percentage
- `init:completed` - Historical data loaded
- `init:error` - Initialization failed

### Batch Events

- `batch:generating` - Record generation started
- `batch:generated` - Records ready for insert
- `batch:inserting` - Insert job submitted to BigQuery
- `batch:inserted` - Insert completed successfully
- `batch:error` - Insert failed

### Cleanup Events

- `cleanup:starting` - Retention cleanup started
- `cleanup:completed` - Old data deleted
- `cleanup:error` - Cleanup failed

## Troubleshooting

### "GCP_PROJECT_ID must be set"

- Ensure `.env` file exists with `GCP_PROJECT_ID=your-project-id`
- Or set environment variable: `export GCP_PROJECT_ID=your-project-id`

### "Load job completed with errors"

- Check BigQuery quota limits (1,500 load jobs per table per day)
- Verify table schema matches data format
- Review BigQuery logs in GCP Console

### High Memory Usage

- Reduce `TOTAL_VESSELS` (default 100,000)
- Decrease `BATCH_SIZE` to process smaller batches

### Slow Historical Data Loading

- Increase `BATCH_SIZE` to insert more records per job
- Reduce number of days to load
- Consider loading in stages

## Technical Details

### Coordinate System

- **Latitude**: -90° (South Pole) to +90° (North Pole)
- **Longitude**: -180° (Date Line West) to +180° (Date Line East)
- **Precision**: 6 decimal places (~0.1 meters)

### Navigation Calculations

- **Distance**: Haversine formula (great circle)
- **Bearing**: Forward azimuth calculation
- **New Position**: Given distance and bearing
- **Speed**: Nautical miles per hour (knots)

### BigQuery Optimization

- **Partitioning**: By timestamp (DAY)
- **Clustering**: By vessel_type, mmsi, report_date
- **Load Jobs**: NDJSON format via temp files
- **Write Disposition**: WRITE_APPEND (preserves existing data)

## License

Apache-2.0

## Contributing

This is part of the harper-bigquery-sync project. Contributions welcome!

## Support

For issues and questions, please file an issue on the project repository.
