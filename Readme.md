# BigQuery Sync Plugin for Harper

Distributed data ingestion from Google BigQuery to Harper using modulo-based partitioning.

**About Harper:** Harper is a distributed application platform that unifies database, cache, and application server. [Learn more](https://harperdb.io)

**Quick Start:** Deploy this component on [Harper Fabric](https://fabric.harper.fast) - no credit card required, free tier available.

## 📦 What's Included

This repository contains two components:

1. **BigQuery Plugin** - Syncs data FROM BigQuery INTO Harper
2. **Maritime Data Synthesizer** - Generates synthetic vessel data TO BigQuery for testing

See [System Overview](docs/SYSTEM-OVERVIEW.md) for how they work together, or jump to [Maritime Synthesizer Quick Start](docs/QUICKSTART.md) to start generating test data in 5 minutes.

## Plugin Features

- **Multi-Table Support**: Sync multiple BigQuery tables simultaneously with independent settings
- **Horizontal Scalability**: Linear throughput increase with cluster size
- **No Coordination**: Each node independently determines its workload
- **Failure Recovery**: Local checkpoints enable independent node recovery
- **Adaptive Polling**: Batch sizes adjust based on sync lag
- **Continuous Validation**: Automatic data completeness checks across all tables
- **Native Replication**: Leverages Harper's clustering for data distribution ([docs](https://docs.harperdb.io/docs/developers/replication))
- **Generic Storage**: Stores complete BigQuery records without schema constraints

## Maritime Synthesizer Features

- **Realistic Data**: 100,000+ vessels with physics-based movement patterns
- **Global Scale**: 29 major ports worldwide with weighted traffic distribution
- **6 Vessel Types**: Container ships, bulk carriers, tankers, cargo, passenger, fishing
- **Production-Ready**: 144K+ records/day with automatic retention management
- **Easy Testing**: Perfect for validating the BigQuery plugin with realistic workloads
- **Shared Config**: Uses the same `config.yaml` as the plugin - no duplicate setup

### Running the Synthesizer

The maritime synthesizer generates test data TO BigQuery, which the plugin then syncs FROM BigQuery to Harper.

**Prerequisites:**

1. GCP project with BigQuery enabled
2. Service account key with BigQuery write permissions
3. Update `config.yaml` with your BigQuery credentials

**Quick Start:**

```bash
# Install dependencies (if not already done)
npm install

# Generate test data - auto-detects mode from config.yaml
npx maritime-data-synthesizer initialize realistic
```

**Available Commands:**

- `initialize <scenario>` - Generate test data (scenarios: small, realistic, stress)
  - `small`: 100 positions, 10 events, 20 metadata (~1 hour of data)
  - `realistic`: 10k positions, 500 events, 100 metadata (~24 hours)
  - `stress`: 100k positions, 5k events, 1k metadata (~7 days)
- `start` - Continuous generation with rolling window (single-table mode only)
- `stats` - View BigQuery table statistics
- `clear` - Clear all data (keeps schema)
- `reset N` - Delete and reload with N days of data

**Note:** Multi-table mode (current default config) supports `initialize` command. For continuous generation with `start`, use single-table config format.

**Documentation:**

- **[5-Minute Quick Start](docs/QUICKSTART.md)** - Get generating data immediately
- **[System Overview](docs/SYSTEM-OVERVIEW.md)** - How plugin + synthesizer work together
- **[Full Guide](docs/maritime-data-synthesizer.md)** - Comprehensive synthesizer documentation
- **[Feature Overview](docs/MARITIME-SYNTHESIZER-README.md)** - Use cases and examples

## Architecture

Each node:

1. Discovers cluster topology via Harper's clustering API
2. Calculates its node ID from ordered peer list
3. Pulls only records where `hash(timestamp) % clusterSize == nodeId`
4. Writes to local Harper instance
5. Relies on Harper's native replication

## Installation

### Option 1: Deploy on Fabric (Recommended)

1. Sign up at [fabric.harper.fast](https://fabric.harper.fast)
2. Create a new application
3. Upload this component
4. Configure BigQuery credentials
5. Component auto-deploys across your cluster

### Option 2: Self-Hosted

1. Deploy Harper cluster (3+ nodes recommended) - [Quick start guide](https://docs.harperdb.io/docs/getting-started/quickstart)
2. Configure clustering between nodes - [Clustering docs](https://docs.harperdb.io/docs/developers/replication)
3. Copy this component to each node:

   ```bash
   harper deploy bigquery-sync /path/to/component
   ```

4. Configure `config.yaml` with BigQuery credentials:
   ```yaml
   bigquery:
     projectId: your-project
     dataset: your_dataset
     table: your_table
     credentials: /path/to/service-account.json
   ```

## Configuration

### Multi-Table Support

The plugin supports syncing **multiple BigQuery tables** simultaneously, each with independent sync settings:

```yaml
bigquery:
  projectId: your-project
  credentials: service-account-key.json
  location: US

  tables:
    - id: vessel_positions
      dataset: maritime_tracking
      table: vessel_positions
      timestampColumn: timestamp
      columns: [timestamp, mmsi, latitude, longitude, speed_knots]
      targetTable: VesselPositions
      sync:
        initialBatchSize: 10000
        catchupBatchSize: 1000
        steadyBatchSize: 500

    - id: port_events
      dataset: maritime_tracking
      table: port_events
      timestampColumn: event_time
      columns: ['*'] # Fetch all columns
      targetTable: PortEvents
      sync:
        initialBatchSize: 5000
        catchupBatchSize: 500
        steadyBatchSize: 100
```

**Key Features:**

- Each table syncs to a separate Harper table
- Independent batch sizes and sync rates per table
- Different timestamp column names supported
- Isolated checkpoints - one table failure doesn't affect others
- Per-table validation and monitoring
- Backward compatible with single-table configuration

**Important Constraint:**
Each BigQuery table MUST sync to a **different** Harper table. Multiple BigQuery tables syncing to the same Harper table is not supported and will cause:

- Record ID collisions and data overwrites
- Validation failures (can only validate one source)
- Checkpoint confusion (different sync states)
- Schema conflicts (mixed field sets)

If you need to combine data from multiple BigQuery tables, sync them to separate Harper tables and join at query time.

See `config.multi-table.yaml` for a complete example.

### Data Storage

BigQuery records are stored as-is at the top level:

```graphql
type BigQueryData @table {
	id: ID! @primaryKey
	# All BigQuery fields stored directly at top level
	_syncedAt: String @createdTime
}
```

Example stored record:

```json
{
	"id": "a1b2c3d4e5f6g7h8",
	"_syncedAt": "2025-11-04T16:00:00Z",
	"timestamp": "2025-11-04T15:59:00Z",
	"mmsi": "367123456",
	"imo": "IMO9876543",
	"vessel_name": "MARITIME VOYAGER",
	"vessel_type": "Container Ship",
	"latitude": 37.7749,
	"longitude": -122.4194,
	"speed_knots": 12.5,
	"heading": 275,
	"status": "Under way using engine"
}
```

This provides maximum flexibility - all BigQuery fields are directly queryable without nested paths.

### BigQuery Setup

Ensure service account has:

- `bigquery.jobs.create` permission
- `bigquery.tables.getData` permission on target table

[BigQuery IAM documentation](https://cloud.google.com/bigquery/docs/access-control)

### Harper Setup

For Harper installation and configuration, see the [Harper Getting Started Guide](https://docs.harperdb.io/docs/getting-started).

<!--
Additional considerations for production deployments:
- Fixed node IDs for clustering stability
- Peer discovery configuration for multi-node setups
- IOPS capacity planning for write throughput
-->

### Batch Size Tuning

Adjust based on:

- Record size
- Network bandwidth
- IOPS capacity
- Desired latency

## Querying Data

BigQuery fields are stored directly at the top level for easy querying:

```javascript
// Get all records
SELECT * FROM BigQueryData LIMIT 10;

// Query by vessel MMSI (direct field access)
SELECT * FROM BigQueryData
WHERE mmsi = '367123456';

// Filter by timestamp
SELECT * FROM BigQueryData
WHERE timestamp > '2025-11-01T00:00:00Z'
ORDER BY timestamp DESC;

// Select specific fields - find fast-moving vessels
SELECT id, timestamp, vessel_name, speed_knots, latitude, longitude
FROM BigQueryData
WHERE speed_knots > 20;

// Check sync status
SELECT id, _syncedAt, timestamp, vessel_name, mmsi
FROM BigQueryData
ORDER BY _syncedAt DESC
LIMIT 10;
```

## Monitoring

### Check Sync Status

```javascript
// Query checkpoint table
SELECT * FROM SyncCheckpoint ORDER BY nodeId;
```

### View Recent Audits

```javascript
// Check validation results
SELECT * FROM SyncAudit
WHERE timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;
```

### Monitor Lag

```javascript
// Calculate current lag
SELECT
  nodeId,
  lastTimestamp,
  (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(lastTimestamp)) as lag_seconds,
  phase
FROM SyncCheckpoint;
```

## API Endpoints

### Get Status

```bash
GET /SyncControl
```

Returns current sync status for the node.

### Control Sync

```bash
POST /SyncControl
{
  "action": "start" | "stop"
  # Note: "validate" action is not yet implemented
}
```

## Troubleshooting

### Node Not Ingesting

- Check BigQuery credentials
- Verify node can reach BigQuery API
- Check checkpoint table for errors

### Data Drift Detected

- Check for partition key collisions
- Verify all nodes are running
- Review checkpoint timestamps across nodes

### High Lag

- Increase batch sizes
- Add more nodes
- Check IOPS capacity

**Need help?** Visit [Harper documentation](https://docs.harperdb.io) or reach out to our team at [harperdb.io](https://harperdb.io)

## Performance Tuning

### IOPS Calculation

```
Indexes: 1 primary + 1 timestamp = 2 indexes
IOPS per record: ~4 IOPS
Target throughput: 5000 records/sec per node
Required IOPS: 20,000 per node
```

Learn more about [Harper's storage architecture](https://docs.harperdb.io/docs/reference/storage-algorithm)

### Scaling Guidelines

- 3 nodes: ~15K records/sec total
- 6 nodes: ~30K records/sec total
- 12 nodes: ~60K records/sec total

**Note:** Harper doesn't autoscale. Nodes must be added/removed manually. Fabric makes this easier through its UI, but changing cluster size requires rebalancing consideration (see Limitations below).

## Limitations

- Cluster size should remain stable (node additions require rebalancing)
- BigQuery costs increase with query frequency
- Modulo partitioning requires hashable timestamp

## Roadmap

### Crawl (v1.0 - Complete)

**Status:** ✅ Shipped

Single-threaded, single-table ingestion:

- ✅ Modulo-based partitioning for distributed workload
- ✅ Adaptive batch sizing (phase-based: initial/catchup/steady)
- ✅ Checkpoint-based recovery per node
- ✅ Durable thread identity (survives restarts)
- ✅ Monitoring via REST API (`/SyncControl`)

### 🚶 Walk (v2.0 - Complete)

**Status:** ✅ Shipped

Multi-table ingestion with column selection:

- ✅ **Multiple BigQuery tables** - Ingest from multiple tables simultaneously
- ✅ **Column selection** - Choose specific columns per table (reduce data transfer)
- ✅ **Per-table configuration** - Different batch sizes, intervals, and strategies per table
- ✅ **Multi-table validation** - Independent validation per table
- ✅ **Unified monitoring** - Single dashboard for all table ingestions

**Use Cases:**

- Ingest multiple related datasets (e.g., vessels, ports, weather)
- Reduce costs by selecting only needed columns
- Different sync strategies per data type (real-time vs batch)

**Configuration Example:**

```yaml
bigquery:
  projectId: your-project
  credentials: service-account-key.json
  location: US

  tables:
    - id: vessel_positions
      dataset: maritime_tracking
      table: vessel_positions
      columns: [mmsi, timestamp, latitude, longitude, speed_knots]
      targetTable: VesselPositions
      sync:
        initialBatchSize: 10000
        catchupBatchSize: 1000
        steadyBatchSize: 500

    - id: port_events
      dataset: maritime_tracking
      table: port_events
      columns: [port_id, vessel_mmsi, event_type, timestamp]
      targetTable: PortEvents
      sync:
        initialBatchSize: 5000
        catchupBatchSize: 500
        steadyBatchSize: 100
```

### 🏃 Run (v3.0 - Planned)

**Status:** 📋 Future

Multi-threaded, multi-instance Harper cluster support:

- [ ] **Multi-threaded ingestion** - Multiple worker threads per node
- [ ] **Full cluster distribution** - Automatic workload distribution across all Harper nodes
- [ ] **Dynamic rebalancing** - Handle node additions/removals without manual intervention
- [ ] **Improved monitoring** - Cluster-wide health dashboard
- [ ] **Thread-level checkpointing** - Fine-grained recovery per worker thread

**Benefits:**

- Linear scaling across cluster nodes
- Better resource utilization per node
- Automatic failover and rebalancing

**Note:** The code already supports multiple worker threads per instance via `server.workerIndex`. Each thread gets a durable identity (`hostname-workerIndex`) that persists across restarts, enabling checkpoint-based recovery.

---

**Get Started:** Deploy on [Harper Fabric](https://fabric.harper.fast) - free tier available, no credit card required.

**Learn More:** [Harper Documentation](https://docs.harperdb.io) | [harperdb.io](https://harperdb.io)
