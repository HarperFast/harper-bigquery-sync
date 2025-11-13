# Project TODO List

Consolidated task list for Harper BigQuery Sync plugin and Maritime Data Synthesizer.

## High Priority

### Production Readiness

- [x] **Add exponential backoff for transient BigQuery errors** [#3](https://github.com/HarperFast/harper-bigquery-sync/issues/3)
  - Currently errors are retried with simple logic
  - Need exponential backoff strategy for transient failures
  - Prevents overwhelming BigQuery API during issues

- [ ] **Production deployment documentation** [#4](https://github.com/HarperFast/harper-bigquery-sync/issues/4)
  - Fabric deployment guide with one-click setup
  - Self-hosted installation for on-premise clusters
  - Monitoring dashboards (Grafana/CloudWatch templates)
  - Operational runbooks for common scenarios

### Code Quality

- [x] **Fix memory leak in journey tracking** [#5](https://github.com/HarperFast/harper-bigquery-sync/issues/5)
  - Memory leak in `src/generator.js` journey tracking system
  - Blocks re-enabling certain tests
  - Related to vessel position generation

## Medium Priority

### Feature Enhancements

- [ ] **Multi-table rolling window support** [#6](https://github.com/HarperFast/harper-bigquery-sync/issues/6)
  - Currently multi-table orchestrator only supports `initialize` command
  - Add `start` command for continuous generation with rolling window
  - Add `backfill` capability
  - Add `cleanup`/retention management
  - Reference: single-table MaritimeDataSynthesizer has working implementation

- [ ] **Dynamic Harper table creation via Operations API** [#7](https://github.com/HarperFast/harper-bigquery-sync/issues/7)
  - Currently requires manual schema.graphql definition
  - Could dynamically create tables based on BigQuery schema at runtime
  - Enables automatic table creation from BigQuery metadata
  - Supports schema evolution without manual intervention
  - Reference: https://docs.harperdb.io/docs/developers/operations-api

- [ ] **Streaming insert API option for production** [#8](https://github.com/HarperFast/harper-bigquery-sync/issues/8)
  - Current implementation uses load job API (free tier compatible)
  - Add opt-in streaming insert for production deployments with benefits:
    - Lower latency for real-time use cases
    - Different cost model (may be preferred at scale)
  - Make it configurable per table

## Future (v3.0 Roadmap)

### Multi-Threaded Ingestion

- [ ] **Multi-threaded ingestion per node** [#9](https://github.com/HarperFast/harper-bigquery-sync/issues/9)
  - Better CPU utilization on multi-core nodes
  - Code already supports durable thread identity via `hostname-workerIndex`
  - Thread-level checkpointing for fine-grained recovery
  - Automatic thread scaling based on lag

### Dynamic Rebalancing

- [ ] **Dynamic rebalancing for autoscaling** [#10](https://github.com/HarperFast/harper-bigquery-sync/issues/10)
  - Detect topology changes → pause → recalculate → resume
  - Graceful node additions/removals without manual intervention
  - Zero-downtime scaling capabilities
  - Currently requires stable cluster topology

### Monitoring & Observability

- [ ] **Enhanced monitoring and observability** [#11](https://github.com/HarperFast/harper-bigquery-sync/issues/11)
  - Cluster-wide health dashboard
  - Per-table metrics and thread-level statistics
  - Lag histograms
  - Pre-built Grafana dashboards
  - CloudWatch/DataDog integrations
  - Alert configurations

## Nice to Have

### Testing & Quality

- [ ] **Comprehensive unit tests**
  - Core sync engine logic
  - Type conversion edge cases
  - Error handling paths

- [ ] **Integration test suite**
  - End-to-end sync validation
  - Multi-table scenarios
  - Failure recovery testing

- [ ] **Performance benchmarks**
  - Throughput measurements
  - Latency profiles
  - Resource usage baselines

### Documentation

- [ ] **Video tutorials**
  - Setup walkthrough
  - Configuration examples
  - Troubleshooting guide

- [ ] **Architecture diagrams**
  - System overview visuals
  - Data flow diagrams
  - Deployment topologies

- [ ] **More examples**
  - Additional use cases
  - Configuration patterns
  - Integration examples

### Developer Experience

- [ ] **Better CLI output**
  - Colorized status messages
  - Progress indicators
  - Formatted table output

- [ ] **Debug mode**
  - Verbose logging option
  - Request/response inspection
  - Performance profiling

- [ ] **Configuration validation**
  - Pre-flight config checks
  - Helpful error messages
  - Suggested fixes

## Completed ✅

- ✅ Multi-table support with column selection (v2.0)
- ✅ Maritime data synthesizer with realistic test data
- ✅ Modulo-based partitioning for distributed workload
- ✅ Adaptive batch sizing (phase-based)
- ✅ Checkpoint-based recovery per node
- ✅ Per-table validation and monitoring
- ✅ Backward compatibility with single-table format
- ✅ CI/CD with lint, test, format checks
- ✅ Project history documentation
- ✅ Reorganized codebase (src/ vs tools/)

---

**Last Updated:** 2025-11-13
