# Examples

This directory contains example and demonstration scripts for the maritime vessel data synthesizer.

## Demo Scripts

### test-config.js
Demonstrates loading and parsing configuration from `config.yaml`. Shows how the synthesizer and plugin share configuration.

```bash
node examples/test-config.js
```

### test-generator.js
Demonstrates the vessel data generator creating realistic vessel positions.

```bash
node examples/test-generator.js
```

### test-rolling-window.js
Demonstrates the rolling window feature with different scenarios (empty table, partial data, sufficient data).

```bash
node examples/test-rolling-window.js
```

### test-clear-commands.js
Compares the `clear`, `clean`, and `reset` commands with use cases and workflows.

```bash
node examples/test-clear-commands.js
```

### test-bigquery-config.js
Tests BigQuery configuration and connection (requires valid credentials).

```bash
node examples/test-bigquery-config.js
```

## Running Examples

All examples can be run with Node.js 20+:

```bash
node examples/<script-name>.js
```

Some examples require configuration in `config.yaml` or `.env` file.
