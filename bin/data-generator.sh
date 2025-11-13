#!/usr/bin/env bash

# Maritime Data Generator - Wrapper script
# Invokes the maritime data synthesizer CLI tool

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Execute the CLI from tools directory
exec node "${SCRIPT_DIR}/../tools/maritime-data-synthesizer/cli.js" "$@"
