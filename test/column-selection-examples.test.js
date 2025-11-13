/**
 * Column Selection Examples and Tests
 *
 * Demonstrates how to use column selection to reduce data transfer costs
 * and improve query performance when syncing from BigQuery.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { QueryBuilder, formatColumnList } from '../src/query-builder.js';
import { validateAndNormalizeColumns } from '../src/validators.js';

describe('Column Selection Examples', () => {
	describe('Basic Column Selection', () => {
		it('Example 1: Select all columns (default behavior)', () => {
			// When you don't specify columns, get everything
			const builder = new QueryBuilder({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: ['*'], // or omit entirely
			});

			const query = builder.buildPullPartitionQuery();

			// Verify wildcard is used
			assert.ok(query.includes('SELECT *'));
			assert.ok(!query.includes('SELECT timestamp'));

			console.log('✓ Example 1: Wildcard selection includes all columns');
		});

		it('Example 2: Select only essential columns for cost savings', () => {
			// Select only the columns you need - reduces BigQuery scanning costs
			const essentialColumns = [
				'timestamp', // Required: for ordering and time filtering
				'mmsi', // Vessel identifier
				'latitude', // Position
				'longitude', // Position
			];

			const builder = new QueryBuilder({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: essentialColumns,
			});

			const query = builder.buildPullPartitionQuery();

			// Verify specific columns are selected
			assert.ok(query.includes('SELECT timestamp, mmsi, latitude, longitude'));
			assert.ok(!query.includes('SELECT *'));

			console.log('✓ Example 2: Specific column selection (4 columns)');
			console.log(`  Columns: ${essentialColumns.join(', ')}`);
		});

		it('Example 3: Select subset with additional metadata', () => {
			// Include only the fields needed for your specific use case
			const columns = ['timestamp', 'mmsi', 'vessel_name', 'vessel_type', 'speed_knots', 'heading', 'status'];

			const builder = new QueryBuilder({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: columns,
			});

			const query = builder.buildPullPartitionQuery();

			// Verify all requested columns are in the query
			for (const column of columns) {
				assert.ok(query.includes(column), `Query should include column: ${column}`);
			}

			console.log('✓ Example 3: Metadata-focused selection (7 columns)');
			console.log(`  Use case: Vessel tracking dashboard`);
		});
	});

	describe('Configuration Validation', () => {
		it('Example 4: Configuration must include timestamp column', () => {
			const timestampColumn = 'timestamp';

			// This will throw because timestamp column is missing
			assert.throws(
				() => {
					validateAndNormalizeColumns(
						['mmsi', 'latitude', 'longitude'], // Missing 'timestamp'
						timestampColumn
					);
				},
				/Timestamp column 'timestamp' must be included/,
				'Should require timestamp column'
			);

			// This works - includes timestamp
			const validColumns = validateAndNormalizeColumns(['timestamp', 'mmsi', 'latitude', 'longitude'], timestampColumn);

			assert.deepStrictEqual(validColumns, ['timestamp', 'mmsi', 'latitude', 'longitude']);

			console.log('✓ Example 4: Timestamp column validation');
		});

		it('Example 5: Wildcard bypasses timestamp validation', () => {
			// Wildcard automatically includes all columns (including timestamp)
			const wildcardColumns = validateAndNormalizeColumns('*', 'timestamp');

			assert.deepStrictEqual(wildcardColumns, ['*']);

			console.log('✓ Example 5: Wildcard includes timestamp implicitly');
		});

		it('Example 6: Undefined columns defaults to wildcard', () => {
			// Not specifying columns is the same as using wildcard
			const defaultColumns = validateAndNormalizeColumns(undefined, 'timestamp');

			assert.deepStrictEqual(defaultColumns, ['*']);

			console.log('✓ Example 6: Undefined columns → wildcard');
		});
	});

	describe('Query Generation with Column Selection', () => {
		it('Example 7: Full query with minimal columns', () => {
			const builder = new QueryBuilder({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: ['timestamp', 'mmsi'],
			});

			const query = builder.buildPullPartitionQuery();

			// Verify query structure
			assert.ok(query.includes('SELECT timestamp, mmsi'));
			assert.ok(query.includes('FROM `maritime_tracking.vessel_positions`'));
			assert.ok(query.includes('WHERE'));
			assert.ok(query.includes('MOD(UNIX_MICROS(timestamp)'));
			assert.ok(query.includes('timestamp > TIMESTAMP(@lastTimestamp)'));
			assert.ok(query.includes('ORDER BY timestamp ASC'));
			assert.ok(query.includes('LIMIT'));

			console.log('✓ Example 7: Full query structure preserved with column selection');
		});

		it('Example 8: Compare query sizes - wildcard vs specific', () => {
			// Wildcard query
			const wildcardBuilder = new QueryBuilder({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: ['*'],
			});
			const wildcardQuery = wildcardBuilder.buildPullPartitionQuery();

			// Specific columns query
			const specificBuilder = new QueryBuilder({
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: ['timestamp', 'mmsi', 'latitude', 'longitude'],
			});
			const specificQuery = specificBuilder.buildPullPartitionQuery();

			// Both queries should have same structure, just different SELECT
			const wildcardLines = wildcardQuery.split('\n').length;
			const specificLines = specificQuery.split('\n').length;

			assert.strictEqual(wildcardLines, specificLines, 'Query structure should be identical');

			console.log('✓ Example 8: Query size comparison');
			console.log(`  Wildcard query: ${wildcardQuery.length} characters`);
			console.log(`  Specific query: ${specificQuery.length} characters`);
		});
	});

	describe('Real-World Use Cases', () => {
		it('Example 9: Location tracking only (minimal data)', () => {
			// Use case: Simple vessel position tracking
			// Reduces costs by excluding vessel_name, status, destination, etc.
			const locationColumns = ['timestamp', 'mmsi', 'latitude', 'longitude'];

			const columnList = formatColumnList(locationColumns);
			assert.strictEqual(columnList, 'timestamp, mmsi, latitude, longitude');

			console.log('✓ Example 9: Location-only tracking (minimal bandwidth)');
			console.log(`  Columns: ${locationColumns.join(', ')}`);
			console.log(`  Use case: Real-time position display`);
		});

		it('Example 10: Full vessel data (everything)', () => {
			// Use case: Comprehensive vessel monitoring
			// Keep all columns for complete analysis
			const allColumns = ['*'];

			const columnList = formatColumnList(allColumns);
			assert.strictEqual(columnList, '*');

			console.log('✓ Example 10: Full data sync');
			console.log(`  Use case: Data warehouse / analytics`);
		});

		it('Example 11: Movement analysis (velocity & direction)', () => {
			// Use case: Analyzing vessel movement patterns
			const movementColumns = ['timestamp', 'mmsi', 'latitude', 'longitude', 'speed_knots', 'heading', 'course'];

			const columnList = formatColumnList(movementColumns);
			assert.ok(columnList.includes('speed_knots'));
			assert.ok(columnList.includes('heading'));

			console.log('✓ Example 11: Movement analysis');
			console.log(`  Columns: ${movementColumns.join(', ')}`);
			console.log(`  Use case: Traffic pattern analysis`);
		});

		it('Example 12: Identity verification (vessel details only)', () => {
			// Use case: Vessel registry / identification
			const identityColumns = ['timestamp', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'flag', 'callsign'];

			const columnList = formatColumnList(identityColumns);
			assert.ok(columnList.includes('vessel_name'));
			assert.ok(columnList.includes('vessel_type'));

			console.log('✓ Example 12: Identity verification');
			console.log(`  Columns: ${identityColumns.join(', ')}`);
			console.log(`  Use case: Vessel registry database`);
		});
	});

	describe('Cost Optimization Examples', () => {
		it('Example 13: Calculate potential cost savings', () => {
			// Assume a table with 20 columns, average 100 bytes per column
			const totalColumns = 20;
			const avgBytesPerColumn = 100;
			const totalRecords = 1000000; // 1 million records

			// Scenario 1: Wildcard (all columns)
			const wildcardBytes = totalColumns * avgBytesPerColumn * totalRecords;
			const wildcardGB = wildcardBytes / (1024 * 1024 * 1024);

			// Scenario 2: Select 4 columns
			const selectedColumns = 4;
			const selectedBytes = selectedColumns * avgBytesPerColumn * totalRecords;
			const selectedGB = selectedBytes / (1024 * 1024 * 1024);

			const savings = wildcardGB - selectedGB;
			const savingsPercent = (savings / wildcardGB) * 100;

			console.log('✓ Example 13: Cost savings calculation');
			console.log(`  Scenario: 1M records, 20 columns, 100 bytes/column`);
			console.log(`  Wildcard (*): ${wildcardGB.toFixed(2)} GB`);
			console.log(`  Selected (4 cols): ${selectedGB.toFixed(2)} GB`);
			console.log(`  Savings: ${savings.toFixed(2)} GB (${savingsPercent.toFixed(0)}%)`);
			console.log(`  BigQuery pricing: ~$6.25/TB scanned (as of 2024)`);

			// Verify the math
			assert.ok(selectedGB < wildcardGB);
			assert.ok(savingsPercent > 70); // Should save >70%
		});

		it('Example 14: Network transfer savings', () => {
			// Less data = faster sync, less network cost
			const recordsPerBatch = 1000;
			const bytesPerColumnAvg = 50;

			// Full table (20 columns)
			const fullBatchBytes = recordsPerBatch * 20 * bytesPerColumnAvg;
			const fullBatchMB = fullBatchBytes / (1024 * 1024);

			// Selected columns (5 columns)
			const selectedBatchBytes = recordsPerBatch * 5 * bytesPerColumnAvg;
			const selectedBatchMB = selectedBatchBytes / (1024 * 1024);

			console.log('✓ Example 14: Network transfer savings per batch');
			console.log(`  Full data: ${fullBatchMB.toFixed(2)} MB/batch`);
			console.log(`  Selected: ${selectedBatchMB.toFixed(2)} MB/batch`);
			console.log(`  Transfer savings: ${((1 - selectedBatchMB / fullBatchMB) * 100).toFixed(0)}%`);

			assert.ok(selectedBatchMB < fullBatchMB);
		});
	});
});

