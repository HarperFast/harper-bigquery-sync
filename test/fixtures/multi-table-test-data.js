/**
 * Test Data Fixtures for Multi-Table Testing
 * Defines test scenarios with different data volumes
 */

export const TEST_SCENARIOS = {
	small: {
		description: 'Small dataset for quick tests',
		vessel_positions: 100,
		port_events: 10,
		vessel_metadata: 20,
		duration: '1 hour',
		durationMs: 60 * 60 * 1000,
	},
	realistic: {
		description: 'Realistic 24-hour dataset',
		vessel_positions: 10000,
		port_events: 500,
		vessel_metadata: 100,
		duration: '24 hours',
		durationMs: 24 * 60 * 60 * 1000,
	},
	stress: {
		description: 'Large dataset for stress testing',
		vessel_positions: 100000,
		port_events: 5000,
		vessel_metadata: 1000,
		duration: '7 days',
		durationMs: 7 * 24 * 60 * 60 * 1000,
	},
};

/**
 * Sample multi-table configuration for testing
 */
export const MULTI_TABLE_CONFIG = {
	bigquery: {
		projectId: 'test-project',
		credentials: '/path/to/service-account-key.json',
		location: 'US',

		tables: [
			{
				id: 'vessel_positions',
				dataset: 'maritime_tracking',
				table: 'vessel_positions',
				timestampColumn: 'timestamp',
				columns: ['timestamp', 'mmsi', 'latitude', 'longitude', 'speed_knots', 'heading'],
				targetTable: 'VesselPositions',
				sync: {
					initialBatchSize: 10000,
					catchupBatchSize: 1000,
					steadyBatchSize: 500,
				},
			},
			{
				id: 'port_events',
				dataset: 'maritime_tracking',
				table: 'port_events',
				timestampColumn: 'event_time',
				columns: ['event_time', 'port_id', 'vessel_mmsi', 'event_type', 'status'],
				targetTable: 'PortEvents',
				sync: {
					initialBatchSize: 5000,
					catchupBatchSize: 500,
					steadyBatchSize: 100,
				},
			},
			{
				id: 'vessel_metadata',
				dataset: 'maritime_tracking',
				table: 'vessel_metadata',
				timestampColumn: 'last_updated',
				columns: ['*'],
				targetTable: 'VesselMetadata',
				sync: {
					initialBatchSize: 1000,
					catchupBatchSize: 100,
					steadyBatchSize: 10,
				},
			},
		],
	},

	sync: {
		pollInterval: 30000,
		catchupThreshold: 3600,
		steadyThreshold: 300,
	},
};

/**
 * Legacy single-table configuration for backward compatibility testing
 */
export const LEGACY_SINGLE_TABLE_CONFIG = {
	bigquery: {
		projectId: 'test-project',
		dataset: 'maritime_tracking',
		table: 'vessel_positions',
		timestampColumn: 'timestamp',
		columns: ['timestamp', 'mmsi', 'latitude', 'longitude'],
		credentials: '/path/to/service-account-key.json',
		location: 'US',
	},

	sync: {
		initialBatchSize: 10000,
		catchupBatchSize: 1000,
		steadyBatchSize: 500,
		pollInterval: 30000,
	},
};

/**
 * Sample vessel data for testing relationships
 */
export const SAMPLE_VESSELS = [
	{
		mmsi: '367123456',
		imo: 'IMO9876543',
		vessel_name: 'PACIFIC TRADER',
		vessel_type: 'Container Ship',
		flag: 'US',
		callsign: 'WDD1234',
		length: 300,
		beam: 40,
		draft: 12,
	},
	{
		mmsi: '367789012',
		imo: 'IMO9876544',
		vessel_name: 'OCEAN VOYAGER',
		vessel_type: 'Bulk Carrier',
		flag: 'LR',
		callsign: 'D5AB2',
		length: 225,
		beam: 32,
		draft: 10,
	},
	{
		mmsi: '367345678',
		imo: 'IMO9876545',
		vessel_name: 'SEA SPIRIT',
		vessel_type: 'Tanker',
		flag: 'PA',
		callsign: 'H3RC4',
		length: 250,
		beam: 44,
		draft: 15,
	},
];

/**
 * Sample ports for testing port events
 */
export const SAMPLE_PORTS = [
	{ port_id: 'SFO', name: 'San Francisco', lat: 37.7749, lon: -122.4194 },
	{ port_id: 'LAX', name: 'Los Angeles', lat: 33.7405, lon: -118.272 },
	{ port_id: 'SEA', name: 'Seattle', lat: 47.6062, lon: -122.3321 },
	{ port_id: 'SIN', name: 'Singapore', lat: 1.2644, lon: 103.8223 },
	{ port_id: 'SHA', name: 'Shanghai', lat: 31.2304, lon: 121.4737 },
];

/**
 * Port event types
 */
export const EVENT_TYPES = ['ARRIVAL', 'DEPARTURE', 'BERTHED', 'ANCHORED', 'UNDERWAY'];

/**
 * Vessel statuses
 */
export const VESSEL_STATUSES = [
	'Under way using engine',
	'At anchor',
	'Not under command',
	'Restricted manoeuverability',
	'Constrained by her draught',
	'Moored',
	'Aground',
];

export default {
	TEST_SCENARIOS,
	MULTI_TABLE_CONFIG,
	LEGACY_SINGLE_TABLE_CONFIG,
	SAMPLE_VESSELS,
	SAMPLE_PORTS,
	EVENT_TYPES,
	VESSEL_STATUSES,
};
