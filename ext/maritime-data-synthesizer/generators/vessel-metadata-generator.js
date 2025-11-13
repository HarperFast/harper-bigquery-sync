/**
 * Vessel Metadata Data Generator
 *
 * Generates realistic vessel metadata including vessel details,
 * specifications, and registration information.
 * This data changes infrequently compared to position or event data.
 */

import { SAMPLE_VESSELS, VESSEL_STATUSES } from '../../../test/fixtures/multi-table-test-data.js';

// Additional data for realistic vessel generation
const VESSEL_TYPES = [
	'Container Ship',
	'Bulk Carrier',
	'Tanker',
	'Cargo Ship',
	'Passenger Ship',
	'Fishing Vessel',
	'Tug',
	'Naval Vessel',
	'Yacht',
	'Other',
];

const FLAGS = [
	'US',
	'PA',
	'LR',
	'MH',
	'BS',
	'CY',
	'MT',
	'GR',
	'SG',
	'HK',
	'CN',
	'JP',
	'KR',
	'GB',
	'NO',
	'DK',
	'NL',
	'DE',
	'IT',
	'FR',
];

const VESSEL_NAME_PREFIXES = [
	'PACIFIC',
	'OCEAN',
	'SEA',
	'ATLANTIC',
	'MARINE',
	'GLOBAL',
	'STAR',
	'CROWN',
	'ROYAL',
	'GOLDEN',
	'SILVER',
	'DIAMOND',
];

const VESSEL_NAME_SUFFIXES = [
	'TRADER',
	'VOYAGER',
	'SPIRIT',
	'PIONEER',
	'EXPLORER',
	'NAVIGATOR',
	'GUARDIAN',
	'PRINCE',
	'QUEEN',
	'KING',
	'FORTUNE',
	'GLORY',
];

export class VesselMetadataGenerator {
	/**
	 * Creates a new VesselMetadataGenerator
	 * @param {Object} options - Configuration options
	 * @param {Date} options.startTime - Start timestamp for last_updated field
	 * @param {number} options.durationMs - Duration in milliseconds
	 * @param {Array<string>} options.mmsiList - List of MMSI identifiers to generate metadata for
	 */
	constructor({ startTime, durationMs, mmsiList = [] }) {
		this.startTime = new Date(startTime);
		this.durationMs = durationMs;
		this.endTime = new Date(this.startTime.getTime() + durationMs);

		// Use provided MMSI list or generate from sample vessels
		this.mmsiList = mmsiList.length > 0 ? mmsiList : SAMPLE_VESSELS.map((v) => v.mmsi);

		// Track generated vessels to ensure consistency
		this.generatedVessels = new Map();
	}

	/**
	 * Generates a batch of vessel metadata records
	 * @param {number} count - Number of records to generate
	 * @returns {Array<Object>} Array of vessel metadata records
	 */
	generate(count) {
		const vessels = [];

		// If we have sample vessels and count matches, use them
		if (count === SAMPLE_VESSELS.length) {
			for (const sampleVessel of SAMPLE_VESSELS) {
				vessels.push(this.enrichVesselMetadata(sampleVessel));
			}
			return vessels;
		}

		// Generate new vessels
		for (let i = 0; i < count; i++) {
			const mmsi = this.mmsiList[i % this.mmsiList.length];

			// Check if we already generated this vessel
			if (this.generatedVessels.has(mmsi)) {
				vessels.push(this.generatedVessels.get(mmsi));
				continue;
			}

			const vessel = this.generateVesselMetadata(mmsi);
			this.generatedVessels.set(mmsi, vessel);
			vessels.push(vessel);
		}

		return vessels;
	}

	/**
	 * Generates metadata for a single vessel
	 * @param {string} mmsi - MMSI identifier
	 * @returns {Object} Vessel metadata record
	 * @private
	 */
	generateVesselMetadata(mmsi) {
		// Generate random vessel name
		const prefix = VESSEL_NAME_PREFIXES[Math.floor(Math.random() * VESSEL_NAME_PREFIXES.length)];
		const suffix = VESSEL_NAME_SUFFIXES[Math.floor(Math.random() * VESSEL_NAME_SUFFIXES.length)];
		const vesselName = `${prefix} ${suffix}`;

		// Generate IMO number (7 digits with check digit)
		const imoBase = 9000000 + Math.floor(Math.random() * 999999);
		const imo = `IMO${imoBase}`;

		// Select vessel type
		const vesselType = VESSEL_TYPES[Math.floor(Math.random() * VESSEL_TYPES.length)];

		// Select flag
		const flag = FLAGS[Math.floor(Math.random() * FLAGS.length)];

		// Generate callsign (4-6 alphanumeric characters)
		const callsign = this.generateCallsign();

		// Generate dimensions based on vessel type
		const dimensions = this.generateDimensions(vesselType);

		// Random timestamp within the time range
		const lastUpdated = new Date(this.startTime.getTime() + Math.random() * this.durationMs).toISOString();

		return {
			last_updated: lastUpdated,
			mmsi: mmsi,
			imo: imo,
			vessel_name: vesselName,
			vessel_type: vesselType,
			flag: flag,
			callsign: callsign,
			length: dimensions.length,
			beam: dimensions.beam,
			draft: dimensions.draft,
			gross_tonnage: dimensions.grossTonnage,
			deadweight: dimensions.deadweight,
			year_built: this.generateYearBuilt(),
			home_port: this.generateHomePort(flag),
			owner: this.generateOwner(),
			status: VESSEL_STATUSES[Math.floor(Math.random() * VESSEL_STATUSES.length)],
		};
	}

	/**
	 * Enriches a sample vessel with additional metadata and timestamps
	 * @param {Object} sampleVessel - Sample vessel from fixtures
	 * @returns {Object} Enriched vessel metadata
	 * @private
	 */
	enrichVesselMetadata(sampleVessel) {
		const lastUpdated = new Date(this.startTime.getTime() + Math.random() * this.durationMs).toISOString();

		return {
			last_updated: lastUpdated,
			mmsi: sampleVessel.mmsi,
			imo: sampleVessel.imo,
			vessel_name: sampleVessel.vessel_name,
			vessel_type: sampleVessel.vessel_type,
			flag: sampleVessel.flag,
			callsign: sampleVessel.callsign,
			length: sampleVessel.length,
			beam: sampleVessel.beam,
			draft: sampleVessel.draft,
			gross_tonnage: this.calculateGrossTonnage(sampleVessel.length, sampleVessel.beam),
			deadweight: this.calculateDeadweight(sampleVessel.vessel_type, sampleVessel.length),
			year_built: this.generateYearBuilt(),
			home_port: this.generateHomePort(sampleVessel.flag),
			owner: this.generateOwner(),
			status: VESSEL_STATUSES[0], // Default to first status
		};
	}

	/**
	 * Generates realistic vessel dimensions based on type
	 * @param {string} vesselType - Type of vessel
	 * @returns {Object} Dimensions object
	 * @private
	 */
	generateDimensions(vesselType) {
		const dimensionRanges = {
			'Container Ship': { length: [200, 400], beam: [30, 60], draft: [10, 16] },
			'Bulk Carrier': { length: [150, 300], beam: [25, 50], draft: [8, 15] },
			'Tanker': { length: [180, 350], beam: [30, 70], draft: [10, 20] },
			'Cargo Ship': { length: [100, 250], beam: [20, 40], draft: [6, 12] },
			'Passenger Ship': { length: [150, 350], beam: [25, 50], draft: [6, 10] },
			'Fishing Vessel': { length: [20, 80], beam: [6, 15], draft: [3, 6] },
			'Tug': { length: [20, 40], beam: [8, 15], draft: [3, 5] },
			'Naval Vessel': { length: [100, 300], beam: [15, 40], draft: [5, 10] },
			'Yacht': { length: [20, 100], beam: [5, 20], draft: [2, 6] },
			'Other': { length: [30, 150], beam: [8, 25], draft: [3, 8] },
		};

		const ranges = dimensionRanges[vesselType] || dimensionRanges['Other'];

		const length = Math.floor(ranges.length[0] + Math.random() * (ranges.length[1] - ranges.length[0]));

		const beam = Math.floor(ranges.beam[0] + Math.random() * (ranges.beam[1] - ranges.beam[0]));

		const draft = Math.floor(ranges.draft[0] + Math.random() * (ranges.draft[1] - ranges.draft[0]));

		const grossTonnage = this.calculateGrossTonnage(length, beam);
		const deadweight = this.calculateDeadweight(vesselType, length);

		return { length, beam, draft, grossTonnage, deadweight };
	}

	/**
	 * Calculates gross tonnage based on dimensions
	 * @param {number} length - Vessel length
	 * @param {number} beam - Vessel beam
	 * @returns {number} Gross tonnage
	 * @private
	 */
	calculateGrossTonnage(length, beam) {
		// Simplified formula: GT ≈ 0.2 * length * beam
		return Math.floor(0.2 * length * beam * 10);
	}

	/**
	 * Calculates deadweight based on vessel type and length
	 * @param {string} vesselType - Type of vessel
	 * @param {number} length - Vessel length
	 * @returns {number} Deadweight tonnage
	 * @private
	 */
	calculateDeadweight(vesselType, length) {
		const multipliers = {
			'Container Ship': 1.5,
			'Bulk Carrier': 2.0,
			'Tanker': 2.5,
			'Cargo Ship': 1.2,
			'Passenger Ship': 0.5,
			'Fishing Vessel': 0.3,
			'Tug': 0.2,
			'Naval Vessel': 0.8,
			'Yacht': 0.1,
			'Other': 1.0,
		};

		const multiplier = multipliers[vesselType] || 1.0;
		return Math.floor(length * multiplier * 10);
	}

	/**
	 * Generates a random callsign
	 * @returns {string} Callsign
	 * @private
	 */
	generateCallsign() {
		const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
		const length = 4 + Math.floor(Math.random() * 3); // 4-6 characters

		let callsign = '';
		for (let i = 0; i < length; i++) {
			callsign += chars[Math.floor(Math.random() * chars.length)];
		}

		return callsign;
	}

	/**
	 * Generates a random year built (between 1980 and current year)
	 * @returns {number} Year built
	 * @private
	 */
	generateYearBuilt() {
		const currentYear = new Date().getFullYear();
		const minYear = 1980;
		return minYear + Math.floor(Math.random() * (currentYear - minYear + 1));
	}

	/**
	 * Generates a home port based on flag
	 * @param {string} flag - Country flag code
	 * @returns {string} Home port name
	 * @private
	 */
	generateHomePort(flag) {
		const homePortsByFlag = {
			US: ['New York', 'Los Angeles', 'Houston', 'Seattle', 'Miami'],
			PA: ['Panama City', 'Colon'],
			LR: ['Monrovia'],
			MH: ['Majuro'],
			BS: ['Nassau', 'Freeport'],
			CY: ['Limassol', 'Larnaca'],
			MT: ['Valletta'],
			GR: ['Piraeus', 'Athens'],
			SG: ['Singapore'],
			HK: ['Hong Kong'],
			CN: ['Shanghai', 'Shenzhen', 'Ningbo', 'Guangzhou'],
			JP: ['Tokyo', 'Osaka', 'Yokohama'],
			KR: ['Busan', 'Incheon'],
			GB: ['London', 'Southampton', 'Liverpool'],
			NO: ['Oslo', 'Bergen'],
			DK: ['Copenhagen', 'Aarhus'],
			NL: ['Rotterdam', 'Amsterdam'],
			DE: ['Hamburg', 'Bremen'],
			IT: ['Genoa', 'Naples', 'Venice'],
			FR: ['Marseille', 'Le Havre'],
		};

		const ports = homePortsByFlag[flag] || ['Unknown Port'];
		return ports[Math.floor(Math.random() * ports.length)];
	}

	/**
	 * Generates a random vessel owner name
	 * @returns {string} Owner name
	 * @private
	 */
	generateOwner() {
		const ownerTypes = ['Shipping', 'Maritime', 'Ocean', 'Lines', 'Carriers'];
		const ownerNames = [
			'Pacific',
			'Atlantic',
			'Global',
			'International',
			'United',
			'Eastern',
			'Western',
			'Northern',
			'Southern',
			'Central',
		];

		const name = ownerNames[Math.floor(Math.random() * ownerNames.length)];
		const type = ownerTypes[Math.floor(Math.random() * ownerTypes.length)];

		return `${name} ${type}`;
	}

	/**
	 * Generates metadata updates over time
	 * Simulates vessels updating their metadata occasionally
	 * @param {number} updatesPerVessel - Number of updates per vessel
	 * @returns {Array<Object>} Array of metadata updates
	 */
	generateUpdates(updatesPerVessel) {
		const updates = [];

		for (const mmsi of this.mmsiList) {
			// Generate initial metadata
			let vessel = this.generateVesselMetadata(mmsi);
			updates.push(vessel);

			// Generate subsequent updates with minor changes
			for (let i = 1; i < updatesPerVessel; i++) {
				const updateTime = new Date(this.startTime.getTime() + (this.durationMs / updatesPerVessel) * i).toISOString();

				// Update with occasional changes
				vessel = {
					...vessel,
					last_updated: updateTime,
					status: VESSEL_STATUSES[Math.floor(Math.random() * VESSEL_STATUSES.length)],
					// Occasionally change owner (ownership transfer)
					owner: Math.random() < 0.1 ? this.generateOwner() : vessel.owner,
					// Occasionally change home port
					home_port: Math.random() < 0.05 ? this.generateHomePort(vessel.flag) : vessel.home_port,
				};

				updates.push(vessel);
			}
		}

		// Sort by last_updated
		updates.sort((a, b) => new Date(a.last_updated) - new Date(b.last_updated));

		return updates;
	}

	/**
	 * Generates metadata for specific vessels by MMSI
	 * @param {Array<string>} mmsiList - List of MMSI to generate metadata for
	 * @returns {Array<Object>} Array of vessel metadata
	 */
	generateForVessels(mmsiList) {
		const vessels = [];

		for (const mmsi of mmsiList) {
			// Check if we have a sample vessel with this MMSI
			const sampleVessel = SAMPLE_VESSELS.find((v) => v.mmsi === mmsi);

			if (sampleVessel) {
				vessels.push(this.enrichVesselMetadata(sampleVessel));
			} else {
				vessels.push(this.generateVesselMetadata(mmsi));
			}
		}

		return vessels;
	}

	/**
	 * Gets statistics about generated vessel metadata
	 * @param {Array<Object>} vessels - Array of vessels
	 * @returns {Object} Statistics object
	 */
	getStatistics(vessels) {
		const stats = {
			totalVessels: vessels.length,
			vesselsByType: {},
			vesselsByFlag: {},
			averageAge: 0,
			averageLength: 0,
			timespan: {
				start: vessels[0]?.last_updated,
				end: vessels[vessels.length - 1]?.last_updated,
			},
		};

		let totalAge = 0;
		let totalLength = 0;
		const currentYear = new Date().getFullYear();

		for (const vessel of vessels) {
			// Count by type
			stats.vesselsByType[vessel.vessel_type] = (stats.vesselsByType[vessel.vessel_type] || 0) + 1;

			// Count by flag
			stats.vesselsByFlag[vessel.flag] = (stats.vesselsByFlag[vessel.flag] || 0) + 1;

			// Calculate average age
			totalAge += currentYear - vessel.year_built;

			// Calculate average length
			totalLength += vessel.length;
		}

		stats.averageAge = vessels.length > 0 ? Math.floor(totalAge / vessels.length) : 0;
		stats.averageLength = vessels.length > 0 ? Math.floor(totalLength / vessels.length) : 0;

		return stats;
	}
}

export default VesselMetadataGenerator;

