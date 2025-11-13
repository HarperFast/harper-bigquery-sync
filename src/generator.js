/**
 * Maritime Vessel Data Generator
 * Generates realistic synthetic vessel tracking data with global distribution
 */

// Major ports around the world with coordinates and traffic weight
const MAJOR_PORTS = [
	// Asia-Pacific (50% of global maritime traffic)
	{ name: 'Singapore', lat: 1.2644, lon: 103.8223, weight: 10, region: 'Asia' },
	{ name: 'Shanghai', lat: 31.2304, lon: 121.4737, weight: 9, region: 'Asia' },
	{ name: 'Hong Kong', lat: 22.3193, lon: 114.1694, weight: 7, region: 'Asia' },
	{ name: 'Busan', lat: 35.1796, lon: 129.0756, weight: 6, region: 'Asia' },
	{ name: 'Guangzhou', lat: 23.1291, lon: 113.2644, weight: 5, region: 'Asia' },
	{ name: 'Qingdao', lat: 36.0671, lon: 120.3826, weight: 5, region: 'Asia' },
	{ name: 'Tokyo', lat: 35.6528, lon: 139.8394, weight: 4, region: 'Asia' },
	{ name: 'Port Klang', lat: 3.0041, lon: 101.3653, weight: 4, region: 'Asia' },
	{ name: 'Kaohsiung', lat: 22.6163, lon: 120.2997, weight: 3, region: 'Asia' },
	{ name: 'Tianjin', lat: 39.0842, lon: 117.201, weight: 4, region: 'Asia' },

	// Europe (20% of global maritime traffic)
	{ name: 'Rotterdam', lat: 51.9225, lon: 4.4792, weight: 7, region: 'Europe' },
	{ name: 'Antwerp', lat: 51.2194, lon: 4.4025, weight: 5, region: 'Europe' },
	{ name: 'Hamburg', lat: 53.5511, lon: 9.9937, weight: 4, region: 'Europe' },
	{ name: 'Valencia', lat: 39.4699, lon: -0.3763, weight: 3, region: 'Europe' },
	{ name: 'Piraeus', lat: 37.9472, lon: 23.6472, weight: 3, region: 'Europe' },
	{ name: 'Felixstowe', lat: 51.9542, lon: 1.3511, weight: 2, region: 'Europe' },

	// Middle East (10% of global maritime traffic)
	{ name: 'Dubai', lat: 25.2769, lon: 55.2963, weight: 6, region: 'Middle East' },
	{ name: 'Jeddah', lat: 21.5433, lon: 39.1728, weight: 3, region: 'Middle East' },

	// Americas (15% of global maritime traffic)
	{ name: 'Los Angeles', lat: 33.7405, lon: -118.272, weight: 6, region: 'Americas' },
	{ name: 'Long Beach', lat: 33.7683, lon: -118.1956, weight: 5, region: 'Americas' },
	{ name: 'New York/New Jersey', lat: 40.67, lon: -74.04, weight: 5, region: 'Americas' },
	{ name: 'Savannah', lat: 32.0809, lon: -81.0912, weight: 3, region: 'Americas' },
	{ name: 'Houston', lat: 29.7604, lon: -95.3698, weight: 4, region: 'Americas' },
	{ name: 'Vancouver', lat: 49.2827, lon: -123.1207, weight: 3, region: 'Americas' },
	{ name: 'Santos', lat: -23.9618, lon: -46.3322, weight: 3, region: 'Americas' },
	{ name: 'Manzanillo', lat: 19.0544, lon: -104.3188, weight: 2, region: 'Americas' },

	// Africa (5% of global maritime traffic)
	{ name: 'Cape Town', lat: -33.9249, lon: 18.4241, weight: 2, region: 'Africa' },
	{ name: 'Durban', lat: -29.8587, lon: 31.0218, weight: 2, region: 'Africa' },
	{ name: 'Lagos', lat: 6.4526, lon: 3.3958, weight: 2, region: 'Africa' },
];

// Vessel types with characteristics
const VESSEL_TYPES = [
	{
		type: 'CONTAINER',
		speedRange: [18, 25], // knots
		lengthRange: [200, 400], // meters
		draftRange: [10, 16], // meters
		distribution: 0.35, // 35% of vessels
	},
	{
		type: 'BULK_CARRIER',
		speedRange: [12, 16],
		lengthRange: [150, 300],
		draftRange: [8, 14],
		distribution: 0.25,
	},
	{
		type: 'TANKER',
		speedRange: [13, 17],
		lengthRange: [180, 330],
		draftRange: [10, 18],
		distribution: 0.2,
	},
	{
		type: 'CARGO',
		speedRange: [14, 19],
		lengthRange: [120, 250],
		draftRange: [7, 12],
		distribution: 0.1,
	},
	{
		type: 'PASSENGER',
		speedRange: [20, 30],
		lengthRange: [200, 360],
		draftRange: [7, 9],
		distribution: 0.05,
	},
	{
		type: 'FISHING',
		speedRange: [8, 14],
		lengthRange: [30, 100],
		draftRange: [3, 6],
		distribution: 0.05,
	},
];

// Vessel status options
const _VESSEL_STATUS = ['UNDERWAY_USING_ENGINE', 'AT_ANCHOR', 'MOORED', 'UNDERWAY_SAILING', 'NOT_UNDER_COMMAND'];

// Flag states (top maritime nations)
const FLAG_STATES = [
	'PA',
	'LR',
	'MH',
	'HK',
	'SG',
	'MT',
	'BS',
	'CY',
	'IM',
	'GR',
	'CN',
	'KR',
	'JP',
	'US',
	'GB',
	'NO',
	'DE',
	'IT',
	'NL',
	'DK',
];

class MaritimeVesselGenerator {
	constructor(config = {}) {
		this.totalVessels = config.totalVessels || 100000; // Global fleet
		this.vesselsPerBatch = config.vesselsPerBatch || 100;
		this.vesselPool = [];
		this.journeys = new Map(); // Track ongoing journeys

		// Initialize vessel pool
		this.initializeVesselPool();
		console.log(`Initialized vessel pool with ${this.vesselPool.length} vessels`);
	}

	/**
	 * Initialize a pool of vessels with persistent identifiers
	 */
	initializeVesselPool() {
		const cacheSize = Math.min(10000, this.totalVessels);

		for (let i = 0; i < cacheSize; i++) {
			const vesselType = this.selectVesselType();
			const vessel = {
				mmsi: this.generateMMSI(),
				imo: this.generateIMO(),
				name: this.generateVesselName(vesselType.type, i),
				type: vesselType.type,
				flag: FLAG_STATES[Math.floor(Math.random() * FLAG_STATES.length)],
				length: Math.floor(
					Math.random() * (vesselType.lengthRange[1] - vesselType.lengthRange[0]) + vesselType.lengthRange[0]
				),
				beam: 0, // will calculate from length
				draft: parseFloat(
					(Math.random() * (vesselType.draftRange[1] - vesselType.draftRange[0]) + vesselType.draftRange[0]).toFixed(1)
				),
				maxSpeed: vesselType.speedRange[1],
				cruiseSpeed: Math.floor(
					Math.random() * (vesselType.speedRange[1] - vesselType.speedRange[0]) + vesselType.speedRange[0]
				),
			};

			// Beam typically 1/8 to 1/6 of length
			vessel.beam = Math.floor(vessel.length / 7);

			this.vesselPool.push(vessel);
		}
	}

	/**
	 * Generate a 9-digit MMSI (Maritime Mobile Service Identity)
	 */
	generateMMSI() {
		// Format: MIDxxxxxx where MID is Maritime Identification Digits (country code)
		const mids = [
			'201',
			'211',
			'219',
			'227',
			'232',
			'236',
			'240',
			'244',
			'247', // European countries
			'303',
			'310',
			'338', // North American countries
			'412',
			'413',
			'414',
			'416',
			'419', // Asian countries
			'563',
			'564',
			'565',
			'566',
			'567', // Southeast Asian countries
		];

		const mid = mids[Math.floor(Math.random() * mids.length)];
		const remaining = String(Math.floor(Math.random() * 1000000)).padStart(6, '0');

		return mid + remaining;
	}

	/**
	 * Generate a 7-digit IMO number
	 */
	generateIMO() {
		const base = String(Math.floor(Math.random() * 1000000) + 1000000);
		return base.substring(0, 7);
	}

	/**
	 * Generate vessel name
	 */
	generateVesselName(type, index) {
		const prefixes = ['MV', 'MT', 'MSC', 'COSCO', 'MAERSK', 'EVERGREEN', 'CMA CGM'];
		const names = ['FORTUNE', 'VOYAGER', 'EXPLORER', 'STAR', 'OCEAN', 'SPIRIT', 'PIONEER', 'LIBERTY'];

		const prefix = prefixes[Math.floor(Math.random() * prefixes.length)];
		const name = names[Math.floor(Math.random() * names.length)];

		return `${prefix} ${name} ${index % 100}`;
	}

	/**
	 * Select vessel type based on distribution
	 */
	selectVesselType() {
		const rand = Math.random();
		let cumulative = 0;

		for (const vesselType of VESSEL_TYPES) {
			cumulative += vesselType.distribution;
			if (rand <= cumulative) {
				return vesselType;
			}
		}

		return VESSEL_TYPES[0];
	}

	/**
	 * Select a random port weighted by traffic volume
	 */
	selectWeightedPort() {
		const totalWeight = MAJOR_PORTS.reduce((sum, port) => sum + port.weight, 0);
		const rand = Math.random() * totalWeight;
		let cumulative = 0;

		for (const port of MAJOR_PORTS) {
			cumulative += port.weight;
			if (rand <= cumulative) {
				return port;
			}
		}

		return MAJOR_PORTS[0];
	}

	/**
	 * Calculate distance between two points (Haversine formula)
	 */
	calculateDistance(lat1, lon1, lat2, lon2) {
		const R = 6371; // Earth radius in km
		const dLat = ((lat2 - lat1) * Math.PI) / 180;
		const dLon = ((lon2 - lon1) * Math.PI) / 180;

		const a =
			Math.sin(dLat / 2) * Math.sin(dLat / 2) +
			Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) * Math.sin(dLon / 2);

		const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		const distance = R * c; // Distance in km

		return distance * 0.539957; // Convert to nautical miles
	}

	/**
	 * Calculate bearing between two points
	 */
	calculateBearing(lat1, lon1, lat2, lon2) {
		const dLon = ((lon2 - lon1) * Math.PI) / 180;
		const y = Math.sin(dLon) * Math.cos((lat2 * Math.PI) / 180);
		const x =
			Math.cos((lat1 * Math.PI) / 180) * Math.sin((lat2 * Math.PI) / 180) -
			Math.sin((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.cos(dLon);

		const bearing = (Math.atan2(y, x) * 180) / Math.PI;
		return (bearing + 360) % 360;
	}

	/**
	 * Calculate new position given start position, bearing, and distance
	 */
	calculateNewPosition(lat, lon, bearing, distance) {
		const R = 6371; // Earth radius in km
		const distanceKm = distance * 1.852; // Convert nautical miles to km

		const bearingRad = (bearing * Math.PI) / 180;
		const lat1 = (lat * Math.PI) / 180;
		const lon1 = (lon * Math.PI) / 180;

		const lat2 = Math.asin(
			Math.sin(lat1) * Math.cos(distanceKm / R) + Math.cos(lat1) * Math.sin(distanceKm / R) * Math.cos(bearingRad)
		);

		const lon2 =
			lon1 +
			Math.atan2(
				Math.sin(bearingRad) * Math.sin(distanceKm / R) * Math.cos(lat1),
				Math.cos(distanceKm / R) - Math.sin(lat1) * Math.sin(lat2)
			);

		return {
			lat: (lat2 * 180) / Math.PI,
			lon: (lon2 * 180) / Math.PI,
		};
	}

	/**
	 * Generate vessel position based on journey status
	 */
	generateVesselPosition(vessel, timestamp) {
		const journeyId = vessel.mmsi;
		let journey = this.journeys.get(journeyId);

		// 30% chance to be in port, 70% at sea
		const inPort = Math.random() < 0.3;

		if (!journey || journey.completed) {
			// Start new journey
			const origin = this.selectWeightedPort();
			let destination = this.selectWeightedPort();

			// Ensure different origin and destination
			let attempts = 0;
			while (destination.name === origin.name && attempts < 10) {
				destination = this.selectWeightedPort();
				attempts++;
			}

			journey = {
				origin,
				destination,
				startTime: timestamp,
				currentLat: origin.lat + (Math.random() - 0.5) * 0.05, // Small offset within port
				currentLon: origin.lon + (Math.random() - 0.5) * 0.05,
				completed: false,
				inPort: true,
			};

			this.journeys.set(journeyId, journey);
		}

		let status, speed, course;

		if (inPort || journey.inPort) {
			// Vessel in port
			status = Math.random() < 0.5 ? 'AT_ANCHOR' : 'MOORED';
			speed = parseFloat((Math.random() * 0.5).toFixed(1)); // Very slow or stationary
			course = Math.floor(Math.random() * 360);

			// Small random movement within port area
			journey.currentLat += (Math.random() - 0.5) * 0.01;
			journey.currentLon += (Math.random() - 0.5) * 0.01;

			// 20% chance to leave port
			if (Math.random() < 0.2) {
				journey.inPort = false;
			}
		} else {
			// Vessel at sea, moving toward destination
			status = 'UNDERWAY_USING_ENGINE';

			const distanceToDestination = this.calculateDistance(
				journey.currentLat,
				journey.currentLon,
				journey.destination.lat,
				journey.destination.lon
			);

			// Speed variation: 80-100% of cruise speed
			speed = parseFloat((vessel.cruiseSpeed * (0.8 + Math.random() * 0.2)).toFixed(1));
			course = Math.floor(
				this.calculateBearing(journey.currentLat, journey.currentLon, journey.destination.lat, journey.destination.lon)
			);

			// Move vessel (assume 1 hour between reports)
			const distanceTraveled = speed; // nautical miles in 1 hour

			if (distanceToDestination <= distanceTraveled * 2) {
				// Arriving at destination
				journey.currentLat = journey.destination.lat + (Math.random() - 0.5) * 0.05;
				journey.currentLon = journey.destination.lon + (Math.random() - 0.5) * 0.05;
				journey.inPort = true;
				speed = 0.5;
				status = 'AT_ANCHOR';
			} else {
				// Continue journey
				const newPos = this.calculateNewPosition(journey.currentLat, journey.currentLon, course, distanceTraveled);

				journey.currentLat = newPos.lat;
				journey.currentLon = newPos.lon;
			}
		}

		return {
			latitude: parseFloat(journey.currentLat.toFixed(6)),
			longitude: parseFloat(journey.currentLon.toFixed(6)),
			speed,
			course,
			status,
			destination: journey.destination.name,
		};
	}

	/**
	 * Generate a batch of vessel position records
	 */
	generateBatch(count = this.vesselsPerBatch, timestampOffset = 0) {
		const records = [];
		const now = new Date(Date.now() - timestampOffset);

		for (let i = 0; i < count; i++) {
			const vessel = this.vesselPool[Math.floor(Math.random() * this.vesselPool.length)];

			// Add some time variation within the batch (spread over last hour)
			const recordTime = new Date(now.getTime() - Math.random() * 3600000);

			const position = this.generateVesselPosition(vessel, recordTime);

			const record = {
				mmsi: vessel.mmsi,
				imo: vessel.imo,
				vessel_name: vessel.name,
				vessel_type: vessel.type,
				flag: vessel.flag,
				length: vessel.length,
				beam: vessel.beam,
				draft: vessel.draft,
				latitude: position.latitude,
				longitude: position.longitude,
				speed_knots: position.speed,
				course: position.course,
				heading: position.course, // Simplified: heading = course
				status: position.status,
				destination: position.destination,
				eta: new Date(recordTime.getTime() + Math.random() * 7 * 24 * 3600000).toISOString(), // Random ETA within 7 days
				timestamp: recordTime.toISOString(),
				report_date: recordTime.toISOString().split('T')[0].replace(/-/g, ''), // YYYYMMDD
			};

			records.push(record);
		}

		return records;
	}

	/**
	 * Get statistics about the vessel pool
	 */
	getStats() {
		const typeDistribution = {};

		for (const vessel of this.vesselPool) {
			typeDistribution[vessel.type] = (typeDistribution[vessel.type] || 0) + 1;
		}

		return {
			totalVessels: this.vesselPool.length,
			typeDistribution,
			portsCount: MAJOR_PORTS.length,
			activeJourneys: this.journeys.size,
		};
	}
}

export default MaritimeVesselGenerator;
