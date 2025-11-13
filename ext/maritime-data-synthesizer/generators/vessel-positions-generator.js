/**
 * Vessel Positions Data Generator
 *
 * Wrapper around the main vessel generator to match the multi-table
 * orchestrator's expected interface.
 */

import MaritimeVesselGenerator from '../../../src/generator.js';

export class VesselPositionsGenerator {
	/**
	 * Creates a new VesselPositionsGenerator
	 * @param {Object} options - Configuration options
	 * @param {Date} options.startTime - Start timestamp
	 * @param {number} options.durationMs - Duration in milliseconds
	 * @param {Array<Object>} options.vessels - Array of vessel objects with mmsi, startLat, startLon, vesselName, vesselType
	 */
	constructor({ startTime, durationMs, vessels = [] }) {
		this.startTime = new Date(startTime);
		this.durationMs = durationMs;
		this.endTime = new Date(this.startTime.getTime() + durationMs);
		this.vessels = vessels;

		// Initialize the underlying vessel generator
		this.generator = new MaritimeVesselGenerator({
			totalVessels: vessels.length,
			startTime: this.startTime,
		});
	}

	/**
	 * Generates a batch of vessel position records
	 * @param {number} count - Number of records to generate
	 * @returns {Array<Object>} Array of vessel position records
	 */
	generate(count) {
		const records = [];
		const timeStep = this.durationMs / count;

		for (let i = 0; i < count; i++) {
			const timestamp = new Date(this.startTime.getTime() + i * timeStep);
			const batch = this.generator.generateBatch(1, timestamp);
			records.push(...batch);
		}

		return records;
	}

	/**
	 * Generates all records for the configured time range
	 * @returns {Array<Object>} Array of all vessel position records
	 */
	generateAll() {
		const recordsPerHour = 144; // ~2.4 records per vessel per hour
		const hours = this.durationMs / (60 * 60 * 1000);
		const totalRecords = Math.floor(recordsPerHour * hours * this.vessels.length);

		return this.generate(totalRecords);
	}
}
