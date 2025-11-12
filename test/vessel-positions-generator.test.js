/**
 * Tests for vessel-positions-generator.js
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { VesselPositionsGenerator } from '../ext/maritime-data-synthesizer/generators/vessel-positions-generator.js';

describe('VesselPositionsGenerator', () => {
  describe('Constructor', () => {
    it('should initialize with vessels array', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP_1', vesselType: 'Container Ship' },
        { mmsi: '367123457', startLat: 37.8, startLon: -122.5, vesselName: 'TEST_SHIP_2', vesselType: 'Cargo' }
      ];

      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000, // 1 hour
        vessels
      });

      assert.strictEqual(generator.vessels.length, 2);
      assert.strictEqual(generator.durationMs, 3600000);
      assert.ok(generator.generator, 'Should have initialized underlying generator');
    });

    it('should default to empty vessels array', () => {
      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000
      });

      assert.strictEqual(generator.vessels.length, 0);
    });

    it('should calculate endTime correctly', () => {
      const startTime = new Date('2024-01-01T00:00:00Z');
      const durationMs = 7200000; // 2 hours

      const generator = new VesselPositionsGenerator({
        startTime,
        durationMs,
        vessels: []
      });

      assert.strictEqual(
        generator.endTime.getTime(),
        startTime.getTime() + durationMs
      );
    });
  });

  describe('generate', () => {
    it('should generate specified number of records', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP', vesselType: 'Container Ship' }
      ];

      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000,
        vessels
      });

      const records = generator.generate(10);

      assert.strictEqual(records.length, 10);
    });

    it('should spread records across duration', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP', vesselType: 'Container Ship' }
      ];

      const startTime = new Date('2024-01-01T00:00:00Z');
      const durationMs = 3600000; // 1 hour

      const generator = new VesselPositionsGenerator({
        startTime,
        durationMs,
        vessels
      });

      const records = generator.generate(4);

      // Records should be spread across the duration
      assert.strictEqual(records.length, 4);

      // First record should be at or after startTime
      const firstTimestamp = new Date(records[0].timestamp);
      assert.ok(firstTimestamp >= startTime);

      // Last record should be before endTime
      const lastTimestamp = new Date(records[records.length - 1].timestamp);
      assert.ok(lastTimestamp < new Date(startTime.getTime() + durationMs));
    });

    it('should handle zero count gracefully', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP', vesselType: 'Container Ship' }
      ];

      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000,
        vessels
      });

      const records = generator.generate(0);

      assert.strictEqual(records.length, 0);
    });
  });

  describe('generateAll', () => {
    it('should calculate total records based on vessels count', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP_1', vesselType: 'Container Ship' },
        { mmsi: '367123457', startLat: 37.8, startLon: -122.5, vesselName: 'TEST_SHIP_2', vesselType: 'Cargo' }
      ];

      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000, // 1 hour
        vessels
      });

      const records = generator.generateAll();

      // recordsPerHour = 144, hours = 1, vessels = 2
      // Expected: 144 * 1 * 2 = 288
      assert.strictEqual(records.length, 288);
    });

    it('should scale with duration', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP', vesselType: 'Container Ship' }
      ];

      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 7200000, // 2 hours
        vessels
      });

      const records = generator.generateAll();

      // recordsPerHour = 144, hours = 2, vessels = 1
      // Expected: 144 * 2 * 1 = 288
      assert.strictEqual(records.length, 288);
    });

    it('should return empty array when no vessels', () => {
      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000,
        vessels: []
      });

      const records = generator.generateAll();

      assert.strictEqual(records.length, 0);
    });
  });

  describe('Integration with MaritimeVesselGenerator', () => {
    it('should produce valid vessel position records', () => {
      const vessels = [
        { mmsi: '367123456', startLat: 37.7749, startLon: -122.4194, vesselName: 'TEST_SHIP', vesselType: 'Container Ship' }
      ];

      const generator = new VesselPositionsGenerator({
        startTime: new Date('2024-01-01T00:00:00Z'),
        durationMs: 3600000,
        vessels
      });

      const records = generator.generate(5);

      // Verify record structure
      assert.strictEqual(records.length, 5);

      for (const record of records) {
        assert.ok(record.timestamp, 'Record should have timestamp');
        assert.ok(record.mmsi, 'Record should have mmsi');
        assert.ok(typeof record.latitude === 'number', 'Record should have numeric latitude');
        assert.ok(typeof record.longitude === 'number', 'Record should have numeric longitude');
        assert.ok(record.vessel_name, 'Record should have vessel_name');
        assert.ok(record.vessel_type, 'Record should have vessel_type');
      }
    });
  });
});
