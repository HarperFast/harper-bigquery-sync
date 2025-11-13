/**
 * Tests for schema-leader-election.js
 *
 * Tests distributed locking and adaptive polling for schema checks
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { SchemaLeaderElection } from '../src/schema-leader-election.js';

describe('SchemaLeaderElection', () => {
	describe('constructor', () => {
		it('should initialize with default timing', () => {
			const mockSchemaManager = {};
			const mockOperationsClient = {};
			const election = new SchemaLeaderElection(mockSchemaManager, mockOperationsClient);

			assert.strictEqual(election.currentInterval, 5 * 60 * 1000); // 5 minutes
			assert.strictEqual(election.minInterval, 5 * 60 * 1000);
			assert.strictEqual(election.maxInterval, 30 * 60 * 1000); // 30 minutes
			assert.strictEqual(election.consecutiveNoChanges, 0);
		});

		it('should generate unique node ID', () => {
			const mockSchemaManager = {};
			const mockOperationsClient = {};
			const election = new SchemaLeaderElection(mockSchemaManager, mockOperationsClient);

			assert.ok(election.nodeId);
			assert.ok(election.nodeId.length > 0);
		});

		it('should store schema manager and operations client', () => {
			const mockSchemaManager = { test: true };
			const mockOperationsClient = { test: true };
			const election = new SchemaLeaderElection(mockSchemaManager, mockOperationsClient);

			assert.strictEqual(election.schemaManager, mockSchemaManager);
			assert.strictEqual(election.operationsClient, mockOperationsClient);
		});
	});

	describe('adjustInterval', () => {
		it('should reset to min interval when changes detected', () => {
			const election = new SchemaLeaderElection({}, {});
			election.currentInterval = 20 * 60 * 1000; // 20 minutes
			election.consecutiveNoChanges = 5;

			election.adjustInterval(true); // hasChanges = true

			assert.strictEqual(election.currentInterval, 5 * 60 * 1000); // Reset to 5 min
			assert.strictEqual(election.consecutiveNoChanges, 0);
		});

		it('should increase interval after multiple checks with no changes', () => {
			const election = new SchemaLeaderElection({}, {});
			election.currentInterval = 5 * 60 * 1000; // 5 minutes
			election.consecutiveNoChanges = 2;

			election.adjustInterval(false); // No changes

			assert.strictEqual(election.consecutiveNoChanges, 3);
			// After 3 consecutive no-changes, should back off
			assert.ok(election.currentInterval > 5 * 60 * 1000);
		});

		it('should not exceed max interval', () => {
			const election = new SchemaLeaderElection({}, {});
			election.currentInterval = 30 * 60 * 1000; // Already at max
			election.consecutiveNoChanges = 10;

			election.adjustInterval(false); // No changes

			assert.strictEqual(election.currentInterval, 30 * 60 * 1000); // Should not exceed max
		});

		it('should increment no-change counter when no changes', () => {
			const election = new SchemaLeaderElection({}, {});
			election.consecutiveNoChanges = 0;

			election.adjustInterval(false); // No changes

			assert.strictEqual(election.consecutiveNoChanges, 1);
		});

		it('should not adjust interval until threshold met', () => {
			const election = new SchemaLeaderElection({}, {});
			const initialInterval = election.currentInterval;
			election.consecutiveNoChanges = 1;

			election.adjustInterval(false); // No changes, but only 2 consecutive

			assert.strictEqual(election.currentInterval, initialInterval); // Should not change yet
			assert.strictEqual(election.consecutiveNoChanges, 2);
		});
	});

	describe('isLockExpired', () => {
		it('should return true if lock expired', () => {
			const election = new SchemaLeaderElection({}, {});
			const pastTime = new Date(Date.now() - 60 * 1000); // 1 minute ago

			assert.strictEqual(election.isLockExpired(pastTime), true);
		});

		it('should return false if lock still valid', () => {
			const election = new SchemaLeaderElection({}, {});
			const futureTime = new Date(Date.now() + 60 * 1000); // 1 minute from now

			assert.strictEqual(election.isLockExpired(futureTime), false);
		});

		it('should handle Date objects', () => {
			const election = new SchemaLeaderElection({}, {});
			const expiry = new Date(Date.now() - 1000);

			assert.strictEqual(election.isLockExpired(expiry), true);
		});

		it('should handle ISO strings', () => {
			const election = new SchemaLeaderElection({}, {});
			const expiry = new Date(Date.now() - 1000).toISOString();

			assert.strictEqual(election.isLockExpired(expiry), true);
		});
	});
});
