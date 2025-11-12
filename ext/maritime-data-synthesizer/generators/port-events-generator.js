/**
 * Port Events Data Generator
 *
 * Generates realistic port event data for vessel arrivals, departures,
 * berthing, anchoring, and underway events.
 */

import { SAMPLE_PORTS, EVENT_TYPES, SAMPLE_VESSELS } from '../../../test/fixtures/multi-table-test-data.js';

export class PortEventsGenerator {
  /**
   * Creates a new PortEventsGenerator
   * @param {Object} options - Configuration options
   * @param {Date} options.startTime - Start timestamp
   * @param {number} options.durationMs - Duration in milliseconds
   * @param {Array<string>} options.mmsiList - List of MMSI identifiers for vessels
   */
  constructor({ startTime, durationMs, mmsiList = [] }) {
    this.startTime = new Date(startTime);
    this.durationMs = durationMs;
    this.endTime = new Date(this.startTime.getTime() + durationMs);

    // Use provided MMSI list or generate from sample vessels
    this.mmsiList = mmsiList.length > 0 ? mmsiList : SAMPLE_VESSELS.map(v => v.mmsi);

    // Track vessel states for realistic event sequences
    this.vesselStates = new Map();
    this.initializeVesselStates();
  }

  /**
   * Initialize tracking state for each vessel
   * @private
   */
  initializeVesselStates() {
    for (const mmsi of this.mmsiList) {
      this.vesselStates.set(mmsi, {
        currentState: 'UNDERWAY',
        lastEvent: null,
        lastPort: null,
        timeSinceLastEvent: 0
      });
    }
  }

  /**
   * Generates a batch of port event records
   * @param {number} count - Number of records to generate
   * @returns {Array<Object>} Array of port event records
   */
  generate(count) {
    const events = [];

    // Calculate average time between events
    const totalEvents = count;
    const avgTimeBetweenEvents = this.durationMs / totalEvents;

    let currentTime = this.startTime.getTime();

    for (let i = 0; i < count; i++) {
      // Select a random vessel
      const mmsi = this.mmsiList[Math.floor(Math.random() * this.mmsiList.length)];
      const state = this.vesselStates.get(mmsi);

      // Select a random port
      const port = SAMPLE_PORTS[Math.floor(Math.random() * SAMPLE_PORTS.length)];

      // Determine next event based on current state
      const eventType = this.getNextEventType(state.currentState);

      // Generate event
      const event = {
        event_time: new Date(currentTime).toISOString(),
        port_id: port.port_id,
        port_name: port.name,
        vessel_mmsi: mmsi,
        event_type: eventType,
        status: this.getStatusFromEventType(eventType),
        latitude: port.lat + (Math.random() - 0.5) * 0.01, // Small variation around port
        longitude: port.lon + (Math.random() - 0.5) * 0.01
      };

      events.push(event);

      // Update vessel state
      state.currentState = eventType;
      state.lastEvent = event;
      state.lastPort = port;
      state.timeSinceLastEvent = 0;

      // Advance time with some randomness
      const timeIncrement = avgTimeBetweenEvents * (0.5 + Math.random());
      currentTime += timeIncrement;

      // Don't exceed end time
      if (currentTime > this.endTime.getTime()) {
        currentTime = this.endTime.getTime();
      }
    }

    // Sort by event_time to ensure chronological order
    events.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));

    return events;
  }

  /**
   * Generates the next logical event type based on current state
   * @param {string} currentState - Current vessel state
   * @returns {string} Next event type
   * @private
   */
  getNextEventType(currentState) {
    // Define realistic state transitions
    const transitions = {
      'UNDERWAY': ['ARRIVAL', 'ANCHORED'],
      'ARRIVAL': ['BERTHED'],
      'BERTHED': ['DEPARTURE'],
      'ANCHORED': ['UNDERWAY', 'ARRIVAL'],
      'DEPARTURE': ['UNDERWAY']
    };

    const possibleNextStates = transitions[currentState] || ['ARRIVAL'];
    return possibleNextStates[Math.floor(Math.random() * possibleNextStates.length)];
  }

  /**
   * Maps event type to vessel status
   * @param {string} eventType - Event type
   * @returns {string} Vessel status
   * @private
   */
  getStatusFromEventType(eventType) {
    const statusMap = {
      'ARRIVAL': 'Arriving',
      'DEPARTURE': 'Departing',
      'BERTHED': 'Moored',
      'ANCHORED': 'At anchor',
      'UNDERWAY': 'Under way using engine'
    };

    return statusMap[eventType] || 'Unknown';
  }

  /**
   * Generates events for a specific vessel over time
   * Useful for creating realistic port call sequences
   * @param {string} mmsi - Vessel MMSI
   * @param {number} numPortCalls - Number of complete port calls to generate
   * @returns {Array<Object>} Array of port events
   */
  generatePortCallSequence(mmsi, numPortCalls) {
    const events = [];
    let currentTime = this.startTime.getTime();

    // Time for a complete port call cycle (arrival → berthed → departure)
    const avgPortCallDuration = this.durationMs / numPortCalls;

    for (let i = 0; i < numPortCalls; i++) {
      const port = SAMPLE_PORTS[Math.floor(Math.random() * SAMPLE_PORTS.length)];

      // Arrival
      events.push({
        event_time: new Date(currentTime).toISOString(),
        port_id: port.port_id,
        port_name: port.name,
        vessel_mmsi: mmsi,
        event_type: 'ARRIVAL',
        status: 'Arriving',
        latitude: port.lat,
        longitude: port.lon
      });

      currentTime += avgPortCallDuration * 0.1; // 10% of time arriving

      // Berthed
      events.push({
        event_time: new Date(currentTime).toISOString(),
        port_id: port.port_id,
        port_name: port.name,
        vessel_mmsi: mmsi,
        event_type: 'BERTHED',
        status: 'Moored',
        latitude: port.lat,
        longitude: port.lon
      });

      currentTime += avgPortCallDuration * 0.6; // 60% of time berthed

      // Departure
      events.push({
        event_time: new Date(currentTime).toISOString(),
        port_id: port.port_id,
        port_name: port.name,
        vessel_mmsi: mmsi,
        event_type: 'DEPARTURE',
        status: 'Departing',
        latitude: port.lat,
        longitude: port.lon
      });

      currentTime += avgPortCallDuration * 0.3; // 30% of time between ports
    }

    return events;
  }

  /**
   * Generates events distributed across multiple ports
   * Useful for testing port-specific queries and aggregations
   * @param {number} eventsPerPort - Number of events per port
   * @returns {Array<Object>} Array of port events
   */
  generateByPort(eventsPerPort) {
    const events = [];

    for (const port of SAMPLE_PORTS) {
      const portEvents = this.generatePortEvents(port, eventsPerPort);
      events.push(...portEvents);
    }

    // Sort by event_time
    events.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));

    return events;
  }

  /**
   * Generates events for a specific port
   * @param {Object} port - Port object
   * @param {number} count - Number of events to generate
   * @returns {Array<Object>} Array of port events
   * @private
   */
  generatePortEvents(port, count) {
    const events = [];
    const avgTimeBetweenEvents = this.durationMs / count;
    let currentTime = this.startTime.getTime();

    for (let i = 0; i < count; i++) {
      const mmsi = this.mmsiList[Math.floor(Math.random() * this.mmsiList.length)];
      const eventType = EVENT_TYPES[Math.floor(Math.random() * EVENT_TYPES.length)];

      events.push({
        event_time: new Date(currentTime).toISOString(),
        port_id: port.port_id,
        port_name: port.name,
        vessel_mmsi: mmsi,
        event_type: eventType,
        status: this.getStatusFromEventType(eventType),
        latitude: port.lat + (Math.random() - 0.5) * 0.01,
        longitude: port.lon + (Math.random() - 0.5) * 0.01
      });

      currentTime += avgTimeBetweenEvents * (0.5 + Math.random());
    }

    return events;
  }

  /**
   * Generates a stream of events over time
   * Useful for testing real-time sync behavior
   * @param {number} eventsPerInterval - Events to generate per interval
   * @param {number} intervalMs - Time interval in milliseconds
   * @returns {Generator<Array<Object>>} Generator yielding batches of events
   */
  *generateStream(eventsPerInterval, intervalMs) {
    const intervals = Math.floor(this.durationMs / intervalMs);
    let currentTime = this.startTime.getTime();

    for (let i = 0; i < intervals; i++) {
      const batchStartTime = currentTime;
      const batchEndTime = currentTime + intervalMs;

      const events = [];
      const avgTimeBetweenEvents = intervalMs / eventsPerInterval;
      let eventTime = batchStartTime;

      for (let j = 0; j < eventsPerInterval; j++) {
        const mmsi = this.mmsiList[Math.floor(Math.random() * this.mmsiList.length)];
        const state = this.vesselStates.get(mmsi);
        const port = SAMPLE_PORTS[Math.floor(Math.random() * SAMPLE_PORTS.length)];
        const eventType = this.getNextEventType(state.currentState);

        events.push({
          event_time: new Date(eventTime).toISOString(),
          port_id: port.port_id,
          port_name: port.name,
          vessel_mmsi: mmsi,
          event_type: eventType,
          status: this.getStatusFromEventType(eventType),
          latitude: port.lat,
          longitude: port.lon
        });

        state.currentState = eventType;
        eventTime += avgTimeBetweenEvents;
      }

      yield events;
      currentTime = batchEndTime;
    }
  }

  /**
   * Gets statistics about generated events
   * Useful for verification and debugging
   * @param {Array<Object>} events - Array of events
   * @returns {Object} Statistics object
   */
  getStatistics(events) {
    const stats = {
      totalEvents: events.length,
      eventsByType: {},
      eventsByPort: {},
      eventsByVessel: {},
      timespan: {
        start: events[0]?.event_time,
        end: events[events.length - 1]?.event_time,
        durationMs: events.length > 0
          ? new Date(events[events.length - 1].event_time) - new Date(events[0].event_time)
          : 0
      }
    };

    for (const event of events) {
      // Count by type
      stats.eventsByType[event.event_type] = (stats.eventsByType[event.event_type] || 0) + 1;

      // Count by port
      stats.eventsByPort[event.port_id] = (stats.eventsByPort[event.port_id] || 0) + 1;

      // Count by vessel
      stats.eventsByVessel[event.vessel_mmsi] = (stats.eventsByVessel[event.vessel_mmsi] || 0) + 1;
    }

    return stats;
  }
}

export default PortEventsGenerator;
