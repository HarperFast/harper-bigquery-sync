/**
 * Maritime Vessel Data Synthesizer
 * Entry point for the maritime synthesizer package
 */

import MaritimeDataSynthesizer from './service.js';
import MaritimeVesselGenerator from './generator.js';
import MaritimeBigQueryClient from './bigquery.js';

export {
  MaritimeDataSynthesizer,
  MaritimeVesselGenerator,
  MaritimeBigQueryClient
};

export default MaritimeDataSynthesizer;
