// my plugin entry point
import { globals } from './globals.js';

import { SyncEngine } from './sync-engine.js';
// TODO: Validation not yet implemented - requires additional testing
// import { ValidationService } from './validation.js';
import { logger } from '@google-cloud/bigquery/build/src/logger.js';

export async function handleApplication(scope) {
	const logger = scope.logger;
	const options = scope.options.getAll();
	const syncEngine = new SyncEngine(options);
	syncEngine.initialize();
	globals.set('syncEngine', syncEngine);
	// TODO: Validation not yet implemented - requires additional testing
	// globals.set('validator', new ValidationService(options));
}
