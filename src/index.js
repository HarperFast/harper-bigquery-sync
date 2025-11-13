// my plugin entry point
import { globals } from './globals.js';

import { SyncEngine } from './sync-engine.js';
// TODO: Validation not yet implemented - requires additional testing
// import { ValidationService } from './validation.js';

export async function handleApplication(scope) {
	const _logger = scope.logger;
	const options = scope.options.getAll();
	const syncEngine = new SyncEngine(options);
	syncEngine.initialize();
	globals.set('syncEngine', syncEngine);
	// TODO: Validation not yet implemented - requires additional testing
	// globals.set('validator', new ValidationService(options));
}
