class Globals {
	constructor() {
		if (Globals.instance) {
			return Globals.instance;
		}
		this.data = {};
		Globals.instance = this;
	}
	set(key, value) {
		logger.debug(`[Globals.set] Setting '${key}' = ${JSON.stringify(value)}`);
		this.data[key] = value;
	}
	get(key) {
		const value = this.data[key];
		if (value === undefined) {
			logger.debug(`[Globals.get] Key '${key}' not found`);
		} else {
			logger.debug(`[Globals.get] Retrieved '${key}' = ${JSON.stringify(value)}`);
		}
		return value;
	}
}

const globals = new Globals();

export { globals, Globals };

export default Globals;
