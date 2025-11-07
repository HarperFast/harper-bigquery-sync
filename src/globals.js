class Globals {
    constructor() {
        if(Globals.instance) {
            return Globals.instance;
        }
        this.data = {};
        Globals.instance = this;
    }
    set(key, value) {
        this.data[key] = value;
    }
    get(key) {
        return this.data[key];
    }
}

const globals = new Globals();

export { globals, Globals };

export default Globals;