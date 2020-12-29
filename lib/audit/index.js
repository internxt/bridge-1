const Storage = require('storj-service-storage-models');

class Audit {
    constructor(config) {
        this.storage = null;
        this._config = config;
    }

    setStorage(newStorage) {
        this.storage = newStorage;
    }

    init() {
        if (!this.storage) {
            this.storage = new Storage();
        }
    }
}

module.exports = Audit;