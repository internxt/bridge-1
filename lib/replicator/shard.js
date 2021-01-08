const async = require('async');
const Storage = require('storj-service-storage-models');
const logger = require('../logger');

class ShardReplicator {
  constructor(config) {
    this.storage = null;
    this._config = config;
    this.initialized = false;
  }

  init(storage) {
    if (this.initialized) {
      return logger.warn('Object ShardReplicator already initialized');
    }

    if (storage) {
      this.storage = storage;
    } else {
      this.storage = new Storage();
    }

    this.initialized = true;
  }

  replicateShard() {
    return async.parallel({
      destinations: (next) => {

      },
      sources: (next) => {

      }
    });
  }

  _fetchDestinations(shard) {
    const { Mirror } = this.storage.models;

    Mirror.findOne({
      shardHash: shard.hash,
      isEstablished: false
    });
        
  }

  _fetchSources(shard) {

  }
}

module.exports = ShardReplicator;