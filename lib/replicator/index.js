const log = require('../logger');
const assert = require('assert');
const ReplicatorConfig = require('./config');
const ms = require('ms');
const crypto = require('crypto');
const async = require('async');
const utils = require('../utils');
const Storage = require('storj-service-storage-models');
const ComplexClient = require('storj-complex').createClient;
const storj = require('storj-lib');
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const Actions = require('../actions');
const logger = require('../logger');

const SIGINT_CHECK_INTERVAL = 1000;
const MAX_SIGINT_WAIT = 5000;

class Replicator {
  constructor(config) {
    this.initialized = false;
    assert(config instanceof ReplicatorConfig, 'Invalid config supplied');
    this._config = config;
    this.storage = null;

    this._timeout = null;
    this._running = false;
  }

  init() {
    if (this.initialized) {
      return log.warn('Replicator already initialized');
    }

    // Initialize all the services
    this.storage = new Storage(
      this._config.storage.mongoUrl,
      this._config.storage.mongoOpts,
      { logger: log }
    );

    this.network = new ComplexClient(this._config.complex);

    this.contracts = new storj.StorageManager(
      new MongoDBStorageAdapter(this.storage),
      { disableReaper: true }
    );

    this.actions = new Actions(this._config, this.storage, this.network, this.contracts);

    this.initialized = true;
  }

  start(callback) {
    if (!this.initialized) {
      return callback(new Error('Replicator not initialized'));
    }
    this.wait();
    callback();
    process.on('SIGINT', this._handleSIGINT.bind(this));
    process.on('exit', this._handleExit.bind(this));
    process.on('uncaughtException', this._handleUncaughtException.bind(this));
  }

  finish() {
    this._running = false;
    this.wait();
  }

  run() {
    if (this._running) {
      return this.wait();
    }

    const limit = this._config.application.queryNumber || 10;

    log.info('Starting replicator round for %s shards', limit);
    this._running = true;

    // Put your logic here, ensure to call finish() to schedule a new round.
    this.checkMirrors();
  }

  checkMirrors() {
    const { User, Shard, Mirror } = this.storage.models;

    const mirrorsQuery = [
      { '$match': { 'maxSpaceBytes': { '$eq': 0 } } },
      {
        '$lookup': {
          'from': 'frames',
          'localField': '_id',
          'foreignField': 'user',
          'as': 'frame'
        }
      },
      { '$unwind': { 'path': '$frame' } },
      {
        '$lookup': {
          'from': 'pointers',
          'localField': 'frame.shards',
          'foreignField': '_id',
          'as': 'pointer'
        }
      },
      { '$unwind': { 'path': '$pointer' } },
      {
        '$lookup': {
          'from': 'shards',
          'localField': 'pointer.hash',
          'foreignField': 'hash',
          'as': 'shards'
        }
      },
      { '$unwind': { 'path': '$shards' } },
      { '$project': { '_id': '$shards.hash', 'total': { '$size': '$shards.contracts' } } },
      { '$match': { 'total': { '$lt': 6 } } },
      { '$sample': { 'size': 50 } }
    ];

    const premiumMirrorsQuery = [
      { '$match': { 'maxSpaceBytes': { '$gt': 0 } } },
      {
        '$lookup': {
          'from': 'frames',
          'localField': '_id',
          'foreignField': 'user',
          'as': 'frame'
        }
      },
      { '$unwind': { 'path': '$frame' } },
      {
        '$lookup': {
          'from': 'pointers',
          'localField': 'frame.shards',
          'foreignField': '_id',
          'as': 'pointer'
        }
      },
      { '$unwind': { 'path': '$pointer' } },
      {
        '$lookup': {
          'from': 'shards',
          'localField': 'pointer.hash',
          'foreignField': 'hash',
          'as': 'shards'
        }
      },
      { '$unwind': { 'path': '$shards' } },
      { '$project': { '_id': '$shards.hash', 'total': { '$size': '$shards.contracts' } } },
      { '$match': { 'total': { '$lt': 6 } } },
      { '$sample': { 'size': 50 } }
    ];

    const recentMirrorsCreated = [
      {
        $match: {
          'created': {
            '$gte': new Date(new Date() - ms('1d')),
            '$lt': new Date(new Date() - ms('15m'))
          }
        }
      },
      {
        '$group': {
          '_id': '$shardHash',
          'm': { '$push': '$$ROOT' }
        }
      },
      {
        '$project': {
          '_id': '$_id',
          'hash': '$_id',
          'total': { '$size': '$m' },
          'established': {
            '$size': {
              '$filter': {
                'input': '$m',
                'as': 'm',
                'cond': { '$eq': ['$$m.isEstablished', true] }
              }
            }
          },
          'nonestablished': {
            '$size': {
              '$filter': {
                'input': '$m',
                'as': 'm',
                'cond': { '$eq': ['$$m.isEstablished', false] }
              }
            }
          },
          'created_min': { '$min': '$m.created' },
          'created_max': { '$max': '$m.created' }
        }
      },
      {
        '$match': {
          '$and': [
            { 'established': { '$lt': 6 } },
            { 'established': { '$gt': 0 } },
            { 'nonestablished': { '$gt': 0 } }
          ]
        }
      },
      { '$sort': { 'established': 1, 'created_min': 1 } },
      { '$sample': { 'size': 100 } }
    ];

    /**
     * Only effective if cleaner has done its job
     */
    async.parallel([
      next => {
        return utils.AggregationCursor(User, premiumMirrorsQuery, (data, nextData) => {
          log.info('Checking premium shard %s...', data._id);
          return Shard.findOne({ hash: data._id }, (err, shardModel) => {
            if (err || !shardModel) {
              log.error('Shard  %s not found for premium user', data._id);
              return nextData(null, data);
            }
            const shardObject = storj.StorageItem(shardModel.toObject());
            return this._replicateShard(shardObject, function (err) {
              if (err) { log.error('Unable to replicate premium shard %s, reason: %s', shardModel.hash, err.message); }
              nextData(null, data);
            });
          });
        }, next);
      },
      next => {
        return utils.AggregationCursor(User, mirrorsQuery, (data, nextData) => {
          log.info('Checking regular shard %s...', data._id);
          return Shard.findOne({ hash: data._id }, (err, shardModel) => {
            if (err) {
              log.error('Shard  %s not found for regular user', data._id);
            }
            const shardObject = storj.StorageItem(shardModel.toObject());
            return this._replicateShard(shardObject, function (err) {
              if (err) { log.error('Unable to replicate regular shard %s, reason: %s', shardModel.hash, err.message); }
              nextData(null, data);
            });
          });
        }, next);
      },
      next => {
        return utils.AggregationCursor(Mirror, recentMirrorsCreated, (data, nextData) => {
          log.info('Checking recent shard %s...', data._id);
          return Shard.findOne({ hash: data._id }, (err, shardModel) => {
            if (err) {
              log.error('Recent shard  %s not found for regular user', data._id);
            }
            const shardObject = storj.StorageItem(shardModel.toObject());
            return this._replicateShard(shardObject, function (err) {
              if (err) { log.error('Unable to replicate recent shard %s, reason: %s', shardModel.hash, err.message); }
              nextData(null, data);
            });
          });
        }, next);
      }
    ], (err) => {
      logger.info('Parallel finished');
      this.finish(err);
    });
  }

  _replicateShard(shard, callback) {
    async.parallel({
      destinations: (next) => {
        this._fetchDestinations(shard, next);
      },
      sources: (next) => {
        this._fetchSources(shard, next);
      }
    }, (err, state) => {
      if (err) {
        return callback(err);
      }

      this._transferShard(shard, state, callback);
    });
  }

  _fetchDestinations(shard, callback) {
    this.storage.models.Mirror
      .find({ shardHash: shard.hash })
      .populate('contact')
      .exec((err, results) => {
        if (err) {
          return callback(err);
        }
        const mirrors = results.filter((m) => {
          if (!m.contact) {
            log.warn('Mirror %s is missing contact in database', m._id);
            return false;
          } else if (shard.contracts[m.contact._id]) {
            //log.warn('Shard %s already established to contact %s', shard.hash, m.contact._id);
            return false;
          } else if (!m.isEstablished) {
            return true;
          }
          return false;
        });
        mirrors.sort(this.sortByTimeoutRate);
        callback(null, mirrors);
      });
  }

  _fetchSources(shard, callback) {
    let farmers = Object.keys(shard.contracts);

    this.storage.models.Contact
      .find({ _id: { $in: farmers } })
      .sort({ lastSeen: -1 })
      .exec((err, results) => {
        if (err) {
          return callback(err);
        }

        let contacts = [];
        for (let i = 0; i < results.length; i++) {
          let c = results[i];
          let contact = null;
          try {
            contact = storj.Contact(c.toObject());
          } catch (e) {
            log.warn('Unable to fetch source, invalid contact: %j', c.toObject());
          }
          if (contact) {
            contacts.push(contact);
          }
        }

        callback(null, contacts);
      });
  }

  sortByTimeoutRate(a, b) {
    const a1 = a.contact.timeoutRate >= 0 ? a.contact.timeoutRate : 0;
    const b1 = b.contact.timeoutRate >= 0 ? b.contact.timeoutRate : 0;
    return (a1 === b1) ? 0 : (a1 > b1) ? 1 : -1;
  }

  _transferShard(shard, state, callback) {
    const source = state.sources[0];
    const destination = state.destinations[0];

    if (!source) {
      return callback(new Error('Sources exhausted'));
    }

    if (!destination) {
      if (this.actions) {
        this.actions.publishNewContractsForShard(shard.hash, (err) => {
          if (err) {
            log.warn('Error publishing new contracts for shard %s, reason: %s', shard.hash, err.message);
          } else {
            log.info('New contracts for shard %s published successfully', shard.hash);
          }
        });
      }
      return callback(new Error('Destinations exhausted'));
    }

    let contract = null;
    try {
      contract = shard.getContract(source);
    } catch (e) {
      log.warn('Unable to transfer shard, invalid contract: %j',
        destination.contract);
      state.destinations.shift();
      this._transferShard(shard, state, callback);
      return;
    }

    this.network.getRetrievalPointer(source, contract, (err, pointer) => {
      if (err || !pointer) {
        log.warn('Failed to get retrieval pointer from farmer %s, reason: %s',
          source, err ? err.message : null);

        state.sources.shift();
        this._transferShard(shard, state, callback);
        return;
      }

      const farmer = storj.Contact(destination.contact);

      this.network.getMirrorNodes([pointer], [farmer], (err) => {
        if (err) {
          log.warn('Unable to mirror to farmer %s, reason: %s', destination.contact.nodeID, err.message);
          state.destinations.shift();
          this._transferShard(shard, state, callback);
          return;
        }

        this._saveShard(shard, destination, callback);
      });
    });
  }

  _saveShard(shard, destination, callback) {

    const contract = storj.Contract(destination.contract);
    const contact = storj.Contact(destination.contact);
    shard.addContract(contact, contract);

    this.contracts.save(shard, (err) => {
      if (err) {
        return callback(new Error('Unable to save contract to shard'));
      }

      log.info('Successfully replicated shard %s', shard.hash);
      destination.isEstablished = true;
      destination.save((err) => {
        if (err) {
          return callback(
            new Error('Unable to update mirror as established, reason: ' +
              err.message)
          );
        }
        callback();
      });
    });
  }

  wait() {
    clearTimeout(this._timeout);

    const max = ms(this._config.application.maxInterval);
    const min = ms(this._config.application.minInterval);

    const milliseconds = utils.randomTime(max, min);
    const minutes = Number(milliseconds / 1000 / 60).toFixed(2);

    log.info('Scheduling next round in %s minutes', minutes);

    this._timeout = setTimeout(() => this.run(), milliseconds);
  }

  _handleUncaughtException(err) {
    if (process.env.NODE_ENV === 'test') {
      throw err;
    }

    log.error('An unhandled exception occurred:', err);
    process.exit(1);
  }

  _handleExit() {
    log.info('Farmer monitor service is shutting down');
  }

  _handleSIGINT() {
    let waitTime = 0;

    log.info('Received shutdown signal, checking for running monitor');
    setInterval(function () {
      waitTime += SIGINT_CHECK_INTERVAL;

      if (!this._running) {
        process.exit();
      }

      if (waitTime > MAX_SIGINT_WAIT) {
        process.exit();
      }
    }, SIGINT_CHECK_INTERVAL);
  }
}

module.exports = Replicator;