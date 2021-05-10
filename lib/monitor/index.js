'use strict';

const async = require('async');
const assert = require('assert');
const storj = require('storj-lib');
const MonitorConfig = require('./config');
const Storage = require('storj-service-storage-models');
const ComplexClient = require('storj-complex').createClient;
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const ms = require('ms');
const log = require('../logger');
const errors = require('storj-service-error-types');
const constants = require('../constants');
const utils = require('../utils');
const Actions = require('../actions');

/**
 * A long running daemon that will monitor farmers uptime and will replace
 * contracts associated with a farmer once the farmer is confirmed to be
 * offline for a duration of time.
 * @param {MonitorConfig} config - An instance of MonitorConfig
 */

class Monitor {
  constructor(config) {
    assert(config instanceof MonitorConfig, 'Invalid config supplied');

    this.storage = null;
    this.network = null;
    this.contracts = null;

    this.actions = null;

    this._config = config;
    this._timeout = null;
    this._running = false;
  }

  init() {
    this.storage = new Storage(this._config.storage.mongoUrl, this._config.storage.mongoOpts, { logger: log });
    this.network = new ComplexClient(this._config.complex);
    this.contracts = new storj.StorageManager(new MongoDBStorageAdapter(this.storage), { disableReaper: true });
    this.actions = new Actions(this._config, this.storage, this.network, this.contracts);
  }

  setStorage(storage) {
    this.storage = storage;
    this.network = new ComplexClient(this._config.complex);
    this.contracts = new storj.StorageManager(new MongoDBStorageAdapter(this.storage), { disableReaper: true });
    this.actions = new Actions(this._config, this.storage, this.network, this.contracts);
  }

  /**
   * Starts the Bridge instance
   * @param {Function} callback
   */
  start(callback) {
    log.info('Monitor service is starting');

    this.init();

    // setup next run event
    this.wait();

    callback();
    process.on('SIGINT', this._handleSIGINT.bind(this));
    process.on('exit', this._handleExit.bind(this));
    process.on('uncaughtException', this._handleUncaughtException.bind(this));
  }

  static sortByTimeoutRate(a, b) {
    const a1 = a.contact.timeoutRate >= 0 ? a.contact.timeoutRate : 0;
    const b1 = b.contact.timeoutRate >= 0 ? b.contact.timeoutRate : 0;

    return (a1 === b1) ? 0 : (a1 > b1) ? 1 : -1;
  }

  async _removeFarmerFromShard(hash, nodeID) {
    const { Mirror, Shard } = this.storage.models;

    return async.waterfall([
      (next) => Shard.update({ hash: hash }, {
        $pull: {
          'challenges': { nodeID: nodeID },
          'trees': { nodeID: nodeID },
          'contracts': { nodeID: nodeID }
        }
      }).then(() => next()).catch(next),
      (next) => Mirror.deleteOne({ shardHash: hash, contact: nodeID }).then(() => next()).catch(next)
    ]);
  }

  /**
   * Gets a list of posible contacts (farmers) available to transfer the shard.
   * TODO: Fit unit tests
   */
  _fetchDestinations(shard, callback) {
    const { Mirror } = this.storage.models;
    Mirror
      .find({
        shardHash: shard.hash,
        isEstablished: false
      })
      .populate('contact')
      .exec((err, results) => {
        if (err) {
          return callback(err);
        }
        const mirrors = results.filter((m) => {
          if (!m.contact) {
            // log.warn('Mirror %s is missing contact in database, removed mirror', m._id);
            this._removeFarmerFromShard(m.shardHash, m.contract.renter_id);
            Mirror.deleteMany({ shardHash: m.shardHash, contact: m.contract.renter_id });

            return false;
          } else if (shard.contracts[m.contact._id]) {
            Mirror.update(
              { shardHash: m.shardHash, contact: m.contract.renter_id },
              { isEstablished: true }
            ).then(result => {
              if (result.nModified === 0) {
                // log.error('Mirror for shard %s from contact %s does not exists. Create?', m.shardHash, m.contract.renter_id);
              }
            });

            // log.warn('Shard %s already established to contact %s, set isEstablished true', shard.hash, m.contact._id);

            return false;
          }

          return true;
        });

        mirrors.sort(Monitor.sortByTimeoutRate);

        callback(null, mirrors);
      });
  }

  /**
   * Gets a list of farmers who owns the given shard.
   */
  _fetchSources(shard, callback) {
    const { Contact } = this.storage.models;
    let farmers = Object.keys(shard.contracts);

    Contact
      .find({ _id: { $in: farmers } })
      .sort({ lastSeen: -1 })
      .exec((err, results) => {
        if (err) {
          return callback(err);
        }

        let contacts = [];
        for (let c of results) {
          let contact = null;
          try {
            contact = storj.Contact(c.toObject());
          } catch (e) {
            console.log(e);
            log.warn('Unable to fetch source, invalid contact: %j', c.toObject());
          }
          if (contact) {
            contacts.push(contact);
          } else {
            console.log('No contact', contact);
          }
        }

        callback(null, contacts);
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

      destination.isEstablished = true;
      destination.save((err) => {
        if (err) {
          return callback(Error('Unable to update mirror as established, reason: ' + err.message));
        }

        const totalDestinations = Object.keys(shard.contracts).length;

        if (totalDestinations + 1 < constants.M_REPLICATE) {
          log.warn('Shard %s needs more mirrors', shard.hash);
          this._replicateShard(shard, callback);
        } else {
          callback();
        }
      });
    });
  }

  _transferShard(shard, state, callback) {
    const source = state.sources[0];
    const destination = state.destinations[0];

    if (!source) {
      return callback(new Error('Sources exhausted'));
    }

    if (!destination) {
      // TODO: Publish new contracts and retry
      return callback(new Error('Destinations exhausted'));
    }

    let contract = null;
    try {
      contract = shard.getContract(source);
    } catch (e) {
      log.warn('Unable to transfer shard, invalid contract: %j', destination.contract);
      state.destinations.shift();
      this._transferShard(shard, state, callback);

      return;
    }

    this.network.getRetrievalPointer(source, contract, (err, pointer) => {
      if (err || !pointer) {
        if (err.message === 'Shard data not found') {
          // TODO: Farmer is not holding this mirror anymore. Delete from database and penalize farmer.
        }
        log.warn('Failed to get retrieval pointer from farmer %s, reason: %s',
          source, err ? err.message : null);

        state.sources.shift();

        return this._transferShard(shard, state, callback);
      }

      const farmer = storj.Contact(destination.contact);

      this.network.getMirrorNodes([pointer], [farmer], (err) => {
        if (err) {
          log.warn('Unable to mirror to farmer %s, reason: %s', destination.contact.nodeID, err.message);
          state.destinations.shift();

          return this._transferShard(shard, state, callback);
        }

        this._saveShard(shard, destination, callback);
      });
    });
  }

  /**
   * Gets a list of sources and a list of destinations of a given shard.
   */
  _replicateShard(shard, callback, retry = false) {
    async.parallel({
      destinations: (next) => this._fetchDestinations(shard, next),
      sources: (next) => this._fetchSources(shard, next)
    }, (err, state) => {
      if (err) {
        return callback(err);
      }

      if (state.sources.length === 0) {
        return callback(Error('Fatal error: Missing shard sources'));
      }

      if (state.destinations.length === 0) {
        if (retry) {
          console.error('Not enough destinations for %s, cannot be created cannot retry', shard.hash);

          return this._transferShard(shard, state, callback);
        }
        // No new farmers can hold this mirror, create new contracts and retry
        console.error('%s sources, %s destinations, creating new contracts for %s', state.sources.length, state.destinations.length, shard.hash);

        return this.actions.publishNewContractsForShard(shard.hash, () => {
          console.log('Successfully created new contracts for %s', shard.hash);
          setTimeout(() => this._replicateShard(shard, callback, true), 10000);
        });
      }

      if (state.destinations.length < 15) {
        // Not enough destinations for future mirrors, create news and continue with existent
        this.actions.publishNewContractsForShard(shard.hash, () => {
          console.log('Successfully created new contracts for %s', shard.hash);
        });

        return this._transferShard(shard, state, callback);
      }

      if (state.sources.length + 1 >= constants.M_REPLICATE) {
        // log.info('Shard %s has enough source mirrors, replication not needed', shard.hash);
        return callback();
      }

      this._transferShard(shard, state, callback);
    });
  }

  _removeFarmer(farmer) {
    log.warn('Removing farmer %s', farmer);
    const { Mirror } = this.storage.models;

    return async.waterfall([
      (next) => Mirror.deleteMany({ contact: farmer }).then(() => next()).catch(next)
    ]);
  }

  _replicateFarmer(contact, callback) {
    log.info('Starting to replicate farmer %s ...', contact.nodeID);
    const replicateInterval = setInterval(() => log.warn('Still replicating farmer %s', contact.nodeID), 30000);

    const { Shard } = this.storage.models;
    // Search all shard from the offline farmer
    const query = [{ '$match': { 'contracts.nodeID': contact.nodeID } }];

    utils.AggregationCursor(Shard, query, async (data, nextData) => {
      const shardModel = await Shard.findOne({ _id: data._id });
      const shard = storj.StorageItem(shardModel.toObject());

      // log.info('Replicating shard %s for farmer %s ...', shard.hash, contact.nodeID);

      this._replicateShard(shard, (err) => {
        if (err) {
          log.error('Unable to replicate shard %s, reason: %s', shard.hash, err.message);
        } else {
          log.info('Succesfully replicated shard %s', shard.hash);
        }

        this._removeFarmerFromShard(shard.hash, contact.nodeID).then(() => nextData(null, data));
      });
    }, (err) => {
      if (err) {
        log.error('Unable to replicate farmer %s, reason: %s', contact.nodeID, err.message);
      } else {
        log.warn('Ending replication of farmer %s', contact.nodeID);
      }

      clearInterval(replicateInterval);

      const removeInterval = setInterval(() => log.warn('Still removing farmer %s', contact.nodeID), 30000);

      this._removeFarmer(contact.nodeID).finally(() => {
        clearInterval(removeInterval);
        if (callback) {
          callback(err);
        }
      });
    });
  }

  run() {
    // If a check round is being executed, wait.
    if (this._running) {
      return this.wait();
    }

    const { Contact } = this.storage.models;

    const limit = this._config.application.queryNumber || 10;
    const pingConcurrency = this._config.application.pingConcurrency || 10;
    const timeoutRateThreshold = this._config.application.timeoutRateThreshold;

    const finish = (err) => {
      if (err) {
        log.error('Monitor finish error: ' + err);
      }
      log.info('Ending farmer monitor round');

      this._running = false;
      this.wait();
    };

    log.info('Starting farmer monitor round for %s contacts less than %s', limit, timeoutRateThreshold);
    this._running = true;

    // Query the least seen contacts with timeout rates below threshold
    const query = {
      $and: [
        {
          _id: { $nin: constants.XNODES }
        },
        {
          $or: [
            { timeoutRate: { $lt: timeoutRateThreshold } },
            { timeoutRate: { $exists: false } },
            // { $and: [{ timeoutRate: { $gte: timeoutRateThreshold } }, { $expr: { $gt: ['$lastSeen', '$lastTimeout'] } }] }
          ]
        }
      ]
    };

    const cursor = Contact.find(query).limit(limit).sort({ lastSeen: 1 });

    cursor.exec((err, contacts) => {
      if (err) {
        return finish(err);
      }

      if (!contacts) {
        return finish(new errors.InternalError('No contacts in contacts collection'));
      }

      log.info('Total farmers found to scan: %s', contacts.length);

      // Ping the least seen contacts
      async.eachLimit(contacts, pingConcurrency, (contactData, next) => {
        log.info('PING farmer %s ... (current timeoutRate: %s)', contactData.nodeID, contactData.timeoutRate);

        const contact = storj.Contact(contactData);

        this.network.ping(contact, (err) => {
          if (err) {
            log.error('Farmer %s failed ping, reason: %s', contact.nodeID, err.message);

            contactData.recordTimeoutFailure().save((err) => {
              if (err) {
                log.error('Unable to save ping failure, farmer: %s, reason: %s', contact.nodeID, err.message);
              }

              Contact.findOne({ _id: contact.nodeID }).then(contact2 => {
                if (contact2.timeoutRate >= timeoutRateThreshold) {
                  if (contact2.lastSeen < contact2.lastTimeout) {
                    log.warn('Shards from farmer %s must be replicated, timeoutRate: %s, reputation: %s', contact2.nodeID, contact2.timeoutRate, contact2.reputation);
                    const START = new Date();
                    this._replicateFarmer(contact, () => {
                      const END = new Date();
                      console.log(contact.nodeID, START, END, END - START);
                      next();
                    });
                  } else {
                    log.warn('Last seen updated for %s', contact2._id);
                    // Give a new round opportunity to the farmer
                    contact2.lastTimeout = contact2.lastSeen;
                    contact2.timeoutRate = timeoutRateThreshold - 0.01;
                    contact2.save();
                    next();
                  }
                } else {
                  log.info('PING to %s not OK, but below thershold (%s)', contact.nodeID, contactData.timeoutRate);
                  next();
                }
              }).catch(err => {
                log.error('Error finding contact to replicate');
                log.error(err);
                next();
              });

            });

          } else {
            log.info('PING OK: Farmer %s is online.', contact.nodeID);
            contactData.lastTimeout = new Date();
            if (contactData.timeoutRate >= timeoutRateThreshold) {
              // Contact came back to the network
              contactData.timeoutRate = 0;
            }
            contactData.save();
            next();
          }
        });
      }, finish);
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
      waitTime += Monitor.SIGINT_CHECK_INTERVAL;

      if (!this._running) {
        process.exit();
      }

      if (waitTime > Monitor.MAX_SIGINT_WAIT) {
        process.exit();
      }
    }, Monitor.SIGINT_CHECK_INTERVAL);
  }
}

Monitor.SIGINT_CHECK_INTERVAL = 1000;
Monitor.MAX_SIGINT_WAIT = 5000;



module.exports = Monitor;
