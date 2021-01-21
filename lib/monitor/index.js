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
// const constants = require('../constants');
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
    log.info('Farmer monitor service is starting');

    this.init();

    // setup next run event
    this.wait();

    callback();
    process.on('SIGINT', this._handleSIGINT.bind(this));
    process.on('exit', this._handleExit.bind(this));
    process.on('uncaughtException', this._handleUncaughtException.bind(this));
  }

  sortByTimeoutRate(a, b) {
    const a1 = a.contact.timeoutRate >= 0 ? a.contact.timeoutRate : 0;
    const b1 = b.contact.timeoutRate >= 0 ? b.contact.timeoutRate : 0;
    return (a1 === b1) ? 0 : (a1 > b1) ? 1 : -1;
  }

  /**
 * Gets a list of posible contacts (farmers) available to transfer the shard.
 * TODO: Fit unit tests
 */
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
        mirrors.sort(Monitor.sortByTimeoutRate);
        callback(null, mirrors);
      });
  }

  /**
   * Gets a list of farmers who owns the given shard.
   */
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
            new Error('Unable to update mirror as established, reason: ' + err.message)
          );
        }
        callback();
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

  /**
   * Gets a list of sources and a list of destinations of a given shard.
   */
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

  _removeFarmer(farmer) {
    const { Shard, Mirror, Contact } = this.storage.models;

    async.parallel([
      (callback) => {
        Shard.update({
          $or: [
            { 'challenges.nodeID': farmer },
            { 'trees.nodeID': farmer },
            { 'contracts.nodeID': farmer }
          ]
        }, {
          $pull: {
            'challenges': { nodeID: farmer },
            'trees': { nodeID: farmer },
            'contracts': { nodeID: farmer }
          }
        }, (err, updated) => {
          if (err) {
            log.warn('Error cleaning up SHARDS from %s, reason: %s', farmer, err);
            callback(1, false);
          } else {
            log.info('Total %s SHARDS from %s removed from database', updated.ok, farmer);
            callback(null, true);
          }
        });

      },
      (callback) => {
        Mirror.deleteMany({ contact: farmer }, (err, total) => {
          if (err) {
            log.warn('Error cleaning up MIRRORS from %s, reason: %s', farmer, err);
            callback(1, false);
          } else {
            log.info('Total %s MIRRORS from %s removed from database', total.deletedCount, farmer);
            callback(null, true);
          }
        });

      }
    ], (err) => {

      if (err) {
        log.warn('Error cleaning up farmer info from database %s', farmer);
      } else {
        log.info('Entries from %s removed', farmer);

        Contact.deleteOne({ _id: farmer }, (err) => {
          if (err) {
            log.warn('Error removing contact %s, reason: %s', farmer, err);
          } else {
            log.info('Contact removed from database %s', farmer);
          }
        });

      }
    });

  }

  _replicateFarmer(contact, callback) {
    log.info('Starting to replicate farmer %s', contact.nodeID);

    const Shard = this.storage.models.Shard;
    // Search all shard from the offline farmer
    const query = {
      'contracts.nodeID': contact.nodeID
    };
    const cursor = Shard.find(query).cursor();

    cursor.on('error', (err) => {
      log.error('Unable to replicate farmer %s, reason: %s', contact.nodeID, err.message);
    }).on('data', (data) => {
      cursor.pause();
      const shard = storj.StorageItem(data.toObject());
      // log.info('Replicating shard %s for farmer %s', shard.hash, contact.nodeID);
      this._replicateShard(shard, (err) => {
        if (err) {
          log.error('Unable to replicate shard %s, reason: %s',
            shard.hash, err.message);
          cursor.resume();
        } else {
          Shard.update({ hash: shard.hash, 'contracts.nodeID': contact }, {
            $pull: {
              'challenges': { nodeID: contact },
              'trees': { nodeID: contact },
              'contracts': { nodeID: contact }
            }
          }, (err, updated) => {
            if (err) {
              log.error('Cannot delete contact %s from shard %s, reason: %s', contact, shard.hash, err.message);
            } else {
              log.info('Remove contact %s from shard %s', contact, shard.hash, updated);
            }
            cursor.resume();
          });
        }
      });
    }).on('end', () => {
      log.info('Ending replication of farmer %s', contact.nodeID);
      this._removeFarmer(contact.nodeID);

      if (callback) {
        callback();
      }
    }).on('close', () => {
      log.info('Replication cursor closed');
    });
  }

  run() {
    // If a check round is being executed, wait.
    if (this._running) {
      return this.wait();
    }

    let fail = 0;
    let success = 0;
    let total = 0;
    const limit = this._config.application.queryNumber || 10;
    const pingConcurrency = 1;
    const timeoutRateThreshold = this._config.application.timeoutRateThreshold;

    const finish = (err) => {
      if (err) {
        log.error('Monitor finish error: ' + err);
      }
      log.info('Ending farmer monitor round with failure rate of %s/%s from %s', fail, success, total);

      this._running = false;
      this.wait();
    };

    log.info('Starting farmer monitor round for %s contacts', limit);
    this._running = true;

    // Query the least seen contacts with timeout rates below threshold
    const Contact = this.storage.models.Contact;
    const query = {
      $or: [
        { timeoutRate: { $lt: timeoutRateThreshold } },
        { timeoutRate: { $exists: false } },
        { $and: [{ timeoutRate: { $gte: 0.04 } }, { $expr: { $gt: ['$lastSeen', '$lastTimeout'] } }] }
      ]
    };

    const cursor = Contact.find(query).limit(limit).sort({ lastSeen: 1 });

    cursor.exec((err, contacts) => {
      if (err) {
        return finish(err);
      }

      if (!contacts) {
        return finish(
          new errors.InternalError('No contacts in contacts collection')
        );
      }

      // Update total length of contacts
      total = contacts.length;
      log.info('Total farmers found to scan: %s', total);

      // Ping the least seen contacts
      async.eachLimit(contacts, pingConcurrency, (contactData, next) => {
        const contact = storj.Contact(contactData);

        // log.info('PING %s', contact.nodeID);
        this.network.ping(contact, (err) => {
          if (err) {
            fail += 1;
            log.error('Farmer %s failed ping, reason: %s', contact.nodeID, err.message);

            contactData.recordTimeoutFailure().save((err) => {
              if (err) {
                log.error('Unable to save ping failure, farmer: %s, reason: %s', contact.nodeID, err.message);
              }
            });

            if (contactData.timeoutRate >= timeoutRateThreshold) {
              log.warn('Shards from farmer %s must be replicated, timeoutRate: %s', contact.nodeID, contactData.timeoutRate);
              this._replicateFarmer(contact, () => {

              });
            } else {
              log.info('PING to %s not OK, but below thershold', contact.nodeID);
            }
          } else {
            log.info('PING to %s OK: Node is online', contact.nodeID);
            if (contactData.timeoutRate >= timeoutRateThreshold) {
              // Contact came back to the network
              contactData.timeoutRate = 0;
              contactData.save();
            }
            success += 1;
          }
          next();
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
