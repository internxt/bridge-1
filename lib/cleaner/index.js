'use strict';

const async = require('async');
const assert = require('assert');
const crypto = require('crypto');
const storj = require('storj-lib');
const CleanerConfig = require('./config');
const Storage = require('storj-service-storage-models');
const ComplexClient = require('storj-complex').createClient;
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const ms = require('ms');
const log = require('../logger');
const errors = require('storj-service-error-types');

/**
 * A long running daemon that will inspect database looking for orphan
 * documents and perform a clean up.
 * It can be performed once a day.
 * @param {CleanerConfig} config - An instance of CleanerConfig
 */
function Cleaner(config) {
  if (!(this instanceof Cleaner)) {
    return new Cleaner(config);
  }

  assert(config instanceof CleanerConfig, 'Invalid config supplied');

  this.storage = null;
  this.network = null;
  this.contracts = null;

  this._config = config;
  this._timeout = null;
  this._running = false;
  this._underAttack = false;
}

/**
 * Starts the Bridge instance
 * @param {Function} callback
 */
Cleaner.prototype.start = function (callback) {
  log.info('Cleaner service is starting');

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

  // setup next run event
  this.wait();

  callback();
  process.on('SIGINT', this._handleSIGINT.bind(this));
  process.on('exit', this._handleExit.bind(this));
  process.on('uncaughtException', this._handleUncaughtException.bind(this));
};

Cleaner.sortByTimeoutRate = function (a, b) {
  const a1 = a.contact.timeoutRate >= 0 ? a.contact.timeoutRate : 0;
  const b1 = b.contact.timeoutRate >= 0 ? b.contact.timeoutRate : 0;
  return (a1 === b1) ? 0 : (a1 > b1) ? 1 : -1;
};

Monitor.prototype.run = function () {

  // If a check round is being executed, wait.
  if (this._running) {
    return this.wait();
  }

  const limit = this._config.application.queryNumber || 10;

  const finish = (err) => {
    if (err) {
      log.error(err);
    }
    log.info('Ending cleaner round with failure rate of %s/%s from %s', fail, success, total);

    this._running = false;
    this.wait();
  };

  log.info('Starting clean up round');
  this._running = true;

  // Query the least seen contacts with timeout rates below threshold
  // Look for shards below the required number of mirrors


  const Contact = this.storage.models.Contact;
  const query = {
    $or: [
      { timeoutRate: { $lt: timeoutRateThreshold } },
      { timeoutRate: { $exists: false } }
    ]
  };

  return;

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
    console.log("Total farmers found to scan: %s", total);

    // Ping the least seen contacts
    async.eachLimit(contacts, pingConcurrency, (contactData, next) => {

      const contact = storj.Contact(contactData);


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
            this._replicateFarmer(contact);
          }

        } else {
          success += 1;
        }

        next();
      });

    }, finish);

  });

};

Monitor.prototype._randomTime = function (max, min) {
  const range = max - min;

  assert(Number.isSafeInteger(range));
  assert(range > 0, 'maxInterval is expected to be greater than minInterval');

  const entropy = crypto.randomBytes(8).toString('hex');
  const offset = Math.round(parseInt('0x' + entropy) / Math.pow(2, 64) * range);

  return min + offset;
};

/**
 * Will wait and then call `run` after a random amount of time
 */
Cleaner.prototype.wait = function () {
  clearTimeout(this._timeout);

  const max = ms(this._config.application.maxInterval);
  const min = ms(this._config.application.minInterval);

  const milliseconds = this._randomTime(max, min);
  const minutes = Number(milliseconds / 1000 / 60).toFixed(2);

  log.info('Scheduling next round in %s minutes', minutes);

  this._timeout = setTimeout(() => this.run(), milliseconds);
};

/**
 * Handles uncaught exceptions
 * @private
 */
/* istanbul ignore next */
Cleaner.prototype._handleUncaughtException = function (err) {
  if (process.env.NODE_ENV === 'test') {
    throw err;
  }

  log.error('An unhandled exception occurred:', err);
  process.exit(1);
};

/**
 * Handles exit event from process
 * @private
 */
/* istanbul ignore next */
Cleaner.prototype._handleExit = function () {
  log.info('Cleaner service is shutting down');
};

/**
 * Postpones process exit until requests are fullfilled
 * @private
 */
/* istanbul ignore next */
Cleaner.prototype._handleSIGINT = function () {
  let waitTime = 0;

  log.info('Received shutdown signal, checking for running cleaner');
  setInterval(function () {
    waitTime += Cleaner.SIGINT_CHECK_INTERVAL;

    if (!this._running) {
      process.exit();
    }

    if (waitTime > Cleaner.MAX_SIGINT_WAIT) {
      process.exit();
    }
  }, Cleaner.SIGINT_CHECK_INTERVAL);
};

module.exports = Cleaner;
