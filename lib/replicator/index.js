const log = require('../logger')
const assert = require('assert');
const ReplicatorConfig = require('./config');
const ms = require('ms');
const crypto = require('crypto');

class Replicator {
  static SIGINT_CHECK_INTERVAL = 1000;
  static MAX_SIGINT_WAIT = 5000;

  constructor(config) {
    this.initialized = false;
    assert(config instanceof ReplicatorConfig, 'Invalid config supplied')
    this._config = config;
    this.storage = null;

    this._timeout = null;
    this._running = false;
  }

  init() {
    if (this.initialized) {
      return log.warn('Replicator already initialized');
    }

    // Initialize database
    this.storage = new Storage(
      this._config.storage.mongoUrl,
      this._config.storage.mongoOpts,
      { logger: log }
    );

    // Initialize renters
    this.network = new ComplexClient(this._config.complex);
  }

  start(callback) {
    this.wait();
    callback()
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

    // TODO: Put your logic here

    // Mock ending
    setTimeout(() => {
      this.finish();
    }, 10000);
  }

  _randomTime(max, min) {
    const range = max - min;

    assert(Number.isSafeInteger(range));
    assert(range > 0, 'maxInterval is expected to be greater than minInterval');

    const entropy = crypto.randomBytes(8).toString('hex');
    const offset = Math.round(parseInt('0x' + entropy) / Math.pow(2, 64) * range);

    return min + offset;
  }

  wait() {
    clearTimeout(this._timeout);

    const max = ms(this._config.application.maxInterval);
    const min = ms(this._config.application.minInterval);

    const milliseconds = this._randomTime(max, min);
    const minutes = Number(milliseconds / 1000 / 60).toFixed(2);

    log.info('Scheduling next round in %s minutes', minutes);

    this._timeout = setTimeout(() => this.run(), milliseconds);
  }

  _handleUncaughtException() {
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
      waitTime += Replicator.SIGINT_CHECK_INTERVAL;

      if (!this._running) {
        process.exit();
      }

      if (waitTime > Replicator.MAX_SIGINT_WAIT) {
        process.exit();
      }
    }, Replicator.SIGINT_CHECK_INTERVAL);
  }
}

module.exports = Replicator