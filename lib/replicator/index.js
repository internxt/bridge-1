const Logger = require('../logger')
const assert = require('assert');
const ReplicatorConfig = require('./config');

class Replicator {
  constructor(config) {
    this.initialized = false;
    assert(config instanceof ReplicatorConfig, 'Invalid config supplied')
    this._config = config;
  }

  init() {
    if (this.initialized) {
      return Logger.warn('Replicator already initialized');
    }
  }

  start(callback) {
    callback()
    process.on('SIGINT', this._handleSIGINT.bind(this));
    process.on('exit', this._handleExit.bind(this));
    process.on('uncaughtException', this._handleUncaughtException.bind(this));
  }

  run() {

  }

  wait() {

  }

  _handleUncaughtException() {

  }

  _handleExit() {

  }

  _handleSIGINT() {

  }
}

module.exports = Replicator