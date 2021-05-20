'use strict';

const Router = require('./index');
class HealthRouter extends Router {
  constructor(options) {
    super(options);
  }

  health(req, res) {
    if (this.storage.connection.readyState === 1) {
      return res.status(200).send('OK');
    }

    res.status(503).send('Service Unavailable');
  }

  _definitions() {
    return [
      ['GET', '/health', this.health]
    ];
  }
}

module.exports = HealthRouter;
