/**
 * @module storj-bridge/logger
 */

'use strict';

const config = require('./config')(process.env.NODE_ENV);
const CONSTANTS = require('./constants');

const Winston = require('winston');

module.exports = (() => {

  var logger = Winston.createLogger(
      {
          level: 0,
          format: Winston.format.combine(
              Winston.format.colorize({ all: true }),
              Winston.format.timestamp({ format: 'YYYY-MM-DD HH:MM:SS' }),
              Winston.format.splat(),
              Winston.format.printf(info => {
                  return `${info.timestamp} ${info.level}: ${info.message}`;
              })
          ),
          transports: [
              new Winston.transports.Console()
          ]
      }
  )

  return logger;
})();
