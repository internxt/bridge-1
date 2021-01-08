#!/usr/bin/env node

'use strict';

const program = require('commander');
const Config = require('../lib/config');
const Renewal = require('../lib/renewal');
const logger = require('../lib/logger');
const _ = require('lodash');

program.version(require('../package').version);
program.option('-c, --config <path_to_config_file>', 'path to the config file');
program.parse(process.argv);


// Parse config path
const config = new Config(process.env.NODE_ENV || 'develop', program.config, program.datadir);

// Renewal tools
const renewal = new Renewal(config);

// Initialize database and network bindings.
renewal.init();

// Show progression data log
const counter = { processed: 0, renewed: 0, errored: 0, total: 0 };

// Capture events
renewal.on('counter-querysize', (size) => {
  counter.total = size;
  counter.errored = 0;
  counter.processed = 0;
  counter.renewed = 0;
});
renewal.on('counter-processed', () => {
  counter.processed++;
  logger.info('Contracts proccesed: %s, renewed: %s, errored: %s, total: %s, left: %s', counter.processed, counter.renewed, counter.errored, counter.total, counter.total - counter.processed);
});
renewal.on('counter-errored', () => { counter.errored++; });
renewal.on('counter-renewed', ({ contact, contract }) => { logger.info('Renewed %s %s', contact.nodeID, contract.data_hash); counter.renewed++; });
renewal.on('counter-errors', ({ contact, contract, error }) => {
  logger.error('Error nodeID: %s, hash: %s, reason: %s', contact.nodeID, contract.data_hash, error.message);
  switch (error.message) {
  case 'Invalid farmer contract':
    // TODO: Should check shard issues and clean database if needed
    break;
  default:
    if (error.message.includes('timed out')) {
      // console.log('TIMEOUT')
    } else {
      console.log('Not handled: %s', error.message);
    }
  }
});

renewal.on('end', () => { startRenewal(); });

function startRenewal() {
  renewal.initContractRenew((err) => {
    if (err) {
      console.log('Contract renew round finished with errors: %s', err.message);
    } else {
      console.log('Contract renew round finished');
    }
  });
}

startRenewal();
