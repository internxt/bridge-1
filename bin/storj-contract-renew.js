#!/usr/bin/env node

'use strict';

const program = require('commander');
const Config = require('../lib/config');
const Renewal = require('../lib/renewal');
const logger = require('../lib/logger');

program.version(require('../package').version);
program.option('-c, --config <path_to_config_file>', 'path to the config file');
program.parse(process.argv);


// Parse config path
const config = new Config(process.env.NODE_ENV || 'develop', program.config, program.datadir);

// Renewal tools
const renewal = new Renewal(config);

// Initialize database and network bindings.
renewal.init();

const counter = { processed: 0, renewed: 0, errored: 0 }

setTimeout(() => {
  renewal.on('counter-processed', () => { counter.processed++ });
  renewal.on('counter-errored', () => { counter.errored++ });
  renewal.on('counter-renewed', ({ contact, contract }) => { logger.info('Renewed %s %s', contact.nodeID, contract.data_hash); counter.renewed++; });
  renewal.on('counter-errors', ({ contact, contract, error }) => logger.error('Error nodeID: %s, hash: %s, reason: %s', contact.nodeID, contract.data_hash, error.message))

  const log = setInterval(() => {
    logger.info('Contracts proccesed: %s, renewed: %s, errored: %s', counter.processed, counter.renewed, counter.errored);
  }, 15000)

  renewal.initContractRenew((err) => {
    console.log('Contract renew finished')
    clearInterval(log);
  });
}, 3000);
