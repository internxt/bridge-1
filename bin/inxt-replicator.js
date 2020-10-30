#!/usr/bin/env node

'use strict';

const program = require('commander');
const Config = require('../lib/replicator/config');
const Replicator = require('../lib/replicator');

program.version(require('../package').version);
program.option('-c, --config <path_to_config_file>', 'path to the config file');
program.parse(process.argv);

var config = new Config(program.config);
var replicator = new Replicator(config);

replicator.init();

replicator.start(function (err) {
  if (err) {
    console.error(err);
    process.exit(1);
  }
});

module.exports = replicator;
