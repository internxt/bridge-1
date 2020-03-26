#!/usr/bin/env node

'use strict';

const program = require('commander');
const Config = require('../lib/monitor/config');
const Cleaner = require('../lib/cleaner');

program.version(require('../package').version);
program.option('-c, --config <path_to_config_file>', 'path to the config file');
program.parse(process.argv);

var config = new Config(program.config);

var cleaner = new Cleaner(config);

cleaner.start(function(err) {
  if (err) {
    console.log(err);
  }
});

module.exports = cleaner;
