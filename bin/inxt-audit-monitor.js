const Config = require('../lib/config');
const program = require('commander');
// const Storage = require('storj-service-storage-models');
// const logger = require('../lib/logger');

const Audit = require('../lib/audit');

program
  .version('0.0.1')
  .option('-n, --nodeId <node_id>', 'id of the node to be audited')
  .option('-s, --shardId <node_id>', 'id of the shard to be audited')
  .option('-c, --config <path_to_config_file>', 'path to the config file')
  .option('-o, --outputdir <path_to_outputdir>', 'path to where shards are saved')
  .option('-a, --attempts <attempts_to_retry>', 'number of attempts to audit the shard (sometimes nodes fail to send the shard)')
  .parse(process.argv);


/* SETUP */
const config = new Config(process.env.NODE_ENV || 'develop', program.config, program.datadir);

const audit = new Audit(config, program.attempts);
audit.init();
audit.start(program.nodeId, program.shardId)
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

module.exports = audit;