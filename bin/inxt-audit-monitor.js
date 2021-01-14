const Config = require('../lib/config');
const program = require('commander');
// const Storage = require('storj-service-storage-models');
// const logger = require('../lib/logger');

const Audit = require('../lib/audit');

program
  .version('0.0.1')
  .option('-w, --wallet <wallet_hash>', 'hash of the payment wallet whose nodes to be audited')
  .option('-n, --nodeId <node_id>', 'id of the node to be audited')
  .option('-s, --shardId <node_id>', 'id of the shard to be audited')
  .option('-c, --config <path_to_config_file>', 'path to the config file')
  .option('-a, --attempts <attempts_to_retry>', 'number of attempts to audit the shard (sometimes nodes fail to send the shard)')
  .parse(process.argv);


/* SETUP */
const config = new Config(process.env.NODE_ENV || 'develop', program.config, program.datadir);

const audit = new Audit(config, program.attempts);
audit.init();

// Audit a wallet
if(program.wallet) {
  audit.wallet(program.wallet).catch(console.log);
  return;
}


if(program.nodeId) {
  const attempts = program.attempts && !isNaN(program.attempts) ? program.attempts : 1;

  if(program.shardId) {
    // Audit a shard
    audit.shard(program.shardId, program.nodeId, attempts).catch(console.log);
    return;
  } else {
    // Audit a node
    audit.node(program.nodeId).catch(console.log);
    return;
  }
}

console.log('please provide a valid option');

module.exports = audit;