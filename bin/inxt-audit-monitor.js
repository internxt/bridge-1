const Config = require('../lib/config');
const program = require('commander');
const log = require('../lib/logger');

const Audit = require('../lib/audit');

program
  .version('0.0.1')
  .option('-w, --wallet <wallet_hash>', 'hash of the payment wallet whose nodes to be audited')
  .option('-n, --nodeId <node_id>', 'id of the node to be audited')
  .option('-h, --shardHash <node_id>', 'id of the shard to be audited')
  .option('-f, --fileId <file_id>', 'id of the file whose shards are going to be audited')
  .option('-c, --config <path_to_config_file>', 'path to the config file')
  .option('-a, --attempts <attempts_to_retry>', 'number of attempts to audit the shard (sometimes nodes fail to send the shard)')
  .parse(process.argv);


/* SETUP */
const config = new Config(process.env.NODE_ENV || 'develop', program.config, program.datadir);

async function startMonitor() {
  const audit = new Audit(config, program.attempts);
  audit.init();

  let exitCode = 0;

  try {
    if (program.wallet) {
      await audit.wallet(program.wallet);
      return;
    }

    if(program.fileId) {
      await audit.file(program.fileId, 3);
      return;
    }  

    if(program.nodeId) {
      const attempts = program.attempts && !isNaN(program.attempts) ? program.attempts : 1;

      if(program.shardHash) {
        await audit.shardInNode(program.shardHash, program.nodeId, attempts);
        return;
      } else {
        await audit.node(program.nodeId);
        return;
      }
    }

    if (program.shardHash) {
      await audit.shard(program.shardHash, 3);
      return;
    }

    log.warn('Wrong usage, use --help flag to see available options');
  } catch (err) {
    log.error('Unexpected error during audit');
    console.error(err);

    exitCode = -1;
  } finally {
    process.exit(exitCode);
  }
}

startMonitor();