const Storage = require('storj-service-storage-models');
const Config = require('../lib/config');
const program = require('commander');
const logger = require('../lib/logger');
const os = require('os');
const path = require('path');
const fs = require('fs');

program
  .version('0.0.1')
  .option('-c, --config <path_to_config_file>', 'path to the config file')
  .parse(process.argv);

const config = new Config(process.env.NODE_ENV || 'develop', program.config,
  program.datadir);
const { mongoUrl, mongoOpts } = config.storage;
const storage = new Storage(mongoUrl, mongoOpts, { logger });

storage.models.Contact.distinct('_id', (err, contacts) => {
  const filename = path.join(__dirname, 'contacts.csv');
  contacts = Array.from(contacts, c => [c]);
  contacts = ['nodeID', ...contacts];
  fs.writeFileSync(filename, contacts.join(os.EOL));
  storage.connection.close();
});