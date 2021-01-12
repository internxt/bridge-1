const Storage = require('storj-service-storage-models');
const http = require('http');
const crypto = require('crypto');
const storj = require('storj-lib');
const ComplexClient = require('storj-complex').createClient;
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const log = require('../logger');

const SHARD_SOCKET_TIMEOUT = 90 * 1000; // milliseconds

function requestShard(contact, token, hash) {
  return new Promise((resolve, reject) => {
    const hasher = crypto.createHash('sha256');
    let size = 0;
    const request = http.get({
      protocol: 'http:',
      hostname: contact.address,
      port: contact.port,
      path: `/shards/${hash}?token=${token}`,
      timeout: SHARD_SOCKET_TIMEOUT,
      headers: {
        'content-type': 'application/octet-stream',
        'x-storj-node-id': contact.nodeID
      }
    }, (res) => {
      const hasResponseFailed = res.statusCode !== 200;

      if(hasResponseFailed) {
        reject(`Request to ${res.url} failed with HTTP ${res.statusCode}`);
      }

      res.on('data', chunk => {
        size += chunk.length;
        hasher.update(chunk);
      });
      res.on('end', () => resolve({size, hasher}));

    });

    request.on('error', reject);
    request.end();
  });
}

class Audit {
  constructor(config, attempts) {
    this.storage = null;
    this._config = config;
    this._attempts = attempts ? attempts : 1;
    this._attemptsCounter = 0;
    this._success = false;
    this.initialized = false;
  }

  init() {
    if(this.initialized) {
      return log.warn('Audit already initialized');
    }

    this.storage = new Storage(
      this._config.storage.mongoUrl,
      this._config.storage.mongoOpts,
      { logger: log }
    );

    this.network = new ComplexClient(this._config.complex);

    this.contracts = new storj.StorageManager(
      new MongoDBStorageAdapter(this.storage),
      { disableReaper: true }
    );

    this.initialized = true;
  }

  async start(nodeId, shardId){
    if(!this.initialized) {
      throw new Error('Audit not initialized');
    }

    try {
      await this.isShardAvailableForNode({ nodeId, shardId });
    } catch (err) {
      log.warn('Error while checking if shard is available:' + err.message);
    } finally {
      this.finish(nodeId, shardId);
    }
  }

  getShardByIdRelatedToNode({ nodeId, shardId }) {
    return new Promise((resolve, reject) => {
      const query = {
        $and : [ 
          { 'trees.nodeID': nodeId },
          { '_id': shardId },
        ]
      }; 

      this.storage.models.Shard.findOne(query, function (err, shard) {
        if(err) {
          log.error('There was an error querying a shard!', err);
          reject(err);
        }
        if(!shard || shard === null) {
          log.info('There is no shard %s for node %s', shardId, nodeId);
        } else {
          log.info(`Shard ${shardId} found!`);
        }
        resolve(shard);
      });
    });
  }

  getContactByNodeId({ nodeId }) {
    return new Promise((resolve, reject) => {
      this.storage.models.Contact.findOne({ '_id': nodeId }, function (err, contact) {
        if(err) {
          log.error('There was an error querying a contact!', err);
          reject(err);
        }
        if(!contact || contact === null) {
          log.info('There is no contact %s for node %s', nodeId);
        } else {
          log.info(`Contact for ${nodeId} found!`);
        }
        resolve(contact);
      });
    });
  }

  isShardAvailableForNode({ nodeId, shardId }) {

    return new Promise((resolve, reject) => {
      if(!shardId || !nodeId) {
        log.warn('No shardId or nodeId provided!');
      }
  
      Promise.all([
        this.getShardByIdRelatedToNode({ nodeId, shardId }),
        this.getContactByNodeId({ nodeId })
      ]).then((response) => {
        let [shard, contact] = response;

        if(!shard || shard === null) {
          log.warn('shard %s not found', shardId);
        }
    
        contact = storj.Contact(contact);
        const contractData = shard.contracts.filter((contract) => 
          contract.nodeID == nodeId
        )[0];
    
        if(!contractData || !contractData.contract) {
          log.error('contract not found node %s shard %s', nodeId, shard.hash);
          return;
        }
    
        const contract = storj.Contract.fromObject(contractData.contract);
    
        this.network.getRetrievalPointer(contact, contract, async (err, pointer) => {
          if(err || !pointer || !pointer.token) {
            throw new Error(`no token for node ${contact} shard ${shard.hash}`);
          }
          
          const {size, hasher} = await requestShard(contact, pointer.token, shard.hash);
    
          const shardDownloadedHash = storj.utils.rmd160b(hasher.digest()).toString('hex');
          log.info(`Shard downloaded hash is ${shardDownloadedHash}, shard hash from database is ${shard.hash}`);
    
          if(shardDownloadedHash !== shard.hash) {
            throw new Error(`Hashes do not match, hash from file downloaded: ${shardDownloadedHash}, but expected: ${shard.hash}`);
          } else if(size !== contract.get('data_size')) {
            throw new Error(`Shard ${contract.get('data_size')} wrong size, current: ${size}`);
          } else {
            this._success = true;
            log.info('Shard %s susccesfully downloaded!', shard.hash);
            resolve();
          }
        });
      }).catch(reject);
      
    });
  }

  finish(nodeId, shardId) {
    if(this._attempts < this._attemptsCounter && !this._success) {
      this._attemptsCounter++;
      this.start(nodeId, shardId);
    } else {
      log.info(`Audit finished: ${this._attemptsCounter + 1} attempt(s) | result: ${this._success ? 'successful' : 'not succesful' }`);
    }
  }

  
}

module.exports = Audit;

// mkdirp(fileDir, async function (err) {
//   if(err) {
//     log.warn('ERROR creating the directory');
//   }

//   log.debug('creating open file for shard %s', shard.hash);

//   const file = fs.createWriteStream('/dev/null');
//   file.on('close', function () {
//     log.debug('file closed for shard %s', shard.hash);
//   });

//   const {size, hasher} = await requestShard(contact, file, pointer.token, shard.hash);

//   const shardDownloadedHash = storj.utils.rmd160b(hasher.digest()).toString('hex');
//   console.log(size, contract.get('data_size'));
//   if(shardDownloadedHash !== shard.hash) {
//     log.error('Hashes do not match, hash from file downloaded: %s, but expected: %s', 
//       shardDownloadedHash, shard.hash
//     );
//     return false;
//   } else if(size !== contract.get('data_size')) {
//     log.error('Shard %s wrong size, current: %s', contract.get('data_size'), size);
//     return false;
//   } else {
//     log.info('Shard %s susccesfully downloaded! Audit finished.', shard.hash);
//     return true;
//   }
// });