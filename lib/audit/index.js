const Storage = require('storj-service-storage-models');
const http = require('http');
const crypto = require('crypto');
const storj = require('storj-lib');
const ComplexClient = require('storj-complex').createClient;
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const log = require('../logger');
const AuditService = require('./service');

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
    this._start = 0;
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

    this.service = new AuditService({ storage: this.storage });

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

  retrieveShardToken(contact, contract) {
    return new Promise((resolve, reject) => {
      this.network.getRetrievalPointer(contact, contract, async (err, pointer) => {
        if(err || !pointer || !pointer.token) reject(err);
        else resolve(pointer.token);
      });
    });
  }


  isShardAvailableForNode({ nodeId, shardId }) {

    if(this._success) this._success = false;

    return new Promise((resolve, reject) => {
      if(!shardId || !nodeId) {
        log.warn('No shardId or nodeId provided!');
      }
  
      Promise.all([
        this.getShardByIdRelatedToNode({ nodeId, shardId }),
        this.getContactByNodeId({ nodeId })
      ]).then(async (response) => {
        let [shard, contact] = response;

        if(!shard) {
          log.warn('shard %s not found for node %s', shardId, nodeId);
          resolve(false);
        } 

        if(!contact) {
          log.warn('contact for shard %s not found', shardId);
          resolve(false);
        }
    
        contact = storj.Contact(contact);
        let contractData;

        for(let i = 0; i < shard.contracts.length; i++) {
          if(shard.contracts[i].nodeID == nodeId) {
            contractData = shard.contracts[i];
            break;
          }
        }
    
        if(!contractData || !contractData.contract) {
          log.error('contract not found node %s shard %s', nodeId, shard.hash);
          return;
        }
    
        const contract = storj.Contract.fromObject(contractData.contract);

        const token = await this.retrieveShardToken(contact, contract);

        const {status: isHealthy, msg} = await this.isShardHealthy(contact, contract, token, shard.hash);
        log.info(msg);
        this._success = isHealthy;
        resolve(this._success);
      }).catch(reject);
      
    });
  }

  measureTime () {
    return process.hrtime(this._start)[0];
  }

  async isShardHealthy (contact, contract, token, shardHash) {
    const createResponse = (status, msg) => ({ status, msg });

    const { size, hasher } = await requestShard(contact, token, shardHash);
    const shardDownloadedHash = storj.utils.rmd160b(hasher.digest()).toString('hex');
    log.info(`Shard downloaded hash is ${shardDownloadedHash}, shard hash from database is ${shardHash}`);

    if(shardDownloadedHash !== shardHash) {
      return createResponse(false, `Hashes do not match, hash from file downloaded: ${shardDownloadedHash}, but expected: ${shardHash}`);
    
    } else if(size !== contract.get('data_size')) {
      return createResponse(false, `Shard ${contract.get('data_size')} wrong size, current: ${size}`);
    
    } else {
      return createResponse(true, `Shard ${shardHash} susccesfully downloaded!`);
    }
  }

  finish(nodeId, shardId) {
    if(this._attempts < this._attemptsCounter && !this._success) {
      this._attemptsCounter++;
      this.start(nodeId, shardId);
    } else {
      log.info(`Audit finished: ${this._attemptsCounter + 1} attempt(s), result: ${this._success ? 'successful' : 'not succesful' }`);
    }
  }

  async areShardsAvailableForNode({ nodeId }) {
    this._start = process.hrtime();

    const sample = 100;
    let shardsList = [];
    shardsList = await this.service.getShardsSampleByNodeId({ nodeId, sample });

    let sanityCounter = 0;

    log.info(shardsList.length);

    await Promise.all(shardsList.map(async (shard) => {
      log.info('Retrieving shard');
      let currentContact = await this.service.getContactById({ id: shard.contact });
      currentContact = storj.Contact(currentContact);
      let currentContract = storj.Contract(shard.contract);

      try {
        const token = await this.retrieveShardToken(currentContact, currentContract);
        const { status: isHealthy } = await this.isShardHealthy(currentContact, currentContract, token, shard.shardHash).catch(console.log);
        console.log(`Shard with hash ${shard.shardHash} token ${token} ${isHealthy ? 'is healthy' : 'is not healthy'}`);
        if(isHealthy) {
          log.info(`Shard with hash ${shard.shardHash} token ${token} is healthy`);
          sanityCounter++;
        } else {
          log.warn(`Shard with hash ${shard.shardHash} token ${token} is not healthy`);
        }
      } catch (e) {
        log.warn(e);
      }
    }));

    const healthyPercentage = ((sanityCounter/sample) * 100).toFixed(2);
    const timeElapsed = this.measureTime();

    log.info(`Audit finished in ${timeElapsed}s`);
    log.info(`${sanityCounter} of ${sample} (${healthyPercentage}%) shards checked are healthy`);
  }
  
}

module.exports = Audit;