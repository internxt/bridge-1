const log = require('../logger');
const crypto = require('crypto');
const http = require('http');

class AuditService {

  constructor({ storage }) {
    this._storage = storage;
  }

  getShardsSampleByNodeId({ nodeId, sample }) {
    return new Promise((resolve, reject) => {
      const query = {
        $and: [
          { 'contact': nodeId },
          { 'isEstablished': true }
        ]
      };
      this._storage.models.Mirror.find(query).limit(sample).exec(function (err, shards) {
        if(err) {
          log.error('There was an error querying the list of shards inside mirrors!', err);
          reject(err);
        }
        if(!shards || shards.length === 0) {
          log.info('There are no shards for node %s', nodeId);
        } else {
          log.info('Shards found!');
        }
        resolve(shards);
      });
    });
  }

  getShardsCountByNodeId({ nodeId }) {
    return new Promise((resolve, reject) => {
      const query = { 'contact': nodeId };  
      this._storage.models.Mirror.find(query).count().exec(function (err, count) {
        if(err) {
          log.error('There was an error querying the list of shards inside mirrors!', err);
          reject(err);
        }
        if(count == 0) {
          log.info('There are no shards for node %s', nodeId);
        } else {
          log.info('Shards found!');
        }
        resolve(count);
      });
    });
  }

  getContactById({ id }) {
    return new Promise((resolve, reject) => {
      const query = { '_id': id };  
      this._storage.models.Contact.findOne(query, function (err, contact) {
        if(err) {
          log.error('There was an error querying a contact!', err);
          reject(err);
        }
        if(!contact) {
          log.info('There are no contacts with id %s', id);
        } else {
          log.info('Contact found!');
        }
        resolve(contact);
      });
    });
  }

  getMirrorsByWallet({ wallet }) {
    const { Mirror } = this._storage.models;
    const query = {
      $and: [
        { 'isEstablished': true },
        { 'contract.payment_destination': wallet }
      ]
    };

    return Mirror.find(query).cursor();
  }

  getMirrorByContactAndShardHash ({ contact, hash }) {
    return new Promise((resolve, reject) => {
      const query = {
        $and: [
          { 'shardHash': hash },
          { 'isEstablished': true },
          { 'contact': contact }
        ]
      };
      this._storage.models.Mirror.findOne(query, function (err, mirror) {
        if(err) {
          log.error('There was an error querying a mirror!', err);
          reject(err);
        }
        if(mirror) {
          log.info('There is no mirror with hash %s', hash);
        } else {
          log.info('Mirror found!');
        }
        resolve(mirror);
      });
    });
  }

  getContactByNodeId({ nodeId }) {
    return new Promise((resolve, reject) => {
      this._storage.models.Contact.findOne({ '_id': nodeId }, function (err, contact) {
        if(err) {
          log.error('There was an error querying a contact!', err);
          reject(err);
        }
        if(!contact || contact === null) {
          log.info('There is no contact %s', nodeId);
        } else {
          log.info(`Contact for ${nodeId} found!`);
        }
        resolve(contact);
      });
    });
  }

  getContactsWhereTimeoutRate({ timeoutRate }) {
    const query = {
      'timeoutRate': { '$lte': timeoutRate }
    };

    return new Promise((resolve, reject) => {
      this._storage.models.Contact.find(query, (err, contacts) => {
        if(err) { reject(err); }
        else    { resolve(contacts); }
      });
    });
  }

  getShardByHashRelatedToNode({ nodeId, shardHash }) {
    return new Promise((resolve, reject) => {
      const query = {
        $and : [ 
          { 'trees.nodeID': nodeId },
          { 'hash': shardHash },
        ]
      }; 

      this._storage.models.Shard.findOne(query, function (err, shard) {
        if(err) {
          log.error('There was an error querying a shard!', err);
          reject(err);
        }
        if(!shard || shard === null) {
          log.info('There is no shard with hash %s for node %s', shardHash, nodeId);
        } else {
          log.info(`Shard with hash ${shardHash} found!`);
        }
        resolve(shard);
      });
    });
  }

  getMirrorByShardHash({ shardHash }) {
    log.info('here');
    return new Promise((resolve, reject) => {
      const query = {
        $and : [ 
          { 'isEstablished': true },
          { 'shardHash': shardHash },
        ]
      }; 

      this._storage.models.Mirror.findOne(query, function (err, shard) {
        if(err) {
          log.error('There was an error querying a shard!', err);
          reject(err);
        }
        if(!shard || shard === null) {
          log.info('There is no mirror for shard hash %s', shardHash);
        } else {
          log.info(`Mirror with shard hash ${shardHash} found!`);
        }
        resolve(shard);
      });
    });
  }

  getShardContent (contact, token, hash) {
    const SHARD_SOCKET_TIMEOUT = 90 * 1000;
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

}

module.exports = AuditService;