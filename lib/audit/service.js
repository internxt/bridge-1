const storj = require('storj-lib');
const ComplexClient = require('storj-complex').createClient;
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const log = require('../logger');

class AuditService {

  constructor({storage}) {
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
      // const query = { 'contact': nodeId, 'isEstablished': true }; 
      // As we are checking for 
      this._storage.models.Mirror.find(query).sort({'contract.data_size': 1}).limit(sample).exec(function (err, shards) {
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

}

module.exports = AuditService;