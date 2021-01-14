const storj = require('storj-lib');
const log = require('../logger');
const AuditService = require('./service');

class ShardAudit {
  constructor ({ nodeId, shardId, shardHash, network, service = new AuditService(), shard, attempts = 1 }) {
    this._nodeId = nodeId;
    this._shardId = shardId;
    this._network = network;
    this._shard = shard;
    this._shardHash = shardHash; 
    this._shardAudited = null;
    this._service = service;
    this._health = false;
    this._logging = false;
    this._attempts = attempts;
  }

  enableLogging () {
    this._logging = true;
    return this;
  }

  retrieveShardToken (contact, contract) {
    return new Promise((resolve, reject) => {
      this._network.getRetrievalPointer(contact, contract, (err, pointer) => {
        if (err || !pointer || !pointer.token) reject(err);
        else resolve(pointer.token);
      });
    });
  }

  /**
   * Starts the shard audit choosing a strategy depending on data setted
   */
  async start () {
    this._attempts--;

    if(this._shard) {
      await this._audit();
      return;
    }

    if(this._shardId && this._nodeId) {
      await this._auditByShardIdAndNodeId();
      return;
    }

    log.warn('No shard/node ids have been provided neither a shard object');
    log.warn('Therefore, audit will do nothing');
  }

  /**
   * Audits a shard, assumes that this._shard contains the shard to audit
   */
  async _audit () {
    let contact = await this._service.getContactById({ id: this._shard.contact });
    contact = storj.Contact(contact);
    let contract = storj.Contract(this._shard.contract);

    const token = await this.retrieveShardToken(contact, contract);
    const response = await this.isShardHealthy(contact, contract, token, this._shard.shardHash);
    const { status, size, hash, reason } = response;
    
    this.setShardAudited(this._shard.shardHash, hash, contract.get('data_size'), size, status, reason);

    if(this._attempts > 0) {
      this.start();
    }
  }

  /**
   * Audits a shard, assumes that this._nodeId and this._shardId are setted.
   */
  async _auditByShardIdAndNodeId () {
    const nodeId = this._nodeId;
    const shardId = this._shardId;
    this._shard = await this._service.getShardByIdRelatedToNode({ nodeId, shardId });
    await this._audit();
  }

  setShardAudited (storedHash, currentHash, storedSize, currentSize, healthy, reason) {
    this._shardAudited = { storedHash, currentHash, storedSize, currentSize, healthy, reason };
  }

  getShardAudited () {
    return this._shardAudited;
  }

  /**
   * Checks shard's hash and size against Database records.
   * @param {storj.Contact} contact 
   * @param {storj.Contract} contract 
   * @param {string} token 
   * @param {string} shardHash 
   */
  async isShardHealthy (contact, contract, token, shardHash) {
    let { size, hasher } = await this._service.getShardContent(contact, token, shardHash);
    let hash = storj.utils.rmd160b(hasher.digest()).toString('hex');

    if(this._logging) {
      log.info(`Shard downloaded hash is ${hash}, shard hash from database is ${shardHash}`);
    }
    
    let status = false;
    let reason = 'NONE';

    if(hash !== shardHash) {
      reason = 'HASHES_DO_NOT_MATCH';
    } else if(size !== contract.get('data_size')) {
      reason = 'SIZES_DO_NOT_MATCH';
    } else {
      status = true;
    }

    return { status, size, hash, reason };
  }
}

module.exports = ShardAudit;