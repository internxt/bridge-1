const storj = require('storj-lib');
const log = require('../logger');
const AuditService = require('./service');
const { mapSeries } = require('async');

class ShardAudit {
  constructor({ nodeId, shardId, shardHash, network, service = new AuditService(), shard, attempts = 1 }) {
    this._nodeId = nodeId;
    this._shardId = shardId;
    this._network = network;
    this._shard = shard;

    this._shardFromShards = null;

    this._shardHash = shardHash;
    this._shardAudited = null;
    this._service = service;
    this._health = false;
    this._logging = false;
    this._attempts = attempts;
  }

  setShard({ shard }) {
    this._shard = shard;
    return this;
  }

  setShardFromShards({ shard }) {
    this._shardFromShards = shard;
    return this;
  }

  enableLogging() {
    this._logging = true;
    return this;
  }

  retrieveShardToken(contact, contract) {
    return new Promise((resolve) => {
      this._network.getRetrievalPointer(contact, contract, (err, pointer) => {
        if (err || !pointer || !pointer.token) resolve({ status: false, result: err });
        else resolve({ status: true, result: pointer.token });
      });
    });
  }

  /**
   * Starts the shard audit choosing a strategy depending on data setted
   */
  async start() {
    this._attempts--;

    try {
      if (this._shard) {
        log.info('Auditing shard from mirrors');
        await this.auditFromMirrors();

        if (this._shardFromShards) {
          log.info('Auditing shard from shard contracts');
          await this.auditFromShardsContracts();
        }
        
        if (this._attempts > 0) { await this.start(); }

        return;
      }

      if (this._nodeId && this._shardHash) {
        await this._auditByShardHashAndNodeId();

        if (this._attempts > 0) { await this.start(); }

        return;
      }

      throw new Error('No shard/node ids have been provided neither a shard object');

    } catch (unknownError) {
      const shardHash = this._shardHash ? this._shardHash : this._shard.shardHash;
      this.setShardAudited(shardHash, '', 0, 0, false, 'UNKNOWN ERROR:' + unknownError);
      log.warn(`An error happened with shard with hash ${shardHash}`);
    }
  }

  async auditFromMirrors() {
    // audit by mirror
    let currentContact = await this._service.getContactById({ id: this._shard.contact });

    if (!currentContact) {
      this.notify('Not found', this._shard.shardHash, 0, false, 'Contact not found');
      return;
    }

    currentContact = storj.Contact(currentContact);
    let contract = storj.Contract(this._shard.contract);

    const tokenResponse = await this.retrieveShardToken(currentContact, contract);

    if (!tokenResponse.status) {
      this.notify(contract.get('farmer_id'), this._shard.shardHash, 0, false, tokenResponse.result);
      return;
    }

    const token = tokenResponse.result;

    const response = await this.isShardHealthy(currentContact, contract, token, this._shard.shardHash);
    const { status, size, hash, reason } = response;

    if (status) {
      this.notify(contract.get('farmer_id'), this._shard.shardHash, size, true, 'NONE');
      // this.setShardAudited(this._shard.shardHash, hash, contract.get('data_size'), size, status, reason);
    } else {
      // this.setShardAudited(this._shard.shardHash, hash, contract.get('data_size'), size, false, reason);
      this.notify(contract.get('farmer_id'), hash, size, false, reason);
    }
  }

  auditFromShardsContracts() {
    let contract, contact, rawContract, nodeID;

    return mapSeries(this._shardFromShards.contracts, (contractWithNode, next) => {
      nodeID = contractWithNode.nodeID;
      rawContract = contractWithNode.contract;

      this._service.getContactById({ id: nodeID })
        .then((requestedContact) => {
          contract = storj.Contract(rawContract);
          contact = storj.Contact(requestedContact);

          return this.getTokenForContact(contact, contract);
        })
        .then((token) => {
          return this.isShardHealthy(contact, contract, token, this._shard.shardHash);
        })
        .then((result) => {
          this.notify(nodeID, result.hash, result.size, result.status, result.reason);
        })
        .catch((err) => {
          this.notify(nodeID, '', 0, false, err.message);
        })
        .finally(() => next());
    });
  }

  async _auditByShardHashAndNodeId() {
    const nodeId = this._nodeId;
    const shardHash = this._shardHash;

    let [shard, contact, mirror] = await Promise.all([
      this._service.getShardByHashRelatedToNode({ nodeId, shardHash }),
      this._service.getContactByNodeId({ nodeId }),
      this._service.getMirrorByShardHash({ shardHash }),
    ]);

    let contractData;

    if (!shard) {
      log.warn('shard with hash %s not found for node %s', shardHash, nodeId);

      if (!mirror) {
        log.warn(`Shard with hash ${shardHash} is not healthy`);
        this.setShardAudited(shardHash, '', 0, 0, false, 'SHARD_AND_MIRROR_NOT_FOUND');
        return;
      } else {
        contractData = {
          nodeID: nodeId,
          contract: mirror.contract
        };
      }
    } else {
      for (let i = 0; i < shard.contracts.length; i++) {
        if (shard.contracts[i].nodeID == nodeId) {
          contractData = shard.contracts[i];
          break;
        }
      }
    }

    if (!contact) {
      log.warn(`Shard with hash ${shardHash} is not healthy`);
      this.setShardAudited(shardHash, '', 0, 0, false, 'CONTACT_NOT_FOUND');
      return;
    }

    contact = storj.Contact(contact);

    if (!contractData || !contractData.contract) {
      log.warn(`Shard with hash ${shardHash} is not healthy`);
      this.setShardAudited(shardHash, '', 0, 0, false, 'CONTRACT_NOT_FOUND');
      return;
    }

    const contract = storj.Contract.fromObject(contractData.contract);
    const tokenResponse = await this.retrieveShardToken(contact, contract);

    if (!tokenResponse.status) {
      log.warn(`Shard with hash ${shardHash} is not healthy`);
      this.setShardAudited(shardHash, '', 0, 0, false, 'TOKEN_FOR_SHARD_NOT_FOUND');
      return;
    }

    const token = tokenResponse.result;

    const response = await this.isShardHealthy(contact, contract, token, shardHash).catch(console.log);
    const { status, size, hash, reason } = response;

    if (!status) {
      log.warn(`Shard with hash ${shardHash} is not healthy`);
      this.setShardAudited(shardHash, hash, contract.get('data_size'), size, false, reason);
      return;
    }

    log.info(`Shard with hash ${shardHash} is healthy`);
    this.setShardAudited(shardHash, hash, contract.get('data_size'), size, status, reason);

  }

  setShardAudited(storedHash, currentHash, storedSize, currentSize, healthy, reason) {
    this._shardAudited = { storedHash, currentHash, storedSize, currentSize, healthy, reason };
  }

  getShardAudited() {
    return this._shardAudited;
  }

  /**
   * Checks shard's hash and size against Database records.
   * @param {storj.Contact} contact 
   * @param {storj.Contract} contract 
   * @param {string} token 
   * @param {string} shardHash 
   */
  async isShardHealthy(contact, contract, token, shardHash) {
    let status = false;
    let reason = 'NONE';

    try {
      let { size, hasher } = await this._service.getShardContent(contact, token, shardHash);
      let hash = storj.utils.rmd160b(hasher.digest()).toString('hex');

      if (this._logging) {
        log.info(`Shard downloaded hash is ${hash}, shard hash from database is ${shardHash}`);
      }

      if (hash !== shardHash) {
        reason = 'HASHES_DO_NOT_MATCH';
      } else if (size !== contract.get('data_size')) {
        reason = 'SIZES_DO_NOT_MATCH';
      } else {
        status = true;
      }

      return { status, size, hash, reason };
    } catch (e) {
      return { status: false, size: 0, hash: '', reason: e };
    }
  }

  /**
   * Retrieves the token required for accessing to a node.
   * @param {storj.Contact} contact
   * @param {storj.Contract} contract 
   * @returns 
   */
  async getTokenForContact(contact, contract) {
    if (!(contact instanceof storj.Contact)) {
      contact = storj.Contact(contact);
    }

    if (!(contract instanceof storj.Contract)) {
      contract = storj.Contract(contract);
    }

    const { status, result } = await this.retrieveShardToken(contact, contract);

    if (!status) {
      log.error(result);
      throw new Error('Token for shard not found');
    }
    
    return result;
  }

  notify(nodeID, currentHash, currentSize, healthy, reason) {
    const header = `(shard ${this._shard.shardHash}, node ${nodeID})`;

    if (!healthy) {
      log.warn(`${header}: shard is not healthy`);
      log.warn(`${header}: size ${currentSize}, current hash ${currentHash}`);
      log.warn(`${header}: ${reason}`);
    } else {
      log.info(`${header}: shard is healthy`);
    }
  }
}

module.exports = ShardAudit;