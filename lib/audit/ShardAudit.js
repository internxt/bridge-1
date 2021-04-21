const storj = require('storj-lib');
const log = require('../logger');
const AuditService = require('./service');

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

    this._from = '';
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
        log.info('Auditing shard from mirror');
        await this.auditByMirrors({ shardHash: this._shardHash });

        if (this._shardFromShards) {
          log.info('Auditing shard from shard contracts');
          await this.auditByShardContracts({ shardHash: this._shardHash });
        }
        
        if (this._attempts > 0) { await this.start(); }

        return;
      }

      if (this._shardHash) {
        if (this._nodeId) {
          await this._auditByShardHashAndNodeId({ shardHash: this._shardHash, nodeId: this._nodeId });

          if (this._attempts > 0) { await this.start(); }

          return;
        } else {
          await this.byHash(this._shardHash);

          return;
        }
      }

      throw new Error('No shard/node ids have been provided neither a shard object');

    } catch (unknownError) {
      const shardHash = this._shardHash ? this._shardHash : this._shard.shardHash;
      this.setShardAudited(shardHash, '', 0, 0, false, 'UNKNOWN ERROR:' + unknownError);
      log.warn(`An error happened with shard with hash ${shardHash}`);
    }
  }

  /**
   * Audits a shard from its current established mirrors
   * @param Object An object with a unique field, the shard hash
   */
  async auditByMirrors({ shardHash }) {
    console.log('shard hash', shardHash);
    try {
      const mirrors = await this._service.getMirrorsByShardHash({ shardHash });

      if (mirrors && mirrors.length > 0) {
        for(const mirror of mirrors) {
          try {
            await this.auditByMirror(mirror);
          } catch (err) {
            log.warn('Unexpected error for shard %s, mirror %s: %s', 
              shardHash, mirror.contact, err.message);
            console.error(err);
          } 
        }
      } else {
        log.warn('Shard %s does not have mirrors. Auditing only from contracts.', shardHash);
      }
    } catch (err) {
      log.error('Unexpected error for shard %s: %s', shardHash, err.message);
    }
  }

  /**
   * Audits a shard from a given mirror
   * @param {Storage.models.Mirror} Mirror A mirror where shard is located
   */
  async auditByMirror(mirror) {
    try {
      const rawContact = await this._service.getContactById({ id: mirror.contact });

      if (!rawContact) {
        throw new Error('Contact not found');
      }

      const contact = storj.Contact(rawContact);
      const contract = storj.Contract(mirror.contract);

      const token = await this.getTokenForContact(contact, contract);
      const res = await this.isShardHealthy(contact, contract, token, mirror.shardHash);

      if (!res.status) {
        throw new Error(res.reason);
      }

      this.notify(mirror.contact, res.hash, res.size, res.status, res.reason);
    } catch (err) {
      if (err.message === 'Contact not found') {
        return this.notify('Not found', '', 0, false, err.message);
      }
     
      this.notify(mirror.contact, '', 0, false, err.message);
    }
  }

  /**
   * Audits a shard given its hash
   * @param shardHash Shard hash
   */
  async byHash(shardHash) {
    this._shardHash = shardHash;

    this._from = 'mirrors';
    await this.auditByMirrors({ shardHash });

    this._from = 'shard contracts';
    await this.auditByShardContracts({ shardHash }); 
  }

  /**
   * Audits a shard by its contracts
   * @param Object An object with only one field, the shard hash
   */
  async auditByShardContracts({ shardHash }) {
    let shard;

    try {
      shard = await this._service.getShardByShardHash({ shardHash });

      if(!shard) {
        throw new Error('Shard does not exist');
      }
    } catch (err) {
      log.error('Unexpected error for shard %s: %s', shardHash, err.message);
      return;
    }

    let contract, contact, rawContract, rawContact, token, res, nodeID = '';

    if (shard.contracts && shard.contracts.length > 0) {
      for(const contractWithNode of shard.contracts) {
        try {
          nodeID = contractWithNode.nodeID;
          rawContract = contractWithNode.contract;
          rawContact = await this._service.getContactById({ id: nodeID });
  
          contract = storj.Contract(rawContract);
          contact = storj.Contact(rawContact);
  
          token = await this.getTokenForContact(contact, contract);
          res = await this.isShardHealthy(contact, contract, token, shardHash);
  
          this.notify(nodeID, res.hash, res.size, res.status, res.reason);
        } catch (err) {
          this.notify(nodeID, '', 0, false, err.message);
        }
      }
    } else {
      log.warn('Shard %s does not have contracts.', shardHash);
    }
  }

  async _auditByShardHashAndNodeId({ nodeId, shardHash }) {
    this._from = 'mirrors';

    try {
      let [shard, contact, mirror] = await Promise.all([
        this._service.getShardByHashRelatedToNode({ nodeId, shardHash }),
        this._service.getContactByNodeId({ nodeId }),
        this._service.getMirrorByShardHash({ shardHash }),
      ]);

      let contractData;

      if (!shard) {
        if (!mirror) {
          throw new Error('Shard and mirror not found');
        } 

        contractData = {
          nodeID: nodeId,
          contract: mirror.contract
        };
      } else {
        for (let i = 0; i < shard.contracts.length; i++) {
          if (shard.contracts[i].nodeID == nodeId) {
            contractData = shard.contracts[i];
            break;
          }
        }
      }

      if (!contact) {
        throw new Error('Contact not found');
      }

      contact = storj.Contact(contact);

      if (!contractData || !contractData.contract) {
        throw new Error('Contract not found');
      }

      const contract = storj.Contract.fromObject(contractData.contract);
      const token = await this.getTokenForContact(contact, contract);

      const res = await this.isShardHealthy(contact, contract, token, shardHash);

      if (!res.status) {
        throw new Error(res.reason);
      }

      this.notify(nodeId, res.hash, res.size, res.status, res.reason);
    } catch (err) {
      this.notify(nodeId, '', 0, false, err.message);
    } 
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
    const header = `[shard ${this._shardHash}, node ${nodeID}]`;

    if (!healthy) {
      log.warn(`${header}: not OK (from ${this._from})`);
      log.warn(`${header}: size ${currentSize}, current hash ${currentHash}`);
      log.warn(`${header}: ${reason}`);
    } else {
      log.info(`${header}: OK (from ${this._from})`);
    }
  }
}

module.exports = ShardAudit;