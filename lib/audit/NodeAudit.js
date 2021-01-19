const log = require('../logger');
const ShardAudit = require('./ShardAudit');
const AuditService = require('./service');

class NodeAudit {
  constructor ({ nodeId, shards = [], service = new AuditService(), network }) {
    this._nodeId = nodeId;
    this._shards = shards;
    this._shardsAudited = [];
    this._health = 0;
    this._sample = 100;
    this.service = service;
    this.network = network;
    this._logging = false;
    this._started = false;
    this._finished = false;
  } 

  enableLogging () {
    this._logging = true;
    return this;
  }

  async start () {
    this._started = true;

    if(this._shards.length === 0) {
      const nodeId = this._nodeId;
      const sample = this._sample;
      this._shards = await this.service.getShardsSampleByNodeId({ nodeId, sample });
    }
    await Promise.all(this._shards.map(async (shard) => {
      const shardHash = shard.shardHash;
      const network = this.network;
      const service = this.service;

      const shardAudit = new ShardAudit({ shardHash, network, shard, service });
      await shardAudit.start();

      this.addAuditedShard(shardAudit.getShardAudited());
    }));

    this._finished = true;
  }
  
  addAuditedShard ({storedHash, currentHash, storedSize, currentSize, healthy, reason}) {
    const shard = { storedHash, currentHash, storedSize, currentSize, healthy, reason };
    this._shardsAudited.push(shard);
    if(shard.healthy) {
      this._health++;
    }
  }
  
  printStatus () {
    if(!this._started) {
      log.error('Node Audit not started yet, please call start() first');
      return;
    }

    if(!this._finished) {
      log.error('Node audit not finished yet, please "await" start() method');
    }

    // don't use this._sample to obtain statistics as the class allows to inject n shards. 
    // this._shards.length gives the real number of shards audited.
    const healthPercentage = ((this._health / this._shards.length) * 100).toFixed(2);
    log.info(`${this._health} of ${this._shards.length} (${healthPercentage}%) shards checked are healthy`); 
  }

  getHealth () {
    return this._health;
  }

  getShardsAudited () {
    return this._shardsAudited;
  }

  setNodeId ({ nodeId }) {
    this._nodeId = nodeId;
  }
  
}


module.exports = NodeAudit;