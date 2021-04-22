const log = require('../logger');
const ShardAudit = require('./ShardAudit');
const AuditService = require('./service');

class NodeAudit { 
  constructor({ nodeId, mirrors = [], service = new AuditService(), network, config = {} }) {
    const DEFAULTS = { sample: 100, allMirrors: false };

    this._service = service;
    this._network = network;
    this.config = { ...config, ...DEFAULTS };

    this.nodeId = nodeId;
    this._mirrors = mirrors;
  }

  async start() {
    if (this._mirrors.length === 0) {
      if (this.config.allMirrors) {
        this._mirrors = await this._service.getMirrorsByNodeId({ nodeId: this.nodeId });
      } else {
        this._mirrors = await this._service.getMirrorsSampleByNodeId({ 
          nodeId: this.nodeId, 
          sample: this.config.sample 
        });
      }
    }

    if (this._mirrors && this._mirrors.length > 0) {
      log.info('Found at least %s mirrors', this._mirrors.length);

      const shardAudit = new ShardAudit({
        nodeId: this.nodeId,
        network: this._network,
        service: this._service
      });
  
      for (const mirror of this._mirrors) {
        shardAudit._shardHash = mirror.shardHash;
        await shardAudit.start();
      }
    } else {
      log.warn('Mirrors not found for node %s', this.nodeId);
    }
  }
}


module.exports = NodeAudit;