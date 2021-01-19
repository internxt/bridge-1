const log = require('../logger');
const NodeAudit = require('./NodeAudit');
const AuditService = require('./service');

class WalletAudit {
  constructor ({ wallet, service = new AuditService(), network }) {
    this._wallet = wallet;
    this._service = service;
    this._network = network;
    this._nodes = [];
    this._nodesAudited = [];
    this._health = 0;
    this._sample = 100;
    this._currentNode = new NodeAudit({ nodeId: '', network, service });
  }

  start () {
    return new Promise((resolve, reject) => {
      const cursor = this._service.getMirrorsByWallet({wallet: this._wallet});

      cursor.on('data', async (mirror) => {
        cursor.pause(); 
        await this.auditNode(mirror.contact);
        cursor.resume();
      });

      cursor.on('error', reject);
      cursor.on('end', resolve);
    });
  }

  async auditNode (nodeId) {
    try {
      this._currentNode.setNodeId({ nodeId });

      await this._currentNode.start();

      const health = this._currentNode.getHealth();
      const shardsAudited = this._currentNode.getShardsAudited();

      this._nodesAudited.push({ nodeId, health, shardsAudited });
      this._health += health;
    } catch (e) {
      log.warn(e);
    }
  }

  printStatus () {
    const healthyRatio = (this._health / this._sample).toFixed(2); 
    log.info(`Wallet ${this._wallet} audited with a sample of ${this._sample} nodes`);
    log.info(`Has an average healthy ratio of ${healthyRatio}`);
  }

  getNodesAudited () {
    return this._nodesAudited;
  }

}

module.exports = WalletAudit;