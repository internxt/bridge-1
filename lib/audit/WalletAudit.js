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
  }

  start () {
    let nodeAudit = new NodeAudit({ 
      nodeId: '', 
      network: this._network,
      service: this._service 
    });

    return new Promise((resolve, reject) => {
      const cursor = this._service.getMirrorsByWallet({wallet: this._wallet});

      cursor.on('data', async (mirror) => {
        cursor.pause(); // right now is sync

        const nodeId = mirror.contact;
        nodeAudit.setNodeId({ nodeId });
        await nodeAudit.start();

        this._nodesAudited.push({
          nodeId, 
          health: nodeAudit.getHealth(),
          shardAudited: nodeAudit.getShardsAudited()
        });

        cursor.resume();
      });

      cursor.on('error', reject);
      cursor.on('end', resolve);
    });
  }

  async auditNode (node) {
    try {
      const nodeId = node.id;
      const service = this._service;
      const network = this._network;
      const nodeAudit = new NodeAudit({ nodeId, service, network });
      await nodeAudit.start();
      this._health += nodeAudit.getHealth();
      this._nodesAudited.push(nodeAudit);
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