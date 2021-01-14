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
    this._started = false;
    this._finished = false;
  }

  async start () {
    this._started = true;

    const mirrors = await this._service.getMirrorsByWallet({ wallet: this._wallet });
    mirrors.map(m => this._nodes.push({ id: m.contact }));

    const nodeAuditsPromises = this._nodes.map(this.auditNode);
    await Promise.all(nodeAuditsPromises);

    this._finished = true;
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
    if(!this._started) {
      log.error('Wallet audit not started yet, please call start() first');
      return;
    }

    if(!this._finished) {
      log.error('Wallet audit not finished yet, please "await" start() method');
      return;
    }

    const healthyRatio = (this._health / this._sample).toFixed(2); 
    log.info(`Wallet ${this._wallet} audited with a sample of ${this._sample} nodes`);
    log.info(`Has an average healthy ratio of ${healthyRatio}`);
  }

  getNodesAudited () {
    return this._nodesAudited;
  }

}

module.exports = WalletAudit;