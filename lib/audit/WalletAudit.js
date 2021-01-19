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
    // In sequential mode, we reuse this audit instead of creating new objets each time
    this._currentNodeAudit = new NodeAudit({ nodeId: '', network, service });
    this._concurrentMode = false;
  }

  concurrent ({ maxConcurrency }) {
    if(!maxConcurrency) {
      throw new Error('If you want to do an async audit, a maxConcurrency parameter is required');
    }
    this._concurrentMode = true;
    this._maxConcurrency = maxConcurrency;
    return this;
  }

  start () {
    return new Promise((resolve, reject) => {
      if(this._concurrentMode) {
        this._concurrentAudit(resolve, reject);
      } else {
        this._sequentialAudit(resolve, reject);
      }
    });
  }

  _concurrentAudit (resolve = Promise.resolve, reject = Promise.reject) {
    const cursor = this._service.getMirrorsByWallet({ wallet: this._wallet });
    const MAX_CONCURRENCY_LIMIT = this._maxConcurrency;

    const processesRunning = [];
    const allocProcess = () => processesRunning.push({});
    const freeProcess  = () => processesRunning.pop();

    const next = () => { freeProcess(); cursor.resume(); };
    const processData = (mirror) => {
      console.log('process runing!');
      if(processesRunning.length >= MAX_CONCURRENCY_LIMIT) {
        cursor.pause();
      }

      allocProcess();

      this.auditNode(
        new NodeAudit({ service: this._service, network: this._network }), 
        mirror.contact
      ).then(next).catch(next);
    };

    cursor.on('data', processData);
    cursor.on('error', reject);
    cursor.on('end', resolve);
  }

  _sequentialAudit (resolve, reject) {
    const cursor = this._service.getMirrorsByWallet({ wallet: this._wallet });

    cursor.on('data', (mirror) => {
      cursor.pause(); 
      const next = () => cursor.resume();

      this.auditNode(this._currentNodeAudit, mirror.contact).then(next).catch(next);
    });

    cursor.on('error', reject);
    cursor.on('end', resolve);
  }

  async auditNode (nodeAudit = new NodeAudit(), nodeId) {
    try {
      nodeAudit.setNodeId({ nodeId });
      await nodeAudit.start();

      const health = nodeAudit.getHealth();
      const shardsAudited = nodeAudit.getShardsAudited();

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

  getOverallHealth () {
    const nodesCount = this._nodesAudited.length;
    const nodesHealthSum = this._health;
    return (nodesHealthSum / nodesCount).toFixed(2);
  }

}

module.exports = WalletAudit;