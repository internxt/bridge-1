const log = require('../logger');
const NodeAudit = require('./NodeAudit');
const AuditService = require('./service');

class WalletAudit {
  constructor({ wallet, service = new AuditService(), network, config = {} }) {
    const DEFAULTS = {
      concurrentMode: false,
      maxConcurrency: 2,
      sample: 100
    };

    this._wallet = wallet;
    this._service = service;
    this._network = network;
    this.config = { ...config, ...DEFAULTS };

    // In sequential mode, we reuse this audit instead of creating new objets each time
    this._currentNodeAudit = new NodeAudit({ nodeId: '', network, service });
  }

  concurrent({ maxConcurrency }) {
    this.config.concurrentMode = true;
    this.config.maxConcurrency = maxConcurrency;

    return this;
  }

  start() {
    return new Promise((resolve, reject) => {
      if (this.config.concurrentMode) {
        this._concurrentAudit(resolve, reject);
      } else {
        this._sequentialAudit(resolve, reject);
      }
    });
  }

  _concurrentAudit(resolve = Promise.resolve, reject = Promise.reject) {
    const cursor = this._service.getMirrorsByWallet({ wallet: this._wallet });
    const MAX_CONCURRENCY_LIMIT = this.config.maxConcurrency;

    const processesRunning = [];
    const allocProcess = () => processesRunning.push({});
    const freeProcess = () => processesRunning.pop();

    const next = () => {
      freeProcess(); cursor.resume();
    };
    const processData = (mirror) => {
      if (processesRunning.length >= MAX_CONCURRENCY_LIMIT) {
        cursor.pause();
      }

      allocProcess();

      const nodeAudit = new NodeAudit({ service: this._service, network: this._network });
      nodeAudit.nodeId = mirror.contact;

      nodeAudit.start().finally(() => next());
    };

    cursor.on('data', processData);
    cursor.on('error', reject);
    cursor.on('end', resolve);
  }

  _sequentialAudit(resolve, reject) {
    const nodeAudit = new NodeAudit({ network: this._network, service: this._service });
    const cursor = this._service.getMirrorsByWallet({ wallet: this._wallet });

    cursor.on('data', async (mirror) => {
      cursor.pause();

      log.info('[wallet %s, node %s]: Auditing', this._wallet, mirror.contact);

      try {
        nodeAudit.nodeId = mirror.contact;
        // nodeAudit.config.allMirrors = true;

        await nodeAudit.start();
      } catch (err) {
        log.error('Unexpected error auditing node %s: %s', mirror.contact, err.message);
      } finally {
        cursor.resume();
      }
    });

    cursor.on('error', reject);
    cursor.on('end', resolve);
  }
}

module.exports = WalletAudit;