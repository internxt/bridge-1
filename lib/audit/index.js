const Storage = require('storj-service-storage-models');
const storj = require('storj-lib');
const ComplexClient = require('storj-complex').createClient;
const MongoDBStorageAdapter = require('storj-mongodb-adapter');
const log = require('../logger');
const AuditService = require('./service');
const NodeAudit = require('./NodeAudit');
const ShardAudit = require('./ShardAudit');
const WalletAudit = require('./WalletAudit');

class Audit {
  constructor(config, attempts) {
    this.storage = null;
    this._config = config;
    this._attempts = attempts ? attempts : 1;
    this._attemptsCounter = 0;
    this._success = false;
    this.initialized = false;
    this._start = 0;
    this._nodeAuditor = null;
    this._concurrentMode = false;
  }

  init() {
    if(this.initialized) {
      return log.warn('Audit already initialized');
    }

    this.storage = new Storage(
      this._config.storage.mongoUrl,
      this._config.storage.mongoOpts,
      { logger: log }
    );

    this.service = new AuditService({ storage: this.storage });

    this.network = new ComplexClient(this._config.complex);

    this.contracts = new storj.StorageManager(
      new MongoDBStorageAdapter(this.storage),
      { disableReaper: true }
    );

    this.initialized = true;
  }

  /**
   * Audits the nodes related to a payment wallet
   * @param {string} hash Wallet hash
   */
  async wallet (hash) {
    const service = this.service;
    const network = this.network;
    const audit = new WalletAudit({ wallet: hash, service, network });

    if(this._concurrentMode) {
      audit.concurrent({ maxConcurrency: 5 });
    }

    await audit.start();

    const overallHealth = audit.getOverallHealth();
    const nodesAudited  = audit.getNodesAudited();

    return { overallHealth, nodesAudited };
  }

  concurrent () {
    this._concurrentMode = true;
    return this;
  }

  /**
   * Audits a node
   * @param {string} nodeId 
   */
  async node (nodeId) {
    const service = this.service;
    const network = this.network;
    const audit = new NodeAudit({ nodeId, service, network });
    await audit.start();
    return audit.getShardsAudited();
  }

  /**
   * Audits a shard of a given node.
   * @param {string} shardHash 
   * @param {string} nodeId 
   */
  async shard (shardHash, nodeId, attempts) {
    const service = this.service;
    const network = this.network;
    const audit = new ShardAudit({ nodeId, shardHash, network, service, attempts });
    await audit.start();
    return audit.getShardAudited();
  }

}

module.exports = Audit;