const EventEmitter = require('events').EventEmitter;
const complex = require('storj-complex');
const Storage = require('storj-service-storage-models');
const Logger = require('../logger');
const ms = require('ms');
const async = require('async');
const assert = require('assert');
const Complex = require('storj-complex/lib/client');

const storj = require('storj-lib');

const HOURS_24 = ms('24h');

class Renewal extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;

    this.network = null;
    this.storage = null;

    this.initialized = false;
    this.cursor = null;
  }

  init() {
    if (this.initialized) {
      return Logger.warn('Renewal already initialized');
    }

    const { mongoUrl, mongoOpts } = this.config.storage;

    this.network = complex.createClient(this.config.complex);
    this.storage = new Storage(mongoUrl, mongoOpts, { Logger });
    this.initialized = true;

    this.emit('initialized');
  }

  build(network, storage) {
    if (this.initialized) {
      return Logger.warn('Renewwal already initialized');
    }

    assert(network instanceof Complex);
    // Not working on unit tests, should be reviewed
    // assert(storage instanceof Storage);

    this.network = network;
    this.storage = storage;
    this.initialized = true;
  }

  initContractRenew(callback) {
    const NOW = Date.now();
    const LAST_DAY = NOW;
    const TWO_DAYS = NOW + HOURS_24 * 2;

    const query = {
      'contracts.contract.store_end': {
        $gte: LAST_DAY,
        $lte: TWO_DAYS
      }
    };

    // Show info about how many shards will be proccesed
    this.storage.models.Shard.find(query).count((err, querySize) => {
      if (!err) {
        Logger.info('Query size: %s', querySize);
        this.emit('counter-querysize', querySize);
      } else {
        this.emit('counter-querysize', -1);
      }
    });


    this.cursor = this.storage.models.Shard.find(query).cursor();

    this.cursor
      .on('error', (err) => {
        console.error('Cursor error', err);
        callback(err);
      })
      .on('data', (shard) => {
        let needsRenewal = [];

        Logger.debug('CURSOR PAUSE');
        this.cursor.pause();

        for (let contractInfo of shard.contracts) {
          let { contract: contractObj, nodeID } = contractInfo;
          // Only contracts with store_end < 2 days needs renewal
          if (contractObj.store_end > LAST_DAY && contractObj.store_end < TWO_DAYS) {
            let contract = new storj.Contract(contractObj);
            needsRenewal.push([nodeID, contract]);
          }
        }

        let renewalContracts = needsRenewal.map(([nodeId, contract]) => {
          contract.set('store_end', NOW + ms('3650d'));

          return [nodeId, contract];
        });

        // console.log('Contracts to be renewed: %s', renewalContracts.length)

        async.map(renewalContracts, this.lookupFarmer.bind(this), this.maybeRenewContracts.bind(this));
      })
      .on('end', () => {
        this.emit('end');
        callback();
      });
  }

  lookupFarmer([nodeId, contract], next) {
    this.storage.models.Contact.findOne({ _id: nodeId }, (err, contact) => {
      if (!err && !contact) {
        // Missing contact, clean from database
        // console.error('######### Contact missing')
      }
      if (err || !contact) {
        this.emit('counter-errored');

        return next(null, null);
      }

      return next(null, [storj.Contact(contact.toObject()), contract]);
    });
  }

  maybeRenewContracts(err, results) {
    let canBeRenewed = results.filter(result => !!result);
    async.eachLimit(canBeRenewed, 6, this.checkIfNeedsRenew.bind(this), () => {
      Logger.debug('RESUME');
      this.cursor.resume();
    });
  }

  forceRenewContracts(err, results) {
    let canBeRenewed = results.filter((result) => !!result);
    async.eachLimit(canBeRenewed, 6, this.renewContract.bind(this), () => { });
  }

  checkIfNeedsRenew([contact, contract], done) {
    let shardHash = contract.get('data_hash');
    async.waterfall([
      (next) => this.getPointerObjects.call(this, shardHash, next),
      (pointers, next) => this.getAssociatedFrames(pointers, next),
      (frames, next) => this.getParentBucketEntries(frames, next),
      (next) => this.renewContract([contact, contract], next)
    ], () => this.finishProcessingContract(done));
  }

  getPointerObjects(shardHash, next) {
    this.storage.models.Pointer.find({ hash: shardHash }, (err, pointers) => {
      if (err || pointers.length === 0) {
        return next(new Error('No pointers found'));
      }
      next(null, pointers);
    });
  }

  getAssociatedFrames(pointers, next) {
    this.storage.models.Frame.find({
      shards: {
        $in: pointers.map((pointer) => pointer._id)
      }
    }, (err, frames) => {
      if (err || frames.length === 0) {
        return next(new Error('No frames found'));
      }
      next(null, frames);
    });
  }

  getParentBucketEntries(frames, next) {
    this.storage.models.BucketEntry.find({
      frame: {
        $in: frames.map((frame) => frame._id)
      }
    }, (err, entries) => {
      if (err || entries.length === 0) {
        return next(new Error('No bucket entries found'));
      }
      next();
    });
  }

  renewContract([contact, contract], next) {
    this.network.renewContract(contact, contract, (err) => {
      if (err) {
        // TODO: Should contract be removed??
        this.emit('counter-errors', { contact, contract: contract.toObject(), error: err });
        this.emit('counter-errored');
        next();
      } else {
        this.emit('counter-renewed', { contact, contract: contract.toObject() });
        this.updateContractRecord(contact, contract, next);
      }
    });
  }

  updateContractRecord(contact, updatedContract, next) {
    this.storage.models.Shard.findOne({
      hash: updatedContract.get('data_hash')
    }, (err, shard) => {
      if (err) {
        return next(err);
      }

      if (!shard) {
        return next(new Error('Missing shard'));
      }

      for (let contract of shard.contracts) {
        if (contract.nodeID !== contact.nodeID) {
          continue;
        }

        contract.contract = updatedContract.toObject();
      }

      shard.save(next);
      this.emit('renew-ok', updatedContract.get('data_hash'));
    });
  }

  finishProcessingContract(done) {
    this.emit('counter-processed');
    done();
  }

  invalidateContract(hash, callback) {
    const NOW = Date.now();

    this.storage.models.Shard.findOne({ 'hash': hash }, (err, shard) => {
      if (err || !shard) {
        return callback(err || 'shard not found');
      }

      if (!shard.contracts) {
        Logger.error('Renewal error: contracts is null');

        return callback();
      }

      let needsRenewal = [];

      for (let contractInfo of shard.contracts) {
        let { contract: contractObj, nodeID } = contractInfo;
        let contract = new storj.Contract(contractObj);
        needsRenewal.push([nodeID, contract]);
      }

      let renewalContracts = needsRenewal.map(([nodeId, contract]) => {
        contract.set('store_end', NOW - ms('7d'));

        return [nodeId, contract];
      });

      async.map(renewalContracts, this.lookupFarmer.bind(this), this.forceRenewContracts.bind(this));
      callback();
    });
  }
}

module.exports = Renewal;