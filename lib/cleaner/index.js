'use strict';

const async = require('async')
const assert = require('assert')
const crypto = require('crypto')
const MonitorConfig = require('../monitor/config')
const Storage = require('storj-service-storage-models')
const ms = require('ms')
const log = require('../logger')
const { ObjectId } = require('mongoose').Types

/**
 * A long running daemon that will inspect database looking for orphan
 * documents and perform a clean up.
 * It can be performed once a day.
 * @param {CleanerConfig} config - An instance of CleanerConfig
 */
function Cleaner(config) {
  if (!(this instanceof Cleaner)) {
    return new Cleaner(config);
  }

  assert(config instanceof MonitorConfig, 'Invalid config supplied');

  this.storage = null;

  this.currentObjectId = new ObjectId('000000000000000000000000')

  this._config = config;
  this._timeout = null;
  this._running = false;
}

Cleaner.SIGINT_CHECK_INTERVAL = 1000;
Cleaner.MAX_SIGINT_WAIT = 5000;

/**
 * Starts the Bridge instance
 * @param {Function} callback
 */
Cleaner.prototype.start = function (callback) {
  log.info('Cleaner service is starting');

  this.storage = new Storage(
    this._config.storage.mongoUrl,
    this._config.storage.mongoOpts,
    { logger: log }
  );

  // setup next run event
  this.wait();

  callback();
  process.on('SIGINT', this._handleSIGINT.bind(this));
  process.on('exit', this._handleExit.bind(this));
  process.on('uncaughtException', this._handleUncaughtException.bind(this));
};

Cleaner.prototype.run = function () {
  // If a check round is being executed, wait.
  if (this._running) {
    return this.wait();
  }

  const limit = this._config.application.queryNumber || 100;

  const finish = (err) => {
    if (err) {
      log.error(err);
    }

    this._running = false;
    this.wait();
  };

  log.info('Starting clean up round');
  this._running = true;

  async.waterfall([
    this.clearUsers.bind(this),
    this.clearBuckets.bind(this),
    this.clearBucketEntries.bind(this),
    this.clearFramesWithoutUser.bind(this),
    this.clearPointers.bind(this),
    this.clearInvalidContracts.bind(this),
    this.clearContacts.bind(this),
    this.clearShards.bind(this),
    this.clearMirrors.bind(this)
  ], finish)

};

Cleaner.prototype.clearUsers = function (callback) {
  // TODO: Clear users who:
  // - Older than 1 week & not activated
  // - Older than 1 year + 1 week & no buckets
  log.info('Check invalid user accounts')
  callback()
}

Cleaner.prototype.clearBuckets = function (callback) {
  log.info('Check buckets of missing user accounts')
  const Bucket = this.storage.models.Bucket

  const query = [
    {
      '$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user'
      }
    },
    { '$match': { 'user': { '$size': 0 } } },
    { '$project': { '_id': '$_id' } }
  ]

  Bucket.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(results => {
    const bucketIds = results.map((curr) => curr._id)
    if (bucketIds.length > 0)
      log.info('Total buckets without user to be removed: %s', bucketIds.length)
    Bucket.deleteMany({ _id: { $in: bucketIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearBucketEntries = function (callback) {
  log.info('Check bucket entries of non existent buckets')
  const BucketEntry = this.storage.models.BucketEntry
  const query = [
    {
      '$lookup': {
        'from': 'buckets',
        'localField': 'bucket',
        'foreignField': '_id',
        'as': 'bucket'
      }
    },
    { '$match': { 'bucket': { '$size': 0 } } },
    { '$project': { '_id': '$_id' } }
  ]

  BucketEntry.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(function (results) {
    const bucketEntryIds = results.map((curr) => curr._id)
    if (bucketEntryIds.length > 0)
      log.info('Total bucket entries without bucket to be removed: %s', bucketEntryIds.length)
    BucketEntry.deleteMany({ _id: { $in: bucketEntryIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearFramesWithoutUser = function (callback) {
  log.info('Check frames of non existent users')
  const Frame = this.storage.models.Frame
  const query = [
    {
      '$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user'
      }
    },
    { '$match': { 'user': { '$size': 0 } } },
    { '$project': { '_id': '$_id' } }
  ]

  Frame.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(frames => {
    const frameIds = frames.map((curr) => curr._id)
    if (frameIds.length > 0)
      log.info('Total frames without user to be removed: %s', frameIds.length)
    Frame.deleteMany({ _id: { $in: frameIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearPointers = function (callback) {
  log.warn('Check pointers of non existent frames')
  const Pointer = this.storage.models.Pointer
  // Look up pointers with no frames
  // Foreign field "shards" is not index on Frame collection, so this query may take a while.
  // TODO: Pointers should be deleted with its frame in the previous step (clearFramesWithoutUser)
  const query = [
    { '$sample': { size: 2000 } },
    {
      '$lookup': {
        'from': 'frames',
        'localField': '_id',
        'foreignField': 'shards',
        'as': 'shards'
      }
    },
    { '$match': { 'shards': { '$size': 0 } } },
    { '$project': { '_id': '$_id' } }
  ]

  Pointer.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(pointers => {
    const pointerIds = pointers.map((curr) => curr._id)
    if (pointerIds.length > 0)
      log.info('Total pointers without frames to be removed: %s', pointerIds.length)
    Pointer.deleteMany({ _id: { $in: pointerIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearContacts = function (callback) {
  log.info('Check invalid nodes')
  const Contact = this.storage.models.Contact
  const timeoutRateThreshold = this._config.application.timeoutRateThreshold;
  const currentDate = new Date();
  const targetDate = new Date(currentDate.setDate(currentDate.getDate() - 10));


  const query = [
    {
      '$match': {
        '$and': [
          { 'timeoutRate': { '$gte': timeoutRateThreshold } },
          { 'lastSeen': { '$lte': targetDate } }
        ]
      }
    }
  ]

  Contact.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(contacts => {
    const contactIds = contacts.map((curr) => curr._id)
    if (contactIds.length > 0)
      log.info('Total contacts to be removed: %s', contactIds.length)
    Contact.deleteMany({ _id: { $in: contactIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearShards = function (callback) {
  log.info('Checking shards')
  async.waterfall([
    this.clearShardsWithoutPointers.bind(this),
    this.clearShardsWithAllInvalidContracts.bind(this),
    this.clearShardsWithoutContracts.bind(this)
  ], (err) => callback(err))
}

Cleaner.prototype.clearShardsWithoutPointers = function (callback) {
  log.info('Check shards of non existent pointers')
  const Shard = this.storage.models.Shard
  const query = [
    {
      '$sample': { 'size': 10000 }
    },
    {
      '$lookup': {
        'from': 'pointers',
        'localField': 'hash',
        'foreignField': 'hash',
        'as': 'pointer'
      }
    },
    { '$match': { 'pointer': { '$size': 0 } } }
  ]

  Shard.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(shards => {
    const shardIds = shards.map((curr) => curr._id)
    if (shardIds.length > 0)
      log.info('Total shards without pointers to be removed: %s', shardIds.length)
    Shard.deleteMany({ _id: { $in: shardIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearInvalidContracts = function (callback) {
  log.info('Clear contracts from missing contacts')
  // Look for shards and inspect each contract, delete those invalid contracts from shard
  // Shard will be removed in the next steps if the total contracts is zero
  const Shard = this.storage.models.Shard
  const Contact = this.storage.models.Contact

  const cursor = Shard.find({ _id: { $gt: this.currentObjectId } }).sort({ _id: 1 }).limit(300).cursor()
  const flag = this.currentObjectId

  cursor.on('error', (err) => {
    console.log('Cursor error')
    callback(err)
  }).on('data', data => {
    cursor.pause()
    this.currentObjectId = new ObjectId(data._id)

    const contracts = data.contracts

    /**
     * If contract from shard is owned by a missing node,
     * will be removed from the shard document.
     * It means shard has less mirrors.
     */

    async.eachSeries(contracts, (contract, nextContract) => {
      const nodeID = contract.nodeID;
      Contact.findOne({ _id: nodeID }, (err, contact) => {
        if (err) {
          log.error('Error finding contact', err)
          nextContract(err)
        }
        else {
          if (!contact) {
            Shard.updateOne({ _id: this.currentObjectId }, { $pull: { contracts: { nodeID: nodeID } } }, (err) => nextContract(err))
          } else {
            nextContract()
          }
        }
      })
    }, (err) => {
      if (err) {
        log.error('Cannot end async contracts search', err)
      }
      cursor.resume()
    })

  }).on('end', () => {
    log.info('Last shard checked %s', this.currentObjectId)
    if (this.currentObjectId === flag) {
      log.info('Last shard reached, starting from 0')
      this.currentObjectId = new ObjectId('000000000000000000000000')
    }
    callback()
  })

}

Cleaner.prototype.clearShardsWithAllInvalidContracts = function (callback) {
  // Shards whose contracts (mirrors) are full of empty nodes. If only one node is valid, the shard won't be deleted.
  // clearInvalidContracts() will remove particular invalid contracts with missing nodes
  log.info('Check invalid contracts from shards')
  const Shard = this.storage.models.Shard
  const query = [
    { $sample: { size: 15000 } },
    {
      $lookup: {
        'from': 'contacts',
        'localField': 'contracts.nodeID',
        'foreignField': '_id',
        'as': 'validContracts'
      }
    },
    {
      $match: {
        validContracts: {
          $size: 0
        }
      }
    }
  ]

  Shard.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(shards => {
    const shardIds = shards.map((curr) => curr._id)

    if (shardIds.length > 0)
      log.info('Total shards with 0 valid contracts to be removed: %s', shardIds.length)
    Shard.deleteMany({ _id: { $in: shardIds } }, (err) => callback(err))
  }).catch(err => callback(err));

}

Cleaner.prototype.clearShardsWithoutContracts = function (callback) {
  log.info('Clear shards without contracts')
  // This rare case, a shard entry in the database with an empty contracts' array
  // clearShardsWithAllInvalidContracts should have cleared this kind of shards already
  // This step should be removed if clearShardsWithAllInvalidContracts is 100% correct
  const Shard = this.storage.models.Shard
  const query = [
    { $match: { 'contracts': { $size: 0 } } }
  ]
  Shard.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(shards => {
    const shardIds = shards.map((curr) => curr._id)
    if (shardIds.length > 0)
      log.info('Total shards without contacts to be removed: %s', shardIds.length)
    Shard.deleteMany({ _id: { $in: shardIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearMirrors = function (callback) {
  async.waterfall([
    // this.clearMirrorsWithoutShards.bind(this),
    this.clearMirrorsWithoutContacts.bind(this)
  ], (err) => callback(err))
}

/**
 * WARNING: Do not clear mirrors without shards.
 */
Cleaner.prototype.clearMirrorsWithoutShards = function (callback) {
  log.info('Clearing mirrors without shards')
  const Mirror = this.storage.models.Mirror

  const currentDate = new Date();
  const oneWeekAgo = new Date(currentDate.setDate(currentDate.getDate() - 10));

  let query = [
    {
      '$match': { 'created': { '$lt': oneWeekAgo } }
    },
    {
      '$lookup': {
        'from': 'shards',
        'localField': 'shardHash',
        'foreignField': 'hash',
        'as': 'shards'
      }
    },
    {
      '$match': { 'shards': { '$size': 0 } }
    },
    { '$limit': 1000 }
  ]


  Mirror.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(mirrors => {
    const mirrorIds = mirrors.map((curr) => curr._id)
    log.info('Total mirrors without shards to be removed: %s', mirrorIds.length)
    Mirror.deleteMany({ _id: { $in: mirrorIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype.clearMirrorsWithoutContacts = function (callback) {
  log.info('Clearing mirrors without contacts')

  const Mirror = this.storage.models.Mirror
  const query = [
    {
      '$lookup': {
        'from': 'contacts',
        'localField': 'contact',
        'foreignField': '_id',
        'as': 'contact'
      }
    },
    { '$match': { 'contact': { '$size': 0 } } },
    { '$limit': 5000 }
  ]

  Mirror.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(mirrors => {
    const mirrorIds = mirrors.map((curr) => curr._id)
    log.info('Total mirrors without contacts to be removed: %s', mirrorIds.length)
    Mirror.deleteMany({ _id: { $in: mirrorIds } }, (err) => callback(err))
  }).catch(err => callback(err));
}

Cleaner.prototype._randomTime = function (max, min) {
  const range = max - min;

  assert(Number.isSafeInteger(range));
  assert(range > 0, 'maxInterval is expected to be greater than minInterval');

  const entropy = crypto.randomBytes(8).toString('hex');
  const offset = Math.round(parseInt('0x' + entropy) / Math.pow(2, 64) * range);

  return min + offset;
};

/**
 * Will wait and then call `run` after a random amount of time
 */
Cleaner.prototype.wait = function () {
  clearTimeout(this._timeout);

  const max = ms(this._config.application.maxInterval);
  const min = ms(this._config.application.minInterval);

  const milliseconds = this._randomTime(max, min);
  const minutes = Number(milliseconds / 1000 / 60).toFixed(2);

  log.info('Scheduling next round in %s minutes', minutes);

  this._timeout = setTimeout(() => this.run(), milliseconds);
};

/**
 * Handles uncaught exceptions
 * @private
 */
/* istanbul ignore next */
Cleaner.prototype._handleUncaughtException = function (err) {
  if (process.env.NODE_ENV === 'test') {
    throw err;
  }

  log.error('An unhandled exception occurred:', err);
  process.exit(1);
};

/**
 * Handles exit event from process
 * @private
 */
/* istanbul ignore next */
Cleaner.prototype._handleExit = function () {
  log.info('Cleaner service is shutting down');
};

/**
 * Postpones process exit until requests are fullfilled
 * @private
 */
/* istanbul ignore next */
Cleaner.prototype._handleSIGINT = function () {
  let waitTime = 0;

  log.info('Received shutdown signal, checking for running cleaner');
  setInterval(function () {
    waitTime += Cleaner.SIGINT_CHECK_INTERVAL;

    if (!this._running) {
      process.exit();
    }

    if (waitTime > Cleaner.MAX_SIGINT_WAIT) {
      process.exit();
    }
  }, Cleaner.SIGINT_CHECK_INTERVAL);
};

module.exports = Cleaner;
