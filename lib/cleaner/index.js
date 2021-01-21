'use strict';

const async = require('async');
const assert = require('assert');
const MonitorConfig = require('../monitor/config');
const Storage = require('storj-service-storage-models');
const ms = require('ms');
const log = require('../logger');
const { ObjectId } = require('mongoose').Types;
const { AggregationCursor, randomTime } = require('../utils');
const ComplexClient = require('storj-complex').createClient;
const Renewal = require('../renewal');

/**
 * A long running daemon that will inspect database looking for orphan
 * documents and perform a clean up.
 * It can be performed once a day.
 * @param {CleanerConfig} config - An instance of CleanerConfig
 */
class Cleaner {
  constructor(config) {
    assert(config instanceof MonitorConfig, 'Invalid config supplied');

    this.storage = null;

    this.currentObjectId = new ObjectId('5eff5d44ff7b1e6c8fb13d4d');

    this._config = config;
    this._timeout = null;
    this._running = false;
  }
}

Cleaner.SIGINT_CHECK_INTERVAL = 1000;
Cleaner.MAX_SIGINT_WAIT = 5000;

/**
 * Starts the Bridge instance
 * @param {Function} callback
 */
Cleaner.prototype.start = function (callback) {
  log.info('Cleaner service is starting');
  log.debug('Debug is activated');

  this.storage = new Storage(
    this._config.storage.mongoUrl,
    this._config.storage.mongoOpts,
    {
      logger: log
    }
  );

  this.network = new ComplexClient(this._config.complex);
  this.renewal = new Renewal();
  this.renewal.build(this.network, this.storage);

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

  const finish = (err) => {
    if (err) {
      log.error('Cleaner finish error: ' + err);
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
    this.clearFramesWithoutBucketEntries.bind(this),
    this.clearFramesWithoutUser.bind(this),
    this.clearPointers.bind(this),
    this.clearInvalidContracts.bind(this),
    this.clearContacts.bind(this),
    this.clearShards.bind(this),
    this.clearMirrors.bind(this)
  ], finish);

};

Cleaner.prototype.clearUsers = function (callback) {
  // TODO: Clear users who:
  // - Older than 1 week & not activated
  // - Older than 1 year + 1 week & no buckets
  // log.info('Check invalid user accounts');
  callback();
};

Cleaner.prototype.clearBuckets = function (callback) {
  log.info('Remove buckets of missing user accounts');
  const Bucket = this.storage.models.Bucket;

  const query = [
    {
      '$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user'
      }
    },
    {
      '$match': {
        'user': {
          '$size': 0
        }
      }
    },
    {
      '$project': {
        '_id': '$_id'
      }
    }
  ];

  Bucket.aggregate(query).cursor({
    batchSize: 2500
  }).exec().toArray().then(results => {
    const bucketIds = results.map((curr) => curr._id);
    if (bucketIds.length > 0)
      log.info('Total buckets without user to be removed: %s', bucketIds.length);
    Bucket.deleteMany({
      _id: {
        $in: bucketIds
      }
    }, (err) => callback(err));
  }).catch(err => callback(err));
};

Cleaner.prototype.clearBucketEntries = function (callback) {
  log.info('Check bucket entries of non existent buckets');
  const BucketEntry = this.storage.models.BucketEntry;
  const query = [
    {
      '$lookup': {
        'from': 'buckets',
        'localField': 'bucket',
        'foreignField': '_id',
        'as': 'bucket'
      }
    },
    {
      '$match': {
        'bucket': {
          '$size': 0
        }
      }
    },
    {
      '$project': {
        '_id': '$_id'
      }
    }
  ];

  BucketEntry.aggregate(query).cursor({
    batchSize: 2500
  }).exec().toArray().then(function (results) {
    const bucketEntryIds = results.map((curr) => curr._id);
    if (bucketEntryIds.length > 0)
      log.info('Total bucket entries without bucket to be removed: %s', bucketEntryIds.length);
    BucketEntry.deleteMany({
      _id: {
        $in: bucketEntryIds
      }
    }, (err) => callback(err));
  }).catch(err => callback(err));
};

Cleaner.prototype.clearFramesWithoutBucketEntries = function (callback) {
  log.warn('Check frames of non existent bucketentries');
  const {
    Frame, Pointer
  } = this.storage.models;

  // Frames within last 24h should not be deleted
  // We can assume > 2 day is a dead frame
  // Filter by last week to improve query speed
  const query = [
    {
      '$match': {
        '$and': [
          {
            'created': {
              '$gt': new Date(new Date() - ms('14d')).toISOString()
            }
          },
          {
            'created': {
              '$lt': new Date(new Date() - ms('2d')).toISOString()
            }
          }]
      }
    },
    {
      '$lookup': {
        'from': 'bucketentries',
        'localField': '_id',
        'foreignField': 'frame',
        'as': 'bucketentry'
      }
    },
    {
      '$match': {
        'bucketentry': {
          '$size': 0
        }
      }
    }
  ];

  AggregationCursor(Frame, query, (data, nextData) => {
    if (data) {
      log.debug('Deleting frame/pointer %s', data._id);
      async.parallel([
        next => Frame.deleteOne({
          _id: data._id
        }, (err) => next(err)),
        next => Pointer.deleteMany({
          _id: {
            $in: data.shards
          }
        }, (err) => next(err))
      ], (err) => {
        if (err) {
          log.error('Error deleting frames/pointers: %s', err.message);
        }
        // Show must go on
        nextData(null, data);
      });
    }
    else {
      nextData(null, data);
    }
  }, (err) => callback(err));
};

Cleaner.prototype.clearFramesWithoutUser = function (callback) {
  log.info('Check frames of non existent users');
  const {
    Frame, Pointer
  } = this.storage.models;

  const dayAgo = new Date(new Date() - ms('1d'));

  const query = [
    {
      '$match': {
        'created': {
          '$lt': dayAgo
        }
      }
    },
    {
      '$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user'
      }
    },
    {
      '$match': {
        'user': {
          '$size': 0
        }
      }
    },
    {
      '$project': {
        '_id': '$_id'
      }
    }
  ];

  AggregationCursor(Frame, query, (data, nextData) => {
    if (data) {
      Frame.findOne({
        _id: data._id
      }, (err, frame) => {
        if (err) {
          log.error('Cannot find frame %s', data._id);
          nextData();
        } else {
          log.debug('Deleting frame/pointer %s', data._id);
          async.parallel([
            next => Frame.deleteOne({
              _id: data._id
            }, (err) => next(err)),
            next => Pointer.deleteMany({
              _id: {
                $in: data.shards
              }
            }, (err) => next(err))
          ], (err) => {
            if (err) {
              log.error('Error deleting frames/pointers: %s', err.message);
            }
            // Show must go on
            nextData(null, data);
          });
        }
      });
    }
    else {
      nextData(err, data);
    }
  }, (err) => callback(err));
};

Cleaner.prototype.clearPointers = function (callback) {
  log.warn('Check pointers of non existent frames (use sample)');
  const Pointer = this.storage.models.Pointer;
  // Look up pointers with no frames
  // Foreign field "shards" is not index on Frame collection, so this query may take a while.
  // TODO: Pointers should be deleted with its frame in the previous step (clearFramesWithoutUser)

  // TODO: Should match old shards
  const query = [
    {
      '$sample': {
        size: 100
      }
    },
    {
      '$lookup': {
        'from': 'frames',
        'localField': '_id',
        'foreignField': 'shards',
        'as': 'shards'
      }
    },
    {
      '$match': {
        'shards': {
          '$size': 0
        }
      }
    },
    {
      '$project': {
        '_id': '$_id'
      }
    }
  ];

  let totalPointersDeleted = 0;
  AggregationCursor(Pointer, query, (pointer, nextPointer) => {
    if (pointer) {
      totalPointersDeleted++;
      Pointer.deleteOne({
        _id: pointer._id
      }, (err) => nextPointer(err, pointer));
    }
    else {
      nextPointer();
    }
  }, (err) => {
    log.info('Total pointers removed: %s', totalPointersDeleted);
    callback(err);
  });
};

Cleaner.prototype.clearContacts = function (callback) {
  log.info('Check invalid nodes');
  const Contact = this.storage.models.Contact;
  const timeoutRateThreshold = this._config.application.timeoutRateThreshold;

  const query = [
    {
      '$match': {
        '$and': [
          {
            'timeoutRate': {
              '$gte': timeoutRateThreshold
            }
          },
          {
            'lastSeen': {
              '$lte': new Date(new Date() - ms('10d'))
            }
          }
        ]
      }
    }
  ];

  Contact.aggregate(query).cursor({
    batchSize: 2500
  }).exec().toArray().then(contacts => {
    const contactIds = contacts.map((curr) => curr._id);
    if (contactIds.length > 0)
      log.info('Total contacts to be removed: %s', contactIds.length);
    Contact.deleteMany({
      _id: {
        $in: contactIds
      }
    }, (err) => callback(err));
  }).catch(err => callback(err));
};

Cleaner.prototype.clearShards = function (callback) {
  log.info('Checking shards');
  async.waterfall([
    this.clearShardsWithoutPointers.bind(this)
  ], (err) => callback(err));
};

Cleaner.prototype.clearShardsWithoutPointers = function (callback) {
  log.info('Check shards of non existent pointers');
  const {
    Shard, Mirror
  } = this.storage.models;
  const query = [
    {
      '$lookup': {
        'from': 'pointers',
        'localField': 'hash',
        'foreignField': 'hash',
        'as': 'pointer'
      }
    },
    {
      '$match': {
        'pointer': {
          '$size': 0
        }
      }
    }
  ];

  Shard.aggregate(query).cursor({
    batchSize: 2500
  }).exec().toArray().then(shards => {
    const shardIds = shards.map((curr) => curr._id);
    const shardHashes = shards.map((curr) => curr.hash);

    if (shardIds.length > 0) {
      log.info('Total shards without pointers to be removed: %s', shardIds.length);
      log.info('Invalidating contracts');
    }

    async.eachSeries(shardHashes, (shardHash, nextShardHash) => {
      this.renewal.invalidateContract(shardHash, () => {
        nextShardHash();
      });
    }, () => {
      async.parallel([
        next => Shard.deleteMany({
          _id: {
            $in: shardIds
          }
        }, (err) => next(err)),
        next => Mirror.deleteMany({
          shardHash: {
            $in: shardHashes
          }
        }, (err) => next(err))
      ], (err) => callback(err));
    });
  }).catch(err => callback(err));
};

Cleaner.prototype.clearInvalidContracts = function (callback) {
  log.info('Clear contracts from missing contacts from %s', this.currentObjectId);
  // Look for shards and inspect each contract, delete those invalid contracts from shard
  // Shard will be removed in the next steps if the total contracts is zero
  const {
    Shard, Contact, Mirror
  } = this.storage.models;

  const cursor = Shard.find({
    _id: {
      $gt: this.currentObjectId
    }
  }).sort({
    _id: 1
  }).limit(1500).cursor();
  const flag = this.currentObjectId;

  cursor
    .on('error', (err) => {
      console.log('Cursor error');
      callback(err);
    })
    .on('data', data => {
      cursor.pause();
      this.currentObjectId = new ObjectId(data._id);

      const contracts = data.contracts;
      const currentShardHash = data.hash;

      /**
             * If contract from shard is owned by a missing node,
             * will be removed from the shard document.
             * It means shard has less mirrors.
             */

      async.eachSeries(contracts, (contract, nextContract) => {
        const nodeID = contract.nodeID;
        Contact.findOne({
          _id: nodeID
        }, (err, contact) => {
          if (err) {
            log.error('Error finding contact', err);
            nextContract(err);
          }
          else {
            if (!contact) {
              log.warn('REMOVE MIRROR/SHARD with hash: %s', currentShardHash);
              async.parallel([
                next => Shard.updateOne({
                  _id: this.currentObjectId
                }, {
                  $pull: {
                    contracts: {
                      nodeID: nodeID
                    }
                  }
                }, (err) => next(err)),
                next => Mirror.deleteMany({
                  shardHash: currentShardHash
                }, (err) => next(err))
              ], (err) => {
                nextContract();
              });
            } else {
              nextContract();
            }
          }
        });
      }, (err) => {
        if (err) {
          log.error('Cannot end async contracts search', err);
        }
        cursor.resume();
      });

    }).on('end', () => {
      log.info('Last shard checked %s', this.currentObjectId);
      if (this.currentObjectId === flag) {
        log.info('Last shard reached, starting from 0');
        this.currentObjectId = new ObjectId('000000000000000000000000');
      }
      callback();
    });

};

Cleaner.prototype.clearMirrors = function (callback) {
  async.waterfall([
    // this.clearMirrorsWithoutShards.bind(this),
    this.clearMirrorsWithoutContacts.bind(this)
  ], (err) => callback(err));
};

// REMOVED clear mirrors without shards. Mirrors are created first, before the shard.

Cleaner.prototype.clearMirrorsWithoutContacts = function (callback) {
  log.info('Clearing mirrors without contacts (use sample)');

  // Should delete contract from linked shard
  const {
    Mirror, Shard
  } = this.storage.models;
  const query = [
    {
      $sample: {
        size: 500
      }
    },
    {
      '$lookup': {
        'from': 'contacts',
        'localField': 'contact',
        'foreignField': '_id',
        'as': 'contact'
      }
    },
    {
      '$match': {
        'contact': {
          '$size': 0
        }
      }
    },
    {
      '$limit': 500
    }
  ];

  Mirror.aggregate(query).cursor({
    batchSize: 2500
  }).exec().toArray().then(mirrors => {
    const mirrorIds = mirrors.map((curr) => curr._id);
    const mirrorHashes = mirrors.map(curr => curr.shardHash);
    const mirrorContact = mirrors.map(curr => curr.contact);
    log.info('Total mirrors without contacts to be removed: %s', mirrorIds.length);

    async.parallel([
      next => Mirror.deleteMany({
        _id: {
          $in: mirrorIds
        }
      }, (err) => next(err)),
      // next => Shard.updateOne({ _id: this.currentObjectId }, { $pull: { contracts: { nodeID: nodeID } } }, (err) => next(err)),
      next => {
        // TODO: If you remove a mirror, you should delete the shard too.
        /*
                Shard.updateMany(
                    { hash: { $in: mirrorHashes } },
                    { $pull: { contracts: { nodeID: { $in: mirrorContact } } } },
                    { multi: true },
                    (err) => next(err))
                */
        next();
      }
    ], (err) => callback(err));
  }).catch(err => callback(err));
};

/**
 * Will wait and then call `run` after a random amount of time
 */
Cleaner.prototype.wait = function () {
  clearTimeout(this._timeout);

  const max = ms(this._config.application.maxInterval);
  const min = ms(this._config.application.minInterval);

  const milliseconds = randomTime(max, min);
  const minutes = Number(milliseconds / 1000 / 60).toFixed(2);

  log.info('Scheduling next round in %s minutes', minutes);

  this._timeout = setTimeout(() => this.run(), 6000);
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
