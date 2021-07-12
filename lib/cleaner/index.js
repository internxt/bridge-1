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
    { logger: log }
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
    // this.clearOldExchangeReports.bind(this),
    this.clearUsers.bind(this),
    this.clearBuckets.bind(this),
    this.clearBucketEntries.bind(this),
    // this.clearFramesWithoutBucketEntries.bind(this),
    this.clearFramesWithoutUser.bind(this),
    this.clearPointers.bind(this),
    // this.clearInvalidContracts.bind(this),
    // this.clearContacts.bind(this),
    this.clearShards.bind(this),
    this.clearMirrors.bind(this)
  ], finish);

};

Cleaner.prototype.clearOldExchangeReports = function (callback) {
  const { ExchangeReport } = this.storage.models;

  log.info('Removing old exchange reports');

  ExchangeReport.deleteMany({ created: { $lt: new Date() - ms('4 days') } }, (err, result) => {
    if (err) {
      callback(err);
    } else {
      log.info('Deleted %s old exchange reports', result.deletedCount);
      callback();
    }
  });
};

Cleaner.prototype.clearUsers = function (callback) {
  callback();
};

Cleaner.prototype.clearBuckets = function (callback) {
  log.info('Remove buckets of non-existent users');
  const { Bucket } = this.storage.models;

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
        'user': { '$size': 0 }
      }
    }
  ];

  let totalBucketsDeleted = 0;
  AggregationCursor(Bucket, query, (bucket, nextBucket) => {
    totalBucketsDeleted++;
    Bucket
      .deleteOne({ _id: { $in: bucket._id } })
      .then(() => nextBucket(null, bucket))
      .catch((err) => nextBucket(err));
  }, (err) => {
    log.info('Total buckets deleted: %s', totalBucketsDeleted);
    callback(err);
  });
};

Cleaner.prototype.clearBucketEntries = function (callback) {
  log.info('Check bucketentries of non existent buckets');
  const { BucketEntry } = this.storage.models;
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
        'bucket': { '$size': 0 }
      }
    }
  ];

  let totalBucketEntriesDeleted = 0;
  AggregationCursor(BucketEntry, query, (bucketEntry, nextBucketEntry) => {
    totalBucketEntriesDeleted++;
    BucketEntry
      .deleteOne({ _id: bucketEntry._id })
      .then(() => nextBucketEntry(null, bucketEntry))
      .catch((err) => nextBucketEntry(err));
  }, (err) => {
    log.info('Total BucketEntries deleted: %s', totalBucketEntriesDeleted);
    callback(err);
  });
};

Cleaner.prototype.clearFramesWithoutBucketEntries = function (callback) {
  log.warn('Check frames of non existent bucketentries');
  const { Frame, Pointer } = this.storage.models;

  // Frames within last 24h should not be deleted
  // We can assume > 2 day is a dead frame
  // Filter by last week to improve query speed
  const query = [
    {
      '$match': {
        '_id': {
          '$lt': ObjectId.createFromTime(new Date() - ms('2d'))
        },
        locked: true,
        bucketEntry: {
          $exists: true
        }
      }
    },
    {
      '$lookup': {
        'from': 'bucketentries',
        'localField': 'bucketEntry',
        'foreignField': '_id',
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
      log.debug('Deleting frame/pointer %s without bucketentry', data._id);
      async.parallel([
        next => Frame.deleteOne({ _id: data._id }, (err) => next(err)),
        next => Pointer.deleteMany({ _id: { $in: data.shards } }, (err) => next(err))
      ], (err) => {
        if (err) {
          log.error('clearFramesWithoutBucketEntries(): Error deleting frames/pointers: %s', err.message);
        }
        // Show must go on
        nextData(null, data);
      });
    } else {
      nextData(null, data);
    }
  }, (err) => callback(/* err */));
};

Cleaner.prototype.clearFramesWithoutUser = function (callback) {
  log.info('Check frames of non existent users');
  const { Frame, Pointer } = this.storage.models;

  const query = [
    {
      '$match': {
        'created': {
          '$lt': new Date() - ms('1d')
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
    Frame.findOne({ _id: data._id }, (err) => {
      if (err) {
        log.error('Cannot find frame %s', data._id);

        return nextData();
      }
      log.debug('Deleting frame/pointer %s', data._id);
      async.parallel([
        next => Frame.deleteOne({ _id: data._id }, (err) => next(err)),
        next => Pointer.deleteMany({ _id: { $in: data.shards } }, (err) => next(err))
      ], (err) => {
        if (err) {
          log.error('clearFramesWithoutUser(): Error deleting frames/pointers: %s', err.message);
        }
        // Show must go on
        nextData(null, data);
      });
    });
  }, (err) => callback(err));
};

Cleaner.prototype.clearPointers = function (callback) {
  log.warn('Check pointers of non existent frames (> 2 days)');
  const { Pointer } = this.storage.models;
  // Look up pointers with no frames
  // Foreign field "shards" is not index on Frame collection, so this query may take a while.
  // TODO: Pointers should be deleted with its frame in the previous step (clearFramesWithoutUser)

  const query = [
    {
      '$match': {
        '_id': {
          '$lt': ObjectId.createFromTime(new Date() - ms('2 days'))
        },
        'frame': {
          '$exists': true
        }
      }
    },
    {
      '$lookup': {
        'from': 'frames',
        'localField': 'frame',
        'foreignField': '_id',
        'as': 'frames'
      }
    },
    {
      '$match': {
        'frames': {
          '$size': 0
        }
      }
    }
  ];

  let totalPointersDeleted = 0;
  AggregationCursor(Pointer, query, (pointer, nextPointer) => {
    if (pointer) {
      totalPointersDeleted++;
      Pointer.deleteOne({ _id: pointer._id }).then(() => nextPointer(null, pointer)).catch((err) => nextPointer(err));
    } else {
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
              '$lte': new Date(new Date() - ms('60d'))
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
    if (contactIds.length > 0) {
      log.info('Total contacts to be removed: %s', contactIds.length);
    }
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
  const { Shard, Mirror } = this.storage.models;

  const query = [
    {
      '$sample': {
        'size': 800
      }
    },
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

  return AggregationCursor(Shard, query, (shard, nextShard) => {
    console.log('Delete shard without pointer %s with hash %s', shard._id, shard.hash);
    async.parallel([
      (next) => Shard.deleteMany({ _id: shard._id }).then(() => next()).catch(err => next(err)),
      (next) => Mirror.deleteMany({ shardHash: shard.hash }).then(() => next()).catch(err => next(err))
    ], () => nextShard());
  }, () => {
    callback();
  });
};

Cleaner.prototype.clearInvalidContracts = function (callback) {
  log.info('Clear contracts from missing contacts');
  // Remove contracts from shard of missing farmers
  const { Shard, Mirror } = this.storage.models;

  const query = [
    {
      '$sort': {
        '_id': 1
      }
    }, {
      '$unwind': {
        'path': '$contracts'
      }
    }, {
      '$lookup': {
        'from': 'contacts',
        'localField': 'contracts.nodeID',
        'foreignField': '_id',
        'as': 'contact'
      }
    }, {
      '$match': {
        '$or': [
          {
            'contact': {
              '$size': 0
            }
          }, {
            'contact.timeoutRate': {
              '$gt': 0.04
            }
          }
        ]
      }
    }, {
      '$limit': 3000
    }
  ];

  const cursor = AggregationCursor(Shard, query, async (data, nextData) => {
    const nodeID = data.contracts.nodeID;
    const shardId = data._id;

    await Shard.update({ _id: shardId }, {
      $pull: {
        'challenges': { nodeID: nodeID },
        'trees': { nodeID: nodeID },
        'contracts': { nodeID: nodeID }
      }
    });

    await Mirror.deleteMany({ contact: nodeID });
    nextData(null, data);
  }, (err) => callback(err));

  cursor.on('error', () => console.log('cursor error'));

  cursor.on('end', () => console.log('cursor end'));

  cursor.on('close', () => console.log('cursor close'));

};

Cleaner.prototype.clearMirrors = function (callback) {
  async.waterfall([
    this.clearMirrorsWithoutContacts.bind(this)
  ], (err) => callback(err));
};

Cleaner.prototype.clearMirrorsWithoutContacts = function (callback) {
  log.info('Clearing mirrors without contacts');

  // Should delete contract from linked shard
  const { Mirror } = this.storage.models;
  const query = [
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
    }
  ];

  AggregationCursor(Mirror, query, (mirror, nextMirror) => {
    log.warn('Remove mirror without contact %s', mirror._id);
    Mirror.deleteMany({ _id: mirror._id }).then(() => nextMirror(null, mirror)).catch((err) => nextMirror(err));
  }, (err) => callback(err));
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

  log.error('An unhandled exception occurred: %s', err.message);
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

  log.info('Received shutdown signal, checking for running cleaner.');
  setInterval(() => {
    waitTime += Cleaner.SIGINT_CHECK_INTERVAL;

    if (!this._running) {
      log.info('Closing database connection...');
      this.storage.connection.close(() => {
        process.exit();
      });
    }

    if (waitTime > Cleaner.MAX_SIGINT_WAIT) {
      log.info('Cleaner waited too long, force close.');
      process.exit();
    }
  }, Cleaner.SIGINT_CHECK_INTERVAL);
};

module.exports = Cleaner;
