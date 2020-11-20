'use strict';

const Router = require('./index');
const log = require('../../logger');
const middleware = require('storj-service-middleware');
const errors = require('storj-service-error-types');
const inherits = require('util').inherits;
const BucketsRouter = require('./buckets');
const constants = require('../../constants');
const async = require('async');
const storj = require('storj-lib');
const limiter = require('../limiter').DEFAULTS;
const utils = require('../../utils');
const Monitor = require('../../monitor/index');
const MonitorConfig = require('../../monitor/config');
const ObjectId = require('mongoose').Types.ObjectId

/**
 * Handles endpoints for reporting
 * @constructor
 * @extends {Router}
 */
function ReportsRouter(options) {
  if (!(this instanceof ReportsRouter)) {
    return new ReportsRouter(options);
  }

  Router.apply(this, arguments);

  this.getLimiter = middleware.rateLimiter(options.redis);

  this.monitor = new Monitor(MonitorConfig(this.config));
  this.monitor.setStorage(this.storage)
}

inherits(ReportsRouter, Router);

/**
 * Creates an exchange report
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
ReportsRouter.prototype.createExchangeReport = function (req, res, next) {
  const self = this;
  const { ExchangeReport, Shard, Mirror } = this.storage.models
  var exchangeReport = new ExchangeReport(req.body);
  var projection = { hash: true, contracts: true };

  Shard.find({ hash: exchangeReport.dataHash }, projection, function (err, shards) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!shards || !shards.length) {
      return next(new errors.NotFoundError('Shard not found for report'));
    }

    // TODO: Add signature/identity verification

    // NB: Kick off mirroring if needed
    self._handleExchangeReport(exchangeReport, (err) => {
      if (err) {
        switch (err.message) {
          case 'Failed to get pointer':
          case 'All mirrors requests failed':
          case 'No available mirrors':
            // Try again
            /*
            setTimeout(() => {
              console.log('TRY AGAIN')
              self._handleExchangeReport(exchangeReport, (err) => { })
            }, 15000)
            */
            break;
          case 'Auto mirroring limit is reached':
            break;
          default:
            return
          // return log.warn('Exchange Report Error shard %s: %s', exchangeReport.dataHash, err.message);
        }
      }
    });

    exchangeReport.save(function (err) {
      if (err) {
        return next(new errors.BadRequestError(err.message));
      }

      res.status(201).send({});
    });
  });
};

/**
 * @private
 */
ReportsRouter.prototype._handleExchangeReport = function (report, callback) {
  const { dataHash, exchangeResultMessage } = report;

  switch (exchangeResultMessage) {
    case 'MIRROR_SUCCESS':
    case 'SHARD_UPLOADED':
    case 'MIRROR_FAILED':
    case 'TRANSFER_FAILED':
    case 'DOWNLOAD_ERROR':
      this._triggerMirrorEstablish(constants.M_REPLICATE, dataHash, callback);
      break;
    case 'SHARD_DOWNLOADED':
      this._triggerMirrorEstablish(constants.M_REPLICATE, dataHash, callback);
      break;
    default:
      callback(new Error('Exchange result type will not trigger action'));
  }
};

ReportsRouter._sortByTimeoutRate = function (a, b) {
  const a1 = a.contact.timeoutRate >= 0 ? a.contact.timeoutRate : 0;
  const b1 = b.contact.timeoutRate >= 0 ? b.contact.timeoutRate : 0;
  return (a1 === b1) ? 0 : (a1 > b1) ? 1 : -1;
};

ReportsRouter._sortByResponseTime = function (a, b) {
  const aTime = a.contact.responseTime || Infinity;
  const bTime = b.contact.responseTime || Infinity;
  return (aTime === bTime) ? 0 : (aTime > bTime) ? 1 : -1;
};

/**
 * Loads some mirrors for the hash and establishes them
 * @private
 */
ReportsRouter.prototype._triggerMirrorEstablish = function (n, hash, done) {
  const self = this;
  let item = null;

  function _loadShard(callback) {
    self.contracts.load(hash, (err, _item) => {
      if (err) {
        return callback(err);
      }
      item = _item;
      callback();
    });
  }

  function _getMirrors(callback) {
    self.storage.models.Mirror.find({ shardHash: hash })
      .populate('contact')
      .exec((err, mirrors) => callback(err, hash, mirrors));
  }

  function _getMirrorCandidate(shard, mirrors, callback) {
    let established = [], available = [];

    mirrors.forEach((m) => {
      if (!m.contact) {
        // Clean up invalid mirrors: Those with no contacts.
        // log.warn('Reports 1: Mirror %s is missing contact in database. Mirror destroyed.', m._id);
        m.remove()
      } else if (!m.isEstablished) {
        available.push(m);
      } else {
        established.push(m);
      }
    });

    mirrors.sort(() => Math.random() - 0.5)

    // If not enough mirrors AND not enough available mirrors, publish new contracts
    if (established.length < n && available.length - established.length < n) {
      self.monitor.actions.publishNewContractsForShard(shard, (err) => {
        if (err) {
        } else {
          setTimeout(() => {
            self._triggerMirrorEstablish(constants.M_REPLICATE, shard, () => { })
          }, 10000)
        }
      });
    }

    if (available.length === 0) {
      return callback(new Error('No available mirrors'));
    }

    if (established.length > n) {
      return callback(new Error('Auto mirroring limit is reached'));
    }

    available.sort(utils.sortByReputation);

    callback(null, available.shift());
  }

  function _getRetrievalTokenFromFarmer(mirror, callback) {
    let farmers = Object.keys(item.contracts);
    let pointer = null;
    let test = (next) => next(null, farmers.length === 0 || pointer !== null);
    let contact = storj.Contact(mirror.contact.toObject());

    async.until(test, (done) => {
      self.getContactById(farmers.pop(), (err, result) => {
        if (err) {
          return done();
        }

        let farmer = storj.Contact(result.toObject());

        self.network.getRetrievalPointer(
          farmer,
          item.getContract(farmer),
          (err, result) => {
            // NB: Make sure that we don't set pointer to undefined
            // instead of null that would trigger the until loop to quit
            if (err) {
              // log.warn('Unable to get pointer for mirroring, reason: %s', err.message);
            } else {
              pointer = result;
            }
            done();
          }
        );
      });
    }, () => {
      if (!pointer) {
        return callback(new Error('Failed to get pointer'));
      }

      callback(null, pointer, mirror, contact);
    });
  }

  function _establishMirror(source, mirror, contact, callback) {
    self.network.getMirrorNodes(
      [source],
      [contact],
      (err) => {
        if (err) { return callback(err); }

        mirror.isEstablished = true;
        mirror.save();
        item.addContract(contact, storj.Contract(mirror.contract));
        self.contracts.save(item, callback);
      }
    );
  }

  async.waterfall([
    _loadShard,
    _getMirrors,
    _getMirrorCandidate,
    _getRetrievalTokenFromFarmer,
    _establishMirror
  ], done);
};

ReportsRouter.prototype._shardHasMirrors = function (hash, done) {
  const self = this;
  let item = null;
  const n = 6;

  function _loadShard(callback) {
    self.contracts.load(hash, (err, _item) => {
      if (err) {
        return callback(err);
      }
      item = _item;
      callback();
    });
  }

  function _getMirrors(callback) {
    self.storage.models.Mirror.find({ shardHash: hash })
      .populate('contact')
      .exec((err, mirrors) => callback(err, hash, mirrors));
  }

  function _getMirrorCandidate(shard, mirrors, callback) {
    let established = [], available = [];

    mirrors.forEach(m => {
      if (!m.contact) {
        log.warn('Reports 2: Mirror %s is missing contact in database. Mirror destroyed.', m._id);
        m.remove();
      } else if (!m.isEstablished) {
        available.push(m);
      } else {
        established.push(m);
      }
    });

    if (available.length + established.length + n / 2 < n) {
      self.monitor.actions.publishNewContractsForShard(shard, (err) => {
        if (err) {
          log.warn('Could not publish new mirrors for shard %s', shard);
        } else {
          log.info('New mirrors for shard %s published', shard);
        }
      });
    }

    const finalResult = {
      total: mirrors.length,
      available: available.length,
      established: established.length
    }

    callback(null, finalResult);
  }

  async.waterfall([
    _loadShard,
    _getMirrors,
    _getMirrorCandidate
  ], done);
}

/**
 * @private
 */
ReportsRouter.prototype.getContactById = BucketsRouter.prototype.getContactById;

ReportsRouter.prototype.getMirrorsListByHash = function (req, res, next) {
  const shardHash = req.params.bucketEntry;
}

ReportsRouter.prototype.getMirrorsListByObjectId = function (req, res, next) {
  const bucketEntry = req.params.bucketEntry;

  const query = [
    { '$match': { '_id': ObjectId(bucketEntry) } },
    {
      '$lookup': {
        'from': 'frames',
        'localField': 'frame',
        'foreignField': '_id',
        'as': 'frame'
      }
    },
    { '$unwind': { 'path': '$frame' } },
    { '$unwind': { 'path': '$frame.shards' } },
    {
      '$lookup': {
        'from': 'pointers',
        'localField': 'frame.shards',
        'foreignField': '_id',
        'as': 'pointers'
      }
    },
    { '$unwind': { 'path': '$pointers' } },
    {
      '$lookup': {
        'from': 'shards',
        'localField': 'pointers.hash',
        'foreignField': 'hash',
        'as': 'shard'
      }
    },
    { '$unwind': { 'path': '$shard' } },
    {
      '$lookup': {
        'from': 'mirrors',
        'localField': 'shard.hash',
        'foreignField': 'shardHash',
        'as': 'mirrors'
      }
    },
    {
      '$project': {
        '_id': '$shard._id',
        'hash': '$shard.hash',
        'contracts': { '$size': '$shard.contracts' },
        'established': {
          '$size': {
            '$filter': {
              'input': '$mirrors',
              'as': 'm',
              'cond': { '$eq': ['$$m.isEstablished', true] }
            }
          }
        },
        'nonestablished': {
          '$size': {
            '$filter': {
              'input': '$mirrors',
              'as': 'm',
              'cond': { '$eq': ['$$m.isEstablished', false] }
            }
          }
        },
        'total': { '$size': '$mirrors' }
      }
    }
  ]


  this.storage.models.BucketEntry.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(results => {
    res.status(200).send(results)
  }).catch(err => {
    res.status(500).send({ error: 'Cannot perform query' })
  })
}

ReportsRouter.prototype.getMirrorsList = function (req, res, next) {
  const bucketEntry = req.params.bucketentry;

  if (bucketEntry.length === 24) {
    return this.getMirrorsListByObjectId(req, res, next);
  }

  if (bucketEntry.length === 40) {
    return this.getMirrorsListByHash(req, res, next);
  }

  res.status(500).send({ error: 'Invalid identifier' })

}

/**
 * @private
 */
ReportsRouter.prototype._definitions = function () {
  return [
    ['POST', '/reports/exchanges', this.getLimiter(limiter(1000)), middleware.rawbody, this.createExchangeReport.bind(this)],
    ['GET', '/reports/mirrors/:bucketentry', this.getLimiter(limiter(100)), middleware.rawbody, this.getMirrorsList.bind(this)]
  ];
};

module.exports = ReportsRouter;
