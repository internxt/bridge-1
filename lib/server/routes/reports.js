'use strict';

const Router = require('./index');
const log = require('../../logger');
const middleware = require('storj-service-middleware');
const errors = require('storj-service-error-types');
const constants = require('../../constants');
const async = require('async');
const storj = require('storj-lib');
const limiter = require('../limiter').DEFAULTS;
const utils = require('../../utils');
const Monitor = require('../../monitor/index');
const MonitorConfig = require('../../monitor/config');
const ObjectId = require('mongoose').Types.ObjectId;

/**
 * Handles endpoints for reporting
 * @constructor
 * @extends {Router}
 */
class ReportsRouter extends Router {
  static _sortByTimeoutRate(a, b) {
    const a1 = a.contact.timeoutRate >= 0 ? a.contact.timeoutRate : 0;
    const b1 = b.contact.timeoutRate >= 0 ? b.contact.timeoutRate : 0;

    return (a1 === b1) ? 0 : (a1 > b1) ? 1 : -1;
  }

  static _sortByResponseTime(a, b) {
    const aTime = a.contact.responseTime || Infinity;
    const bTime = b.contact.responseTime || Infinity;

    return (aTime === bTime) ? 0 : (aTime > bTime) ? 1 : -1;
  }

  constructor(options) {
    super(options);
    this.getLimiter = middleware.rateLimiter(options.redis);

    this.monitor = new Monitor(MonitorConfig(this.config));
    this.monitor.setStorage(this.storage);
  }

  getContactById(nodeId, callback) {
    const { Contact } = this.storage.models;

    Contact.findOne({ _id: nodeId }, function (err, contact) {
      if (err) {
        return callback(new errors.InternalError(err.message));
      }

      if (!contact) {
        return callback(new errors.NotFoundError('Contact not found'));
      }

      callback(null, contact);
    });
  }

  async createExchangeReport(req, res, next) {
    const { ExchangeReport, Shard } = this.storage.models;
    const exchangeReport = new ExchangeReport(req.body);
    var projection = { hash: true, contracts: true };

    const shard = await Shard.findOne({ hash: exchangeReport.dataHash }, projection);

    if (!shard) {
      return next(new errors.NotFoundError('Shard not found for report'));
    }

    // TODO: Add signature verification

    this._handleExchangeReport(exchangeReport);

    exchangeReport.save((err) => {
      if (err) {
        return next(new errors.BadRequestError(err.message));
      }

      return res.status(201).send({});
    });
  }

  _handleExchangeReport(report) {
    const { dataHash, exchangeResultMessage } = report;

    switch (exchangeResultMessage) {
      case 'MIRROR_SUCCESS':
      case 'SHARD_UPLOADED':
      case 'MIRROR_FAILED':
      case 'TRANSFER_FAILED':
      case 'DOWNLOAD_ERROR':
      case 'SHARD_DOWNLOADED':
        this._triggerMirrorEstablish(dataHash).then(() => {
          log.info('Mirror established for shard %s', dataHash);
        }).catch(err => {
          switch (err.message) {
            case 'Shard already in cluster':
              break;
            default:
              log.error('Error establishing new mirror: %s', err.message);
          }
        });
        break;
      default:
        log.warn('Unknown exchange result code: %s', exchangeResultMessage);
    }
  }

  _loadShard(hash, cb) {
    this.contracts.load(hash, (err, shard) => cb(err, shard));
  }

  _getMirrors(shard, cb) {
    const { Mirror } = this.storage.models;

    return Mirror.find({ shardHash: shard.hash, contact: { $in: constants.CLUSTER } })
      .populate('contact')
      .exec((err, mirrors) => cb(err, shard, mirrors));
  }

  _isMirroringNeeded(shard, mirrors, cb) {
    const isInCluster = mirrors.filter((v) => constants.CLUSTER.indexOf(v) > -1 && v.isEstablished === true);

    if (isInCluster) {
      return cb(Error('Shard already in cluster'));
    }

    return cb(null, shard, mirrors);
  }

  _getMirrorCandidate(shard, mirrors, cb) {
    let established = [];
    let available = [];

    mirrors.forEach(m => {
      if (!m.contact) {
        return m.remove();
      }

      if (m.isEstablished) {
        established.push(m);
      } else {
        available.push(m);
      }
    });

    mirrors.sort(() => Math.random() - 0.5);

    if (available.length === 0) {
      return cb(new Error('No available mirrors, should ALLOC'));
    }

    if (established.length >= constants.M_REPLICATE) {
      return cb(new Error('Auto mirroring limit is reached'));
    }

    available.sort(utils.sortByReputation);

    cb(null, shard, available.shift());
  }

  _getRetrievalTokenFromFarmer(shard, mirror, cb) {
    let farmers = Object.keys(shard.contracts);
    let pointer = null;
    let test = (next) => next(null, farmers.length === 0 || pointer !== null);
    let contact = storj.Contact(mirror.contact.toObject());

    async.until(test, (done) => {
      this.getContactById(farmers.pop(), (err, result) => {
        if (err) {

          return done(err);
        }

        let farmer = storj.Contact(result.toObject());

        this.network.getRetrievalPointer(
          farmer,
          shard.getContract(farmer),
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
        return cb(new Error('Failed to get pointer'));
      }

      cb(null, shard, pointer, mirror, contact);
    });
  }

  _establishMirror(item, source, mirror, contact, cb) {
    this.network.getMirrorNodes(
      [source],
      [contact],
      (err) => {
        if (err) {
          return cb(err);
        }

        mirror.isEstablished = true;
        mirror.save();
        item.addContract(contact, storj.Contract(mirror.contract));
        this.contracts.save(item, cb);
      }
    );

  }

  _triggerMirrorEstablish(hash) {
    return async.waterfall([
      this._loadShard.bind(this, hash),
      this._getMirrors.bind(this),
      this._isMirroringNeeded,
      this._getMirrorCandidate,
      this._getRetrievalTokenFromFarmer.bind(this),
      this._establishMirror.bind(this)
    ]);
  }

  _definitions() {
    return [
      ['POST', '/reports/exchanges', this.getLimiter(limiter(1000)), middleware.rawbody, this.createExchangeReport.bind(this)],
      ['GET', '/reports/mirrors/:bucketEntry', this.getLimiter(limiter(100)), middleware.rawbody, this.getMirrorsListByObjectId.bind(this)]
    ];
  }
}

ReportsRouter.prototype._shardHasMirrors = function (hash, done) {
  const self = this;

  function _loadShard(callback) {
    self.contracts.load(hash, (err, /* _item */) => {
      if (err) {
        return callback(err);
      }

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

    if (available.length < 10) {
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
    };

    callback(null, finalResult);
  }

  async.waterfall([
    _loadShard,
    _getMirrors,
    _getMirrorCandidate
  ], done);
};

ReportsRouter.prototype.getMirrorsListByObjectId = function (req, res) {
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
      '$sort': {
        'pointers.index': 1
      }
    },
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
        'index': '$pointers.index',
        'parity': '$pointers.parity',
        'size': '$pointers.size',
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
        'total': { '$size': '$mirrors' },
        'farmers': '$shard.contracts.nodeID'
      }
    }
  ];


  this.storage.models.BucketEntry.aggregate(query).cursor({ batchSize: 2500 }).exec().toArray().then(results => {
    res.status(200).send(results);
  }).catch((err) => {
    res.status(500).send({ error: 'Cannot perform query', reason: err.message });
  });
};

ReportsRouter.prototype.getMirrorsList = function (req, res, next) {
  const bucketEntry = req.params.bucketentry;

  if (bucketEntry.length === 24) {
    return this.getMirrorsListByObjectId(req, res, next);
  }

  if (bucketEntry.length === 40) {
    return this.getMirrorsListByHash(req, res, next);
  }

  res.status(500).send({ error: 'Invalid identifier' });

};

module.exports = ReportsRouter;
