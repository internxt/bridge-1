'use strict';

const async = require('async');
const storj = require('storj-lib');
const middleware = require('storj-service-middleware');
const crypto = require('crypto');
const authenticate = middleware.authenticate;
const errors = require('storj-service-error-types');
const Router = require('./index');
const inherits = require('util').inherits;
const ms = require('ms');
const log = require('../../logger');
const constants = require('../../constants');
const { MAX_SHARD_SIZE } = require('storj-service-storage-models').constants;
const analytics = require('storj-analytics');
const limiter = require('../limiter').DEFAULTS;
const _ = require('lodash');

/**
 * Handles endpoints for all frame/file staging related operations
 * @constructor
 * @extends {Router}
 */
function FramesRouter(options) {
  if (!(this instanceof FramesRouter)) {
    return new FramesRouter(options);
  }

  Router.apply(this, arguments);
  this._defaults = options.config.application;
  this._verify = authenticate(this.storage);
  this.getLimiter = middleware.rateLimiter(options.redis);
}

inherits(FramesRouter, Router);

/**
 * Creates a file staging frame
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
FramesRouter.prototype.createFrame = async function (req, res, next) {
  const Frame = this.storage.models.Frame;

  let spaceLeft = await userHasFreeSpaceLeft(this.storage, req.user.email);
  if (!spaceLeft.canUpload) {
    // log.warn('User %s exceeded storage limit, no uploads allowed', req.user.id);

    return next(new errors.TransferRateError('Max. space used'));
  }

  analytics.track(req.headers.dnt, {
    userId: req.user.uuid,
    event: 'Frame Created'
  });

  Frame.create(req.user, function (err, frame) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }
    res.send(frame.toObject());
  });
};


/**
 * Negotiates a contract and updates persistence for the given contract data
 * @private
 * @param {storj.Contract} contract - The contract object to publish
 * @param {storj.AuditStream} audit - The audit object to add to persistence
 * @param {Array} blacklist - Do not accept offers from these nodeIDs
 * @param {Object} res - The associated response
 * @param {Function} callback - Called with error or (farmer, contract)
 */
FramesRouter.prototype._getContractForShardSIP6 = function (contr, audit, bl, res, done) {
  this._selectFarmers(bl, (err, farmers) => {
    if (err) {
      return done(new errors.InternalError(err.message));
    }

    if (!farmers || !farmers.length) {
      return done(new errors.InternalError('Could not locate farmers'));
    }

    this._publishContract(farmers, contr, audit, (err, farmerContact, farmerContract, token) => {
      if (err) {
        return done(new errors.InternalError(err.message));
      }

      done(null, farmerContact, farmerContract, token);
    });
  });
};

FramesRouter._sortByResponseTime = function (a, b) {
  const aTime = a.contact.responseTime || Infinity;
  const bTime = b.contact.responseTime || Infinity;

  return (aTime === bTime) ? 0 : (aTime > bTime) ? 1 : -1;
};

FramesRouter._sortByReputation = function (a, b) {
  const aVal = a.contact.reputation || Infinity;
  const bVal = b.contact.reputation || Infinity;

  return (aVal === bVal) ? 0 : (aVal > bVal) ? 1 : -1;
};

FramesRouter._sortByTypeAndResponseTime = function (a, b) {
  const aIsPremium = constants.XNODES.indexOf(a.contact.nodeID) !== -1;
  const bIsPremium = constants.XNODES.indexOf(b.contact.nodeID) !== -1;

  // premium nodes first
  if (aIsPremium !== bIsPremium) {
    return aIsPremium ? -1 : 1;
  }

  // then, sort by response time
  return FramesRouter._sortByResponseTime(a, b);
};

FramesRouter.prototype._selectFarmersLegacy = function (excluded, callback) {
  const Contact = this.storage.models.Contact;

  async.parallel([
    (next) => Contact
      .find({
        _id: {
          $gte: crypto.randomBytes(20).toString('hex'),
          $nin: excluded
        },
        reputation: { $gt: this._defaults.publishBenchThreshold },
        spaceAvailable: true
      }).sort({ _id: -1 })
      .limit(this._defaults.publishTotal)
      .exec(next),
    (next) => Contact.find({
      _id: {
        $lt: crypto.randomBytes(20).toString('hex'),
        $nin: excluded
      },
      reputation: { $lte: this._defaults.publishBenchThreshold },
      spaceAvailable: true
    }).sort({ _id: -1 })
      .limit(this._defaults.publishBenchTotal)
      .exec(next)
  ], (err, results) => {
    if (err) {
      return callback(err);
    }

    const combined = [...results[0], ...results[1]];

    callback(null, combined);
  });
};

FramesRouter.prototype._selectFarmers = function (excluded, callback) {
  const { Contact } = this.storage.models;
  const xNodes = constants.CLUSTER;

  async.parallel([
    (next) => Contact.find({ _id: { $in: xNodes, $nin: excluded } }).sort({ _id: -1 }).exec(next),
  ], (err, results) => {
    if (err) {
      return callback(err);
    }

    const combined = results[0];

    callback(null, combined);
  });
};

FramesRouter.prototype._publishContract = function (nodes, contract, audit, callback) {
  const hash = contract.get('data_hash');

  this.contracts.load(hash, (err, item) => {
    if (err) {
      item = new storj.StorageItem({ hash: hash });
    }

    this.network.publishContract(nodes, contract, (err, data) => {
      if (err) {
        return callback(err);
      }

      const farmerContact = storj.Contact(data.contact);
      const farmerContract = storj.Contract(data.contract);

      item.addContract(farmerContact, farmerContract);
      item.addAuditRecords(farmerContact, audit);

      this.contracts.save(item, (err) => {
        if (err) {
          return callback(new errors.InternalError(err.message));
        }

        callback(null, farmerContact, farmerContract, data.token);
      });
    });
  });
};

/**
 * Negotiates a storage contract and adds the shard to the frame
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
FramesRouter.prototype.addShardToFrame = function (req, res, next) {
  const self = this;
  const { Pointer, Frame, Mirror } = this.storage.models;

  if (req.body.size > MAX_SHARD_SIZE) {
    return next(new errors.BadRequestError('Maximum shard size ' + MAX_SHARD_SIZE + ' exceeded'));
  }

  if (Array.isArray(req.body.exclude) && req.body.exclude.length > constants.MAX_BLACKLIST) {
    return next(new errors.BadRequestError('Maximum blacklist length'));
  }

  let audit;
  let contr;

  try {
    audit = storj.AuditStream.fromRecords(req.body.challenges, req.body.tree);
  } catch (err) {
    return next(new errors.BadRequestError(err.message));
  }

  try {
    contr = new storj.Contract({
      data_size: req.body.size,
      data_hash: req.body.hash,
      store_begin: Date.now(),
      store_end: Date.now() + ms('3650d'),
      audit_count: req.body.challenges.length
    });
  } catch (err) {
    return next(new errors.BadRequestError(err.message));
  }

  let bl = Array.isArray(req.body.exclude) ? req.body.exclude : [];
  let frame = null;
  let farmer = null;
  let contract = null;
  let token = null;

  async.series([
    function checkFrame(done) {
      Frame.findOne({
        _id: req.params.frame,
        user: req.user._id
      }, function (err, _frame) {
        if (err) {
          return done(new errors.InternalError(err.message));
        }

        if (!_frame) {
          done(new errors.NotFoundError('Frame not found'));
        } else {
          frame = _frame;
          done();
        }
      });
    },
    function getContract(done) {
      // First check that we don't already have cached offers for this
      // shard from a previous request that timed out with offers that
      // arrived late.

      Mirror.find({
        shardHash: req.body.hash
      }).populate('contact').exec((err, mirrors) => {
        if (err) {
          log.error('Frame find Mirror error: ' + err.message);
        }

        let mirror = null;
        if (mirrors && mirrors.length) {
          const filtered = mirrors.filter((m) => {
            if (!m.contact) {
              log.warn('Frames: Mirror %s is missing contact in database. Mirror destroyed.', m._id);
              m.remove();

              return false;
            }

            if (m.isEstablished) {
              return false;
            }

            const noSameAddress = _.find(mirrors, x => x.isEstablished && x.address === m.address);

            if (noSameAddress) {
              return false;
            }

            const isBlackListed = bl.includes(m.contact.nodeID);

            if (isBlackListed) {
              return false;
            }

            return true;
          });

          const uniqIps = _.uniqBy(filtered, 'address');

          uniqIps.sort(FramesRouter._sortByTypeAndResponseTime);

          mirror = uniqIps[0];
        }

        if (!mirror) {
          // If we don't have any cached offers go ahead and send out
          // a call into the network for more offers

          log.debug('Requesting contract for frame: %s, shard hash: %s and size: %s',
            req.params.frame, req.body.hash, req.body.size);

          self._getContractForShardSIP6(contr, audit, bl, res, function (err, _contact, _contract, _token) {
            if (err) {
              // log.warn('Could not get contract for frame: %s and shard hash: %s, reason: %s', req.params.frame, req.body.hash, err.message);
              done(new errors.ServiceUnavailableError(err.message));
            } else {
              farmer = _contact;
              contract = _contract;
              token = _token;
              done();
            }
          });
        } else {

          token = mirror.token;

          self.contracts.load(req.body.hash, function (err, item) {
            if (err) {
              item = new storj.StorageItem({ hash: req.body.hash });
            }

            mirror.isEstablished = true;
            mirror.save();

            farmer = storj.Contact(mirror.contact);
            contract = storj.Contract(mirror.contract);

            item.addContract(farmer, contract);
            item.addAuditRecords(farmer, audit);

            self.contracts.save(item, function (err) {
              if (err) {
                return done(new errors.InternalError(err.message));
              }
              done();
            });
          });
        }
      });
    },
    function addPointerToFrame(done) {

      let pointerData = {
        index: req.body.index,
        hash: req.body.hash,
        size: req.body.size,
        tree: req.body.tree,
        parity: req.body.parity,
        challenges: req.body.challenges,
        frame: frame.id
      };

      Pointer.create(pointerData, function (err, pointer) {
        if (err) {
          return done(new errors.BadRequestError(err.message));
        }

        // We need to reload the frame to get the latest copy
        Frame.findOne({
          _id: frame._id
        }).populate('shards').exec(function (err, frame) {
          if (err) {
            return done(new errors.InternalError(err.message));
          }

          req.user.recordUploadBytes(pointer.size, (err) => {
            if (err) {
              log.warn(
                'addShardToFrame: unable to save upload bytes %s, ' +
                'user: %s, reason: %s', pointer.size, req.user.email,
                err.message
              );
            }
          });

          frame.addShard(pointer, (err) => {
            if (err) {
              return done(new errors.InternalError(err.message));
            }
            res.send({
              hash: req.body.hash,
              token: token,
              operation: 'PUSH',
              farmer: farmer
            });
          });
        });
      });
    }
  ], next);
};

/**
 * Destroys the file staging frame if it is not in use by a bucket entry
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
FramesRouter.prototype.destroyFrameById = function (req, res, next) {
  const { BucketEntry, Frame } = this.storage.models;

  BucketEntry.findOne({
    user: req.user._id,
    frame: req.params.frame
  }, function (err, entry) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (entry) {
      return next(new errors.BadRequestError(
        'Refusing to destroy frame that is referenced by a bucket entry'
      ));
    }

    Frame.findOne({
      user: req.user._id,
      _id: req.params.frame
    }, function (err, frame) {
      if (err) {
        return next(new errors.InternalError(err.message));
      }

      if (!frame) {
        return next(new errors.NotFoundError('Frame not found'));
      }

      frame.remove(function (err) {
        if (err) {
          return next(new errors.InternalError(err.message));
        }

        res.status(204).end();
      });
    });
  });
};

/**
 * Returns the caller's file staging frames
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
FramesRouter.prototype.getFrames = function (req, res, next) {
  const Frame = this.storage.models.Frame;

  Frame.find({ user: req.user._id }).limit(10).exec(function (err, frames) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    res.send(frames.map(function (frame) {
      return frame.toObject();
    }));
  });
};

/**
 * Returns the file staging frame by it's ID
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
FramesRouter.prototype.getFrameById = function (req, res, next) {
  const Frame = this.storage.models.Frame;

  Frame.findOne({
    user: req.user._id,
    _id: req.params.frame
  }, function (err, frame) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!frame) {
      return next(new errors.NotFoundError('Frame not found'));
    }

    res.send(frame.toObject());
  });
};

function getStorageUsage(storage, user) {
  const Bucket = storage.models.Bucket;

  return new Promise((resolve, reject) => {
    var agg = Bucket.aggregate([
      {
        $match: {
          user: user
        }
      },
      {
        $lookup: {
          from: 'bucketentries',
          localField: '_id',
          foreignField: 'bucket',
          as: 'join1'
        }
      },
      {
        $unwind: {
          path: '$join1'
        }
      },
      {
        $lookup: {
          from: 'frames',
          localField: 'join1.frame',
          foreignField: '_id',
          as: 'join2'
        }
      },
      {
        $unwind: {
          path: '$join2'
        }
      },
      {
        $project: {
          _id: '$join2._id',
          user: '$join2.user',
          size: '$join2.size'
        }
      },
      {
        $group: {
          _id: '$user',
          total: { $sum: '$size' }
        }
      }
    ]).cursor({ batchSize: 1000 }).exec();

    agg.next().then(data => {
      resolve(data);
    }).catch(err => {
      reject({ message: 'Error', reason: err });
    });
  });
}

function getStorageLimit(storage, user) {
  return new Promise((resolve, reject) => {

    storage.models.User.findOne({ _id: user }, function (err, _user) {
      if (err) {
        reject({ error: 'Internal error', statusCode: 500 });
      }

      if (!_user) {
        reject({ error: 'User not found', statusCode: 404 });
      }

      if (_user.maxSpaceBytes === 0) {
        _user.maxSpaceBytes = 1024 * 1024 * 1024 * 2; // 2GB by default.
      }

      resolve({ error: null, statusCode: 200, maxSpaceBytes: _user.maxSpaceBytes });
    });

  });
}

function userHasFreeSpaceLeft(storage, user) {
  return new Promise((resolve) => {
    getStorageLimit(storage, user).then(limit => {
      const maxSpaceBytes = limit.maxSpaceBytes;
      getStorageUsage(storage, user).then(usage => {
        const usedSpaceBytes = usage ? usage.total : 0;
        // If !maxSpaceBytes models are not updated. Consider no limit due to this variable.
        resolve({ canUpload: !maxSpaceBytes ? true : usedSpaceBytes < maxSpaceBytes });
      }).catch(err => {
        resolve({ canUpload: false, error: err.message });
      });

    }).catch(err => {
      resolve({ canUpload: false, error: err.message });
    });
  });
}

FramesRouter.prototype.getStorageUsage = function (req, res) {
  getStorageUsage(this.storage, req.user._id)
    .then(usage => {
      if (!usage) {
        usage = { total: 0 };
      }
      res.status(200).send(usage);
    })
    .catch(() => {
      res.status(400).send({ message: 'Error' });
    });
};

FramesRouter.prototype.getStorageLimit = function (req, res) {
  getStorageLimit(this.storage, req.user._id).then(result => {
    res.status(result.statusCode).send({ maxSpaceBytes: result.maxSpaceBytes });
  }).catch(err => {
    res.status(err.statusCode).send({ error: err.error });
  });
};


/**
 * Export definitions
 * @private
 */
FramesRouter.prototype._definitions = function () {
  /* jshint maxlen: 140 */
  return [
    ['POST', '/frames', this.getLimiter(limiter(1000)), this._verify, this.createFrame],
    ['PUT', '/frames/:frame', this.getLimiter(limiter(this._defaults.shardsPerMinute)), this._verify, this.addShardToFrame],
    ['DELETE', '/frames/:frame', this.getLimiter(limiter(1000)), this._verify, this.destroyFrameById],
    ['GET', '/frames', this.getLimiter(limiter(1000)), this._verify, this.getFrames],
    ['GET', '/frames/:frame', this.getLimiter(limiter(1000)), this._verify, this.getFrameById],
    ['GET', '/limit', this.getLimiter(limiter(1000)), this._verify, this.getStorageLimit]
  ];
};

module.exports = FramesRouter;