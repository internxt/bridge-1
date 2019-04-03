'use strict';

const assert = require('assert');
const Router = require('./index');
const middleware = require('storj-service-middleware');
const rawbody = middleware.rawbody;
const log = require('../../logger');
const errors = require('storj-service-error-types');
const merge = require('merge');
const inherits = require('util').inherits;
const authenticate = middleware.authenticate;
const crypto = require('crypto');
const storj = require('storj-lib');
const analytics = require('storj-analytics');
const limiter = require('../limiter').DEFAULTS;

/**
 * Handles endpoints for all user related operations
 * @constructor
 * @extends {Router}
 */
function UsersRouter(options) {
  if (!(this instanceof UsersRouter)) {
    return new UsersRouter(options);
  }

  Router.apply(this, arguments);

  this._verify = authenticate(this.storage);
  this.getLimiter = middleware.rateLimiter(options.redis);
}

inherits(UsersRouter, Router);

/**
 * Sends a user activation email
 * @private
 */
UsersRouter.prototype._dispatchActivationEmail = function (user, redir, cb) {
  let self = this;
  let profile = self.config.server.public || self.config.server;
  let host = profile.host;
  let callback = cb || storj.utils.noop;
  let port = [443, 80].indexOf(profile.port) === -1 ?
    ':' + profile.port :
    '';
  let proto = self.config.server.ssl &&
    self.config.server.ssl.cert &&
    self.config.server.ssl.key ?
    'https:' :
    'http:';

  self.mailer.dispatch(user.email, 'confirm', {
    token: user.activator,
    redirect: redir,
    url: proto + '//' + host + port
  }, function (err) {
    if (err) {
      log.error('failed to send activation email, reason: %s', err.message);
      callback(err);
    } else {
      callback(null);
    }
  });
};

/**
 * Sends a delayed activation email with optional `url` for beta signup
 * @private
 */
UsersRouter.prototype._dispatchDelayedActivationEmail = function (user, cb) {
  let self = this;
  self.mailer.dispatch(user.email, 'delayed-activation', {}, function (err) {
    if (err) {
      log.error('failed to send signup throttling email, reason: %s', err.message);
      cb(err);
    } else {
      cb(null);
    }
  });
};

/**
 * Sends an activation email based on configuration options
 * @private
 */
UsersRouter.prototype._dispatchActivationEmailSwitch = function (user, cb) {
  if (this.config.application.delayedActivation) {
    this._dispatchDelayedActivationEmail(user, cb);
  } else {
    this._dispatchActivationEmail(user, cb);
  }
};

/**
 * Registers a new user
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.createUser = function (req, res, next) {

  const self = this;
  const User = this.storage.models.User;

  log.debug('registering user account for %s', req.body.email);

  // Ensure email address is lowercase & trimmed to avoid future search errors
  req.body.email = req.body.email.toLowerCase().trim();

  if (req.body.referralPartner) {
    self._createUserWithOpts(req, res, next);
  } else {
    User.create(req.body.email, req.body.password, function (err, user) {
      if (err) {
        return next(err);
      }

      analytics.identify(req.headers.dnt, {
        userId: user.uuid,
        traits: {
          activated: false
        }
      });
      analytics.track(req.headers.dnt, {
        userId: user.uuid,
        event: 'User Created'
      });

      self._dispatchAndCreatePubKey(user, req, res, next);
    });
  }
};

/**
 * Dispatch activation email and create pubkey if none
 * @param {Object} user
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype._dispatchAndCreatePubKey = function (user, req, res, next) {
  const self = this;
  const PublicKey = this.storage.models.PublicKey;

  if (!req.body.pubkey) {
    self._dispatchActivationEmailSwitch(user, function (err) {
      if (err) {
        log.error('failed to send deactivation email, reason: %s', err.message);
      }
    });

    return res.status(201).send(user.toObject());
  }

  PublicKey.create(user, req.body.pubkey, function (err, pubkey) {
    if (err) {
      user.remove();
      return next(new errors.BadRequestError(err.message));
    }

    // NB: Don't send activation email, send beta list email
    // NB: Send email explaining why they cant activate yet
    self._dispatchActivationEmailSwitch(user, function (err) {
      if (err) {
        log.error('failed to send deactivation email, reason: %s', err.message);
      }
    });

    res.status(201).send(merge(user.toObject(), {
      pubkey: pubkey.key
    }));
  });
};

/**
 * Creates user with opts object, does extra logic involved
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype._createUserWithOpts = function (req, res, next) {
  const self = this;
  const User = this.storage.models.User;
  const Partner = this.storage.models.Partner;
  const opts = {
    email: req.body.email,
    password: req.body.password
  };

  Partner.findOne({ name: req.body.referralPartner },
    function (err, partner) {
      if (err) {
        return next(err);
      }

      opts.referralPartner = partner ? partner.id : null;

      User.create(opts, function (err, user) {
        if (err) {
          return next(err);
        }

        analytics.identify(req.headers.dnt, {
          userId: user.uuid,
          traits: {
            activated: false
          }
        });
        analytics.track(req.headers.dnt, {
          userId: user.uuid,
          event: 'User Created'
        });

        if (partner) {
          analytics.track(req.headers.dnt, {
            userId: user.uuid,
            event: 'Referral Partner Added',
            traits: {
              referralPartner: partner.id,
            }
          });
        }

        self._dispatchAndCreatePubKey(user, req, res, next);
      });
    });
};

/**
 * Confirms a user account
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.confirmActivateUser = function (req, res, next) {
  const User = this.storage.models.User;

  log.debug('activating user with token %s', req.params.token);

  User.findOne({
    activator: req.params.token
  }, function (err, user) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!user) {
      return next(new errors.BadRequestError('Invalid activation token'));
    }

    user.activate(function (err) {
      if (err) {
        return next(new errors.InternalError(err.message));
      }

      analytics.track(req.headers.dnt, {
        userId: user.uuid,
        event: 'User Activated'
      });
      analytics.identify(req.headers.dnt, {
        userId: user.uuid,
        traits: {
          activated: true
        }
      });

      if (req.query.redirect) {
        res.redirect(req.query.redirect);
      } else {
        res.send(user.toObject());
      }
    });
  });
};

/**
 * Reactivates a user account
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.reactivateUser = function (req, res, next) {
  const self = this;
  const User = this.storage.models.User;

  log.debug('sending account reactivation email to %s', req.body.email);

  User.findOne({
    _id: req.body.email
  }, function (err, user) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!user) {
      return next(new errors.NotFoundError('User not found'));
    }

    if (user.activated) {
      return next(new errors.BadRequestError('User is already activated'));
    }

    self._dispatchActivationEmail(user, req.body.redirect);
    res.status(201).send(user.toObject());
  });
};

/**
 * Deactivates a user account
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.destroyUser = function (req, res, next) {
  const self = this;
  const User = this.storage.models.User;

  log.debug('creating user deactivation token');

  User.findOne({ _id: req.params.id }, function (err, user) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!user) {
      return next(new errors.NotFoundError('User not found'));
    }

    if (req.user._id !== user._id) {
      return next(new errors.NotAuthorizedError());
    }

    user.deactivator = crypto.randomBytes(256).toString('hex');

    let profile = self.config.server.public || self.config.server;
    let host = profile.host;
    let port = [443, 80].indexOf(profile.port) === -1 ?
      ':' + profile.port :
      '';
    let proto = self.config.server.ssl &&
      self.config.server.ssl.cert &&
      self.config.server.ssl.key ?
      'https:' :
      'http:';

    self.mailer.dispatch(user.email, 'delete', {
      token: user.deactivator,
      redirect: req.body.redirect,
      url: proto + '//' + host + port
    }, function (err) {
      if (err) {
        log.error('failed to send deactivation email, reason: %s', err.message);
        return next(new errors.InternalError(err.message));
      }

      user.save(function (err) {
        if (err) {
          return next(new errors.InternalError(err.message));
        }

        res.status(200).send(user.toObject());
      });
    });
  });
};

/**
 * Confirms the deletion of a user account
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.confirmDestroyUser = function (req, res, next) {
  const User = this.storage.models.User;

  log.debug('deactivating user account with token %s', req.params.token);

  User.findOne({ deactivator: req.params.token }, function (err, user) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!user) {
      return next(new errors.NotFoundError('User not found'));
    }

    user.deactivate(function (err) {
      if (err) {
        return next(new errors.InternalError(err.message));
      }

      if (req.query.redirect) {
        res.redirect(req.query.redirect);
      } else {
        res.status(200).send(user.toObject());
      }
    });
  });
};

/**
 * Creates a password reset token
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.createPasswordResetToken = function (req, res, next) {
  const self = this;
  const User = this.storage.models.User;

  User.findOne({ _id: req.params.id }, function (err, user) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!user) {
      return next(new errors.NotFoundError('User not found'));
    }

    user.resetter = crypto.randomBytes(256).toString('hex');

    user.save(function (err) {
      if (err) {
        return next(new errors.InternalError(err.message));
      }

      let profile = self.config.server.public || self.config.server;
      let host = profile.host;
      let port = [443, 80].indexOf(profile.port) === -1 ?
        ':' + profile.port :
        '';
      let proto = self.config.server.ssl &&
        self.config.server.ssl.cert &&
        self.config.server.ssl.key ?
        'https:' :
        'http:';

      self.mailer.dispatch(user.email, 'reset', {
        token: user.resetter,
        redirect: req.body.redirect,
        url: req.body.url || proto + '//' + host + port
      }, function (err) {
        if (err) {
          log.error('failed to send reset email, reason: %s', err.message);
          return next(new errors.InternalError(err.message));
        }

        return res.status(200).send(user.toObject());
      });
    });
  });
};

/**
 * Confirms and applies the password reset
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.confirmPasswordReset = function (req, res, next) {
  const User = this.storage.models.User;

  // NB: Mitigate timing attack
  // NB: - attempt to make non-lookup-responses take similar
  // NB: - amounts of time as lookup-responses
  User.count({}, function (err, count) {
    let rand = Math.floor(Math.random() * count);

    function nextPlusTime(value) {
      User.findOne({}).skip(rand).exec(function () { // do nothing with this record
        next(value);
      });
    }

    try {
      assert(Buffer(req.body.password, 'hex').length * 8 === 256);
    } catch (err) {
      return next(new errors.BadRequestError(
        'Password must be hex encoded SHA-256 hash'
      ));
    }

    try {
      assert(Buffer(req.params.token, 'hex').length === 256);
    } catch (err) {
      return nextPlusTime(new Error('Resetter must be hex encoded 256 byte string'));
    }

    User.findOne({ resetter: req.params.token }, function (err, user) {
      if (err) {
        return next(new errors.InternalError(err.message));
      }

      if (!user) {
        return next(new errors.NotFoundError('User not found'));
      }

      user.hashpass = crypto.createHash('sha256').update(req.body.password).digest('hex');
      user.resetter = null;

      user.save(function (err) {
        if (err) {
          return next(new errors.InternalError(err.message));
        }

        if (req.query.redirect) {
          res.redirect(req.query.redirect);
        } else {
          res.send(user.toObject());
        }
      });
    });
  });
};


UsersRouter.prototype.subscribeUserToPlan = function (req, res, next) {
  const config = JSON.parse(require('fs').readFileSync(process.env.HOME + '/.storj-bridge/config/' + process.env.NODE_ENV, 'utf8'));

  const User = this.storage.models.User;

  req.params = req.body;

  if (!req.params.plan_id || req.params.plan_id == '') {
    return next(new errors.BadRequestError('No valid plan to subscribe '));
  }

  let thePlan = config.stripe_plans.filter(o => o.plan_id == req.params.plan_id);

  if (thePlan == []) {
    return next(new errors.BadRequestError('No valid plan provided'));
  }

  const stripeUtils = require('../stripeService');

  // DOES USER HAVE AN STRIPE ACCOUNT?
  stripeUtils.customerExists(req.user._id).then(customer => {
    console.log('DETERMINE USER EXISTENCE');

    if (!customer) {
      console.log('USER DOES NOT EXISTS, CREATING NEW CUSTOMER: ' + req.user._id);

      if (!req.params.token) {
        return next(new errors.BadRequestError('No token provided'));
      }

      console.log(req.params.token);

      var token = JSON.parse(req.params.token).id;

      console.log('TOKEN ID', token);

      // USER DOES NOT EXISTS, CREATE
      stripeUtils.createNewCustomer(req.user._id, token).then(newCustomer => {

        if (!newCustomer) {
          console.log(newCustomer);
          return next(new errors.BadRequestError('Error creating new customer on stripe'));
        }

        console.log('NEW CUSTOMER CREATED SUCCESFULLY: ' + req.user._id);

        // SUBSCRIBE NEW USER TO PLAN
        stripeUtils.subscribeCustomerToPlan(newCustomer.id, req.params.plan_id).then(subscription => {
          console.log('USER ' + req.user._id + ' SUBSCRIBED');

          User.findOne({ _id: req.user.id }, (err, user) => {
            let current_plan_info = config.stripe_plans.filter(pln => pln.plan_id == req.params.plan_id);
            console.log('USER FOUND: ', current_plan_info);
            if (!err) {

              user.subscriptionPlan.isSubscribed = true;
              user.maxSpaceBytes = current_plan_info[0].maxPlanBytes;
              user.subscriptionPlan = {
                paymentBridge: 'stripe',
                plan: current_plan_info[0].plan_name
              };
              user.save(err => {
                if (err) {
                  console.log(err);
                  console.log('ERROR SAVING IN DATABASE 1');
                }
              });
            }
          });

          res.status(200).send({ ok: 'Subscription of a new customer done' });
        }).catch(err => {
          console.log('ERROR CREATING SUBSCRIPTION FOR ' + req.user._id);
          res.status(400).send({ error: err, reason: 'Cannot create subscription' });
        });

      }).catch(err => {
        console.error('CANNOT CREATE NEW USER ' + req.user._id);
        res.status(400).send({ error: err, reason: 'Cannot create new customer' });
      });
    } else {
      // USER DOES EXISTS, HAVE An ACTIVE PLAN ALREADY?

      let subscriptions = customer.data[0].subscriptions.data;

      console.log('--------------- SUBSCRIPTION RETRIEVED ---------------');
      console.log(subscriptions);

      if (subscriptions.length > 0) {
        console.log('USER HAS AN ACTIVE SUBSCRIPTION, LETS CHECK IF HE CAN UPGRADE');

        console.log(subscriptions[0]);

        let customerId = subscriptions[0].customer;
        let subscription_plan = subscriptions[0].plan.id;
        let subscription_id = subscriptions[0].id;

        console.log('Current subscription id: ', subscription_id);
        console.log('Current subscription plan: ', subscription_plan);

        // CAN UPGRADE?
        let current_plan_info = config.stripe_plans.filter(pln => pln.plan_id == subscription_plan);

        console.log('current plan info: ', current_plan_info);

        if (current_plan_info.index == 2) {
          // USER ALREADY HAS THE MAX PLAN, NOT GOING TO UPGRADE
          console.log('Cannot upgrade, you have the highest plan');
          res.status(400).send({ error: 'Cannot upgrade, you have the highest plan' });
        } else {
          // UPGRADE PLAN

          console.log('CURRENT INDEX ', current_plan_info.index);
          console.log('USER CAN UPGRADE');

          // FIRST, UNSUBCRIBE FROM THE OTHER PLAN IF IS NOT FREE PLAN
          if (current_plan_info.index != '0') {
            stripeUtils.cancelSubscriptionById(subscription_id).then(result => {
              console.log('Unsubscribed user: ' + current_plan_info.plan_name);

              stripeUtils.subscribeCustomerToPlan(customer.id, req.params.plan).then(result => {
                console.log('Upgrade succesful');
                User.findOne({ _id: req.user.id }, (err, user) => {
                  let current_plan_info = config.stripe_plans.filter(pln => pln.plan_id == req.params.plan_id);
                  console.log('USER FOUND: ', current_plan_info);
                  if (!err) {

                    user.subscriptionPlan.isSubscribed = true;
                    user.maxSpaceBytes = current_plan_info[0].maxPlanBytes;
                    user.subscriptionPlan = {
                      paymentBridge: 'stripe',
                      plan: current_plan_info[0].plan_name
                    };
                    user.save(err => {
                      if (err) {
                        console.log(err);
                        console.log('ERROR SAVING IN DATABASE 2');
                      }
                    });
                  }
                });
                res.status(200).send({ error: 'Upgrade successful' });
              }).catch(err => {
                console.log('Unable to subscribe to new plan after deleting the other plan');
                res.status(400).send({ error: 'Error on subscription to new plan, please retry' });
              });

            }).catch(err => {
              console.log(err);
              console.log('Error: Cannot unsubscribe from plan');
              res.status(400).send({ error: 'Error: Cannot unsubscribe from plan' });
            });

          } else {
            // USER HAS NOT FREE PLAN, LETS SUBSCRIBE HIM!
            stripeUtils.subscribeCustomerToPlan(customer.id, req.params.plan).then(result => {
              console.log('SUBSCRIPTION succesful');

              User.findOne({ _id: req.user.id }, (err, user) => {
                let current_plan_info = config.stripe_plans.filter(pln => pln.plan_id == req.params.plan_id);
                console.log('USER FOUND: ', current_plan_info);
                if (!err) {

                  user.subscriptionPlan.isSubscribed = true;
                  user.maxSpaceBytes = current_plan_info[0].maxPlanBytes;
                  user.subscriptionPlan = {
                    paymentBridge: 'stripe',
                    plan: current_plan_info[0].plan_name
                  };
                  user.save(err => {
                    if (err) {
                      console.log(err);
                      console.log('ERROR SAVING IN DATABASE 3');
                    }
                  });
                }
              });
              res.status(200).send({ error: 'SUBSCRIPTION successful' });
            }).catch(err => {
              console.log('Unable to subscribe to plan');
              res.status(400).send({ error: 'Error on subscription to plan' });
            });
          }
        }
      } else {
        // USER EXISTS BUT DOES NOT HAVE A SUBSCRIPTION
        console.log('USER DOES NOT HAVE A SUBSCRIPTION ACTIVE, LETS SUBCRIBE HIM');

        stripeUtils.subscribeCustomerToPlan(customer.data[0].id, req.params.plan_id).then(result => {
          console.log('SUBSCRIPTION succesful');

          // TODO subscribe user plan in database
          User.findOne({ _id: req.user.id }, (err, user) => {
            let current_plan_info = config.stripe_plans.filter(pln => pln.plan_id == req.params.plan_id);
            console.log('USER FOUND: ', current_plan_info);
            if (!err) {

              user.subscriptionPlan.isSubscribed = true;
              user.maxSpaceBytes = current_plan_info[0].maxPlanBytes;
              user.subscriptionPlan = {
                paymentBridge: 'stripe',
                plan: current_plan_info[0].plan_name
              };
              user.save(err => {
                if (err) {
                  console.log(err);
                  console.log('ERROR SAVING IN DATABASE 4');
                }
              });
            }
          });

          res.status(200).send({ error: 'SUBSCRIPTION successful' });
        }).catch(err => {
          console.log('Unable to subscribe to plan', err);
          res.status(400).send({ error: 'Error on subscription to plan' });
        });
      }
    }
  }).catch(err => {
    console.log('CANNOT DETERMINE USER EXISTENCE');
    res.status(400).send({ error: err.message });
  });

}

/**
 * Get user activation info
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @param {Function} next
 */
UsersRouter.prototype.isActivated = function (req, res, next) {
  const User = this.storage.models.User;

  log.debug('Getting user activation info for %s', req.headers['email']);

  User.findOne({ _id: req.headers['email'] }, function (err, user) {
    if (err) {
      return next(new errors.InternalError(err.message));
    }

    if (!user) {
      return next(new errors.BadRequestError('User not found'));
    }

    // Send activated info
    res.status(200).send({ activated: user.activated });
  });
};

UsersRouter.prototype.cancelPlanAndDestroyUser = function (req, res, next) {
  console.log('DEACTIVATION OF ', req.user._id);

  const stripeUtils = require('../stripeService');



  if (req.params.id != req.user.id) {
    return next(new errors.BadRequestError('This is not your account'));
  }

  var userToDestroy = req.params.id;
  log.info('Deactivation: User requested deactivation: %s', userToDestroy);

  stripeUtils.customerExists(userToDestroy).then(user => {
    console.log(user);
    if (user) {
      let subscriptions = user.data[0].subscriptions.data;

      if (subscriptions.length > 0) {
        stripeUtils.cancelSubscriptionById(subscriptions[0].id).then(confirmation => {
          this.destroyUser(req, res, next);
        }).catch(err => {
          res.status(400).send({ error: 'Cannot cancel current plan' });
        });
      } else {
        this.destroyUser(req, res, next);
      }
    } else {
      this.destroyUser(req, res, next);
    }
  });


}


/**
 * Export definitions
 * @private
 */
UsersRouter.prototype._definitions = function () {
  return [
    ['POST', '/users', this.getLimiter(limiter(5)), rawbody, this.createUser],
    ['POST', '/activations', this.getLimiter(limiter(5)), rawbody, this.reactivateUser],
    ['GET', '/activations/:token', this.getLimiter(limiter(5)), this.confirmActivateUser],
    ['DELETE', '/users/:id', this.getLimiter(limiter(5)), this._verify, this.destroyUser],
    ['GET', '/deactivations/:token', this.getLimiter(limiter(5)), this.confirmDestroyUser],
    ['PATCH', '/users/:id', this.getLimiter(limiter(5)), rawbody, this.createPasswordResetToken],
    ['POST', '/resets/:token', rawbody, this.getLimiter(limiter(5)), this.confirmPasswordReset],
    ['GET', '/users/isactivated', this.getLimiter(limiter(5)), rawbody, this.isActivated],
    ['POST', '/subscription', this.getLimiter(limiter(5)), this._verify, this.subscribeUserToPlan],
    ['DELETE', '/cancelAccount/:id', this.getLimiter(limiter(5)), this._verify, this.cancelPlanAndDestroyUser]
  ];
};

module.exports = UsersRouter;
