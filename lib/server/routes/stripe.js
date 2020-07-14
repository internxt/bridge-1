'use strict';

const Router = require('./index');
const inherits = require('util').inherits;
const middleware = require('storj-service-middleware');
const authenticate = middleware.authenticate;
const rawbody = middleware.rawbody;
const limiter = require('../limiter').DEFAULTS;
const errors = require('storj-service-error-types');
const log = require('../../logger');



/**
 * Handles endpoints for all stripe related webhooks
 * @constructor
 * @extends {Router}
 */
function StripeRouter(options) {
    if (!(this instanceof StripeRouter)) {
        return new StripeRouter(options);
    }

    Router.apply(this, arguments);

    this._verify = authenticate(this.storage);
    this.getLimiter = middleware.rateLimiter(options.redis);
}

inherits(StripeRouter, Router);

StripeRouter.prototype.getSubscription = function (req, res, next) {
    res.status(200).send({ ok: 'ok' });
}

StripeRouter.prototype.postSubscription = function (req, res, next) {
    res.status(200).send({ ok: 'ok' });
}

StripeRouter.prototype._webhookCheckoutSessionCompleted = function (data, res, next) {
    const User = this.storage.models.User;

    // Prevent test data overriding real users data
    if (!data.livemode && process.env.NODE_ENV === 'production') {
        // return next(errors.InternalError(`Current environment can0t handle test data.`));
    }

    const stripe = require('stripe')(data.livemode ? this.config.stripe.SK_LIVE : this.config.stripe.SK_TEST);


    const object = data.data.object;

    const subscriptionItem = object.display_items[0];
    const subscriptionId = object.subscription;
    const customer = object.customer;
    const planId = subscriptionItem.plan.id;
    const productId = subscriptionItem.plan.product;

    stripe.customers.retrieve(customer, (err, customer_obj) => {
        const email = customer_obj.email;

        log.info('Webhook called by %s', email)

        stripe.products.retrieve(productId, (err, product) => {
            if (err) {
                log.error('Webhook error retrieving product')
                log.error(err);
                return res.status(500).send({ error: 'Error retrieving plan' });
            }

            const metadata = product.metadata;

            User.findOne({ _id: email }, (err, user) => {
                if (err) {
                    log.error('Webhook error updating user')
                    log.error(err);
                    res.status(500).send({ error: 'Cannot find user e-mail' });
                } else {
                    user.maxSpaceBytes = metadata.size_bytes;
                    user.isFreeTier = false;
                    user.save(err => {
                        if (!err) {
                            log.info('Webhook success for %s', email)
                            res.status(200).send();
                        } else {
                            log.error('Webhook failed updating model for %s', email)
                            res.status(500).send({ error: 'Error saving user metadata' });
                        }
                    });
                }
            });
        });
    });

}

StripeRouter.prototype._webhookCustomerSubscriptionDeleted = function (data, res, next) {
    const User = this.storage.models.User;
    const stripe = require('stripe')(data.livemode ? this.config.stripe.SK_LIVE : this.config.stripe.SK_TEST);
    const object = data.data.object;
    const customer = object.customer;

    stripe.customers.retrieve(customer, (err, customer_obj) => {
        if (err) {
            console.error('Webhook error, customer not found on stripe', err)
            return res.status(500).send({ error: 'Unkown customer on stripe' })
        }

        const email = customer_obj.email;
        User.findOne({ _id: email }, (err, user) => {
            if (err || !user) {
                console.error('Webhook error, user %s not found on bridge database', email)
                return res.status(500).send({ error: 'Unkown customer on bridge' })
            }

            user.maxSpaceBytes = 0;
            user.isFreeTier = true;

            user.save(err => {
                if (err) {
                    console.log('Webhook error, cannot update %s on bridge', email)
                    return res.status(500).send({ error: 'Error updating user on bridge database' })
                }

                return res.status(200).send()
            })
        })
    })
}

StripeRouter.prototype.parseWebhook = function (req, res, next) {
    /**
     * Go to stripe dashboard and enable webhook checkout.session.completed
     * to this endpoint
     */
    const webhookObject = req.body;

    switch (webhookObject.type) {
        case 'checkout.session.completed':
            return this._webhookCheckoutSessionCompleted(webhookObject, res, next);
        case 'customer.subscription.deleted':
            return this._webhookCustomerSubscriptionDeleted(webhookObject, res, next);
        default:
            console.warn('Unknown webhook: ', webhookObject.type)
            return next(errors.NotImplementedError(`Webhook ${webhookObject} not implemented yet`));
    }
}

StripeRouter.prototype._definitions = function () {
    return [
        ['GET', '/stripe/subscription', this.getLimiter(limiter(5000)), rawbody, this.getSubscription],
        ['POST', '/stripe/subscription', this.getLimiter(limiter(5000)), rawbody, this.postSubscription],
        ['POST', '/stripe/webhook', this.getLimiter(limiter(5000)), rawbody, this.parseWebhook]
    ];
}

module.exports = StripeRouter;