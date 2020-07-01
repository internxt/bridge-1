const EventEmitter = require('events').EventEmitter;
const complex = require('storj-complex');
const Storage = require('storj-service-storage-models');
const logger = require('../logger');
const ms = require('ms');
const async = require('async');
const assert = require('assert');
const Complex = require('storj-complex/lib/client')

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
            return logger.warn('Renewal already initialized');
        }

        const { mongoUrl, mongoOpts } = this.config.storage;

        this.network = complex.createClient(this.config.complex);
        this.storage = new Storage(mongoUrl, mongoOpts, { logger });
        this.initialized = true;

        this.emit('initialized');
    }

    build(network, storage) {
        if (this.initialized) {
            return logger.warn('Renewwal already initialized');
        }

        assert(network instanceof Complex);
        assert(storage instanceof Storage);

        this.network = network;
        this.storage = storage;
        this.initialized = true;
    }

    initContractRenew() {
        const NOW = Date.now();

        this.cursor = this.storage.models.Shard.find({
            'contracts.contract.store_end': {
                $gte: NOW,
                $lte: NOW + HOURS_24 * 7
            }
        }).cursor();

        this.cursor
            .on('error', (err) => {
                console.error('Cursor error', err)
            })
            .on('data', (shard) => {
                let needsRenewal = [];

                this.cursor.pause();

                for (let contractInfo of shard.contracts) {
                    let { contract: contractObj, nodeID } = contractInfo;
                    let contract = new storj.Contract(contractObj);
                    needsRenewal.push([nodeID, contract]);
                }

                let renewalContracts = needsRenewal.map(([nodeId, contract]) => {
                    contract.set('store_end', NOW + ms('365d'));
                    return [nodeId, contract];
                });

                async.map(renewalContracts, this.lookupFarmer.bind(this), this.maybeRenewContracts.bind(this));
            })
            .on('end', () => {
                this.emit('end');
            });
    }

    lookupFarmer([nodeId, contract], next) {
        this.storage.models.Contact.findOne({ _id: nodeId }, (err, contact) => {
            if (err || !contact) {
                this.emit('counter-errored');
                return next(null, null);
            }
            next(null, [storj.Contact(contact.toObject()), contract]);
        });
    }

    maybeRenewContracts(err, results) {
        let canBeRenewed = results.filter((result) => !!result);
        async.eachLimit(canBeRenewed, 6, this.checkIfNeedsRenew.bind(this), () => this.cursor.resume());
    }

    forceRenewContracts(err, results) {
        let canBeRenewed = results.filter((result) => !!result);
        async.eachLimit(canBeRenewed, 6, this.renewContract.bind(this), () => {});
    }

    checkIfNeedsRenew([contact, contract], done) {
        let shardHash = contract.get('data_hash');
        async.waterfall([
            (next) => this.getPointerObjects.call(this, shardHash, next),
            (pointers, next) => this.getAssociatedFrames(pointers, next),
            (frames, next) => this.getParentBucketEntries(frames, next),
            (next) => this.renewContract([contact, contract], next)
        ], (err) => {
            this.finishProcessingContract(done)
        });
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
                return next(new Error('No frames found'))
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
                return next(new Error('No bucket entries found'))
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
                logger.error('Renewal error: contracts is null')
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