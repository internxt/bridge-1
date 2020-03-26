'use strict';

const async = require('async');
const storj = require('storj-lib');
const log = require('../logger');
const utils = require('../utils')

function Replicate(monitor) {
    if (!(this instanceof Replicate)) {
        return new Replicate(config);
    }

    this.monitor = monitor;
}

Replicate.prototype.triggerMirrorEstablish = function (n, hash, done) {
    const Mirror = this.monitor.storage.models.Mirror;
    const self = this.monitor;
    let item = null;

    function _loadShard(callback) {
        console.log('Load shard')
        self.contracts.load(hash, (err, _item) => {
            if (err) {
                return callback(err);
            }
            item = _item;
            callback();
        });
    }

    function _getMirrors(callback) {
        console.log('get mirrors')
        Mirror.find({ shardHash: hash }).populate('contact').exec(callback);
    }

    function _getMirrorCandidate(mirrors, callback) {
        console.log('get mirror candidate')
        let established = [], available = [];

        mirrors.forEach((m) => {
            if (!m.contact) {
                log.warn('Mirror %s is missing contact in database', m._id);
            } else if (!m.isEstablished) {
                available.push(m);
            } else {
                established.push(m);
            }
        });

        if (available.length === 0) {
            return callback(new Error('No available mirrors'));
        }

        if (established.length >= n) {
            return callback(new Error('Auto mirroring limit is reached'));
        }

        available.sort(utils.sortByReputation);

        callback(null, available.shift());
    }

    function _getRetrievalTokenFromFarmer(mirror, callback) {
        console.log('get retrieval token from farmer')
        let farmers = Object.keys(item.contracts);
        let pointer = null;
        let test = () => farmers.length === 0 || pointer !== null;
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
                            log.warn('Unable to get pointer for mirroring, reason: %s',
                                err.message);
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
        console.log('establish mirror')
        self.network.getMirrorNodes(
            [source],
            [contact],
            (err) => {
                if (err) {
                    return callback(err);
                }

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

module.exports = Replicate