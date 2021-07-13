const async = require('async');
const crypto = require('crypto');
const storj = require('storj-lib');
const ms = require('ms');

function Actions(config, storage, network, contracts) {
  if (!(this instanceof Actions)) {
    return new Actions(config, storage, network, contracts);
  }

  this.config = config;
  this.storage = storage;
  this.network = network;
  this.contracts = contracts;
}

Actions.prototype._selectFarmers = function (blacklist, callback) {
  const Contact = this.storage.models.Contact;
  const AvgRep = this.config.application.publishBenchThreshold;
  const PublishTotal = this.config.application.publishTotal;

  async.parallel([
    next => {
      Contact.find({
        _id: { $lte: crypto.randomBytes(20).toString('hex'), $nin: blacklist },
        reputation: { $gt: AvgRep },
        timeoutRate: { $lt: this.config.application.timeoutRateThreshold },
        spaceAvailable: true
      })
        .sort({ _id: -1 })
        .limit(PublishTotal)
        .exec(next);
    },
    next => {
      Contact.find({
        _id: { $lte: crypto.randomBytes(20).toString('hex'), $nin: blacklist },
        reputation: { $lte: AvgRep },
        timeoutRate: { $lt: this.config.application.timeoutRateThreshold },
        spaceAvailable: true
      })
        .sort({ _id: -1 })
        .limit(PublishTotal)
        .exec(next);
    }
  ], (err, results) => {
    if (err) {
      return callback(err);
    }
    const combined = results[0].concat(results[1]);
    callback(null, combined);
  });

};

Actions.prototype._publishContract = function (nodes, contract, audit, callback) {
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
          return callback(new Error('Save contract error: ' + err.message));
        }

        callback(null, farmerContact, farmerContract, data.token);
      });
    });
  });
};

Actions.prototype._getContractForShard = function (contr, audit, blacklist, callback) {
  this._selectFarmers(blacklist, (err, farmers) => {
    if (err) {
      return callback(err);
    }

    if (!farmers || !farmers.length) {
      return callback(new Error('Could not locate farmers'));
    }

    this._publishContract(farmers, contr, audit, (err, farmerContact, farmerContract, token) => {
      if (err) {
        return callback(err);
      }
      callback(null, farmerContact, farmerContract, token);
    });

  });
};

Actions.prototype._getContractForShardToFarmer = function (nodeID, contr, audit, blacklist, callback) {

  const { Contact } = this.storage.models;

  Contact.findOne({ _id: nodeID }, (err, farmer) => {
    if (err) {
      return callback(new Error('Cannot find farmer'));
    }
    if (!farmer) {
      return callback(new Error('Could not locate farmers'));
    }

    const farmers = [farmer];

    this._publishContract(farmers, contr, audit, (err, farmerContact, farmerContract, token) => {
      if (err) {
        return callback(err);
      }
      callback(null, farmerContact, farmerContract, token);
    });
  });
};


Actions.prototype.publicNewContractToFarmer = async function (hash, nodeID, callback) {
  const { Pointer } = this.storage.models;
  const pointer = await Pointer.findOne({ hash: hash });
  if (!pointer) {
    return callback(new Error('Pointer not found for shard ' + hash));
  }
  const challenges = pointer.challenges;
  const tree = pointer.tree;

  const audit = storj.AuditStream.fromRecords(challenges, tree);

  const contract = new storj.Contract({
    data_size: pointer.size,
    data_hash: pointer.hash,
    store_begin: Date.now(),
    store_end: Date.now() + ms('3650d'),
    audit_count: challenges.length
  });

  this._getContractForShardToFarmer(nodeID, contract, audit, [], (err, _contact, _contract, _token) => {
    return callback(err, _contact, _contract, _token);
  });
};

Actions.prototype.publishNewContractsForShard = function (hash, callback) {
  const { Pointer } = this.storage.models;

  Pointer.findOne({ hash: hash }, (err, pointer) => {
    if (err) {
      return callback(err);
    }

    if (!pointer) {
      return callback(new Error('Pointer not found for shard ' + hash));
    }

    const challenges = pointer.challenges;
    const tree = pointer.tree;

    const audit = storj.AuditStream.fromRecords(challenges, tree);

    const contract = new storj.Contract({
      data_size: pointer.size,
      data_hash: pointer.hash,
      store_begin: Date.now(),
      store_end: Date.now() + ms('3650d'),
      audit_count: challenges.length
    });

    this._getContractForShard(contract, audit, [], (err, _contact, _contract, _token) => {
      return callback(err, _contact, _contract, _token);
    });
  });
};

module.exports = Actions;