const ShardAudit = require('./ShardAudit');
const AuditService = require('./service');
const log = require('../logger');
const { mapSeries, map } = require('async');

class FileAudit {
  constructor({ fileId, network, service = new AuditService(), attemptsPerShard }) {
    this._fileId = fileId;
    this._network = network;
    this._service = service;
    this._attemptsPerShard = attemptsPerShard;
    this._shardsAudited = [];
  }

  async start() {
    try {
      const pointerIds = await this._service.getPointersIdsByFileId({ fileId: this._fileId });
      const shardHashes = [];

      await mapSeries(pointerIds, (pointerId, next) => {
        this._service.getShardHashByPointerId({ pointerId }).then((shardHash) => {
          shardHashes.push(shardHash);
          next();
        }).catch((err) => {
          log.error(`Unexpected error for pointer ${pointerId}`);
          next(err);
        });
      });

      const shardAudit = new ShardAudit({ 
        network: this._network, 
        service: this._service, 
        attempts: this._attemptsPerShard 
      });

      await mapSeries(shardHashes, (shardHash, next) => {
        this._service.getShardByShardHash({ shardHash })
          .then((shard) => {
            // audit them
            shardAudit.setShard({ shard });
            shardAudit.start().then(() => {
              this._shardsAudited.push(shardAudit.getShardAudited());
              next();
            }).catch((err) => {
              log.error(`Unexpected error for shard with hash ${shardHash}`);
              next(err);
            });
          });
      });
    } catch (err) {
      log.error(err);
      return;
    }
  }

  getShardsAudited() {
    return this._shardsAudited;
  }
}

module.exports = FileAudit;