const ShardAudit = require('./ShardAudit');
const AuditService = require('./service');
const log = require('../logger');
const { mapSeries } = require('async');

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

      if (shardHashes.length === 0) {
        log.error('No shards found for file %s', this._fileId);
        return;
      } else {
        log.info('Found %d shards for file %s', shardHashes.length, this._fileId);
      }

      const shardAudit = new ShardAudit({ 
        network: this._network, 
        service: this._service, 
        attempts: this._attemptsPerShard 
      });

      await mapSeries(shardHashes, (shardHash, next) => {
        console.log(shardHash);
        Promise.all([
          this._service.getMirrorByShardHash({ shardHash }),
          this._service.getShardByShardHash({ shardHash })
        ]).then(([mirror, shard]) => {
          return shardAudit
            .setShard({ shard: mirror })
            .setShardFromShards({ shard })
            .start();
        }).catch((err) => {
          log.error(`Unexpected error for shard with hash ${shardHash}`);
          log.error(err);
        }).finally(() => {
          next();
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