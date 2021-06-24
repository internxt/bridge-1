const ShardAudit = require('./ShardAudit');
const AuditService = require('./service');
const log = require('../logger');

class FileAudit {
  constructor({ fileId, network, service = new AuditService(), attemptsPerShard }) {
    this._fileId = fileId;
    this._network = network;
    this._service = service;
    this._attemptsPerShard = attemptsPerShard;
  }

  async start() {
    try {
      const pointerIds = await this._service.getPointersIdsByFileId({ fileId: this._fileId });
      const shardHashes = [];

      if (pointerIds.length === 0) {
        log.error('File %s does not have pointers.', this._fileId);
        return;
      } 

      for (const pointerId of pointerIds) {
        try {
          const shardHash = await this._service.getShardHashByPointerId({ pointerId });
          shardHashes.push(shardHash);
        } catch (err) {
          log.error('Unexpected error for pointer %s', pointerId);
        }
      }

      if (shardHashes.length === 0) {
        log.error('No shards found for file %s', this._fileId);
        return;
      }

      log.info('Found %d shards for file %s', shardHashes.length, this._fileId);

      const shardAudit = new ShardAudit({ 
        network: this._network, 
        service: this._service, 
        attempts: this._attemptsPerShard 
      });

      for (const shardHash of shardHashes) {
        try {
          await shardAudit.byHash(shardHash);
        } catch (err) {
          log.error('Unexpected error for shard %s', shardHash);
        }
      }
    } catch (err) {
      log.error(err);
      return;
    }
  }
}

module.exports = FileAudit;