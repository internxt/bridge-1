/**
 * @module inxt-bridge/utils
 */

'use strict';

const async = require('async');
var through = require('through');
const crypto = require('crypto');
const assert = require('assert');

/**
 * Returns a transform stream that wraps objects written to it
 * in proper JSON array string
 */
module.exports.createArrayFormatter = function (transformer) {
  return through(function (entry) {
    if (!this._openBracketWritten) {
      this.queue('[');
      this._openBracketWritten = true;
    }

    if (this._needsPrecedingComma) {
      this.queue(',');
    } else {
      this._needsPrecedingComma = true;
    }

    this.queue(JSON.stringify(transformer(entry)));
  }, function () {
    if (!this._openBracketWritten) {
      this.queue('[');
    }
    this.queue(']');
    this.queue(null);
  });
};

/**
 * Sort by reputation, to be used with Array.prototype.sort
 * @param {Object} - a
 * @param {Object} - b
 */
module.exports.sortByReputation = function (a, b) {
  const a1 = a.contact.reputation >= 0 ? a.contact.reputation : 0;
  const b1 = b.contact.reputation >= 0 ? b.contact.reputation : 0;

  return (a1 === b1) ? 0 : (a1 > b1) ? -1 : 1;
};

/**
 * Will get a timestamp integer from a string or number
 * argument, including ISO formatted strings.
 * @param {*} - The variable to parse
 */
module.exports.parseTimestamp = function (arg) {
  let startDate = new Date(arg);
  if (Number.isInteger(startDate.getTime())) {
    return startDate.getTime();
  }
  const startDateTimestamp = parseInt(arg);
  if (Number.isInteger(startDateTimestamp)) {
    startDate = new Date(startDateTimestamp);
  }
  if (Number.isInteger(startDate.getTime())) {
    return startDate.getTime();
  }

  return 0;
};

/**
 * Will check to see if a variable is valid MongoDB object id
 * @param {*} - The variable to test
 */
module.exports.isValidObjectId = function (id) {
  if (typeof id !== 'string') {
    return false;
  }

  return /^[0-9a-fA-F]{24}$/.test(id);
};

/**
 * Will expand JSON strings into objects
 * @param {Object|String} - A string or object with potential JSON strings
 */
module.exports.recursiveExpandJSON = function (value) {
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch (e) {
      // noop
    }
  }
  if (typeof value === 'object') {
    for (let prop in value) {
      value[prop] = module.exports.recursiveExpandJSON(value[prop]);
    }
  }

  return value;
};

module.exports.distinct = function (value, index, self) {
  return self.indexOf(value) === index;
};

/**
 * Will iterate an Aggregation Cursor
 */
module.exports.AggregationCursor = function (model, query, manageData, callback) {
  const cursor = model.aggregate(query).allowDiskUse(true).cursor().exec();
  const untilFunction = (next) => cursor.next().then(data => {
    if (!data) {
      return next(null, data);
    }
    manageData(data, (err) => next(err, data));
  }).catch(next);
  const untilCondition = (data, next) => next(null, !data);
  async.doUntil(untilFunction, untilCondition, () => cursor.close(callback));

  return cursor;
};

module.exports.randomTime = (max, min) => {
  const range = max - min;

  assert(Number.isSafeInteger(range));
  assert(range > 0, 'maxInterval is expected to be greater than minInterval');

  const entropy = crypto.randomBytes(8).toString('hex');
  const offset = Math.round(parseInt('0x' + entropy) / Math.pow(2, 64) * range);

  return min + offset;
};
