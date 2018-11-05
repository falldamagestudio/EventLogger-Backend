
'use strict';

module.exports = Object.assign(
  require('./logEvents'),
  require('./saveLogEventsToBigQuery'),
);

// exports.logEvents = require('logEvents');
// exports.saveLogEventsToBigQuery = require('saveLogEventsToBigQuery');

