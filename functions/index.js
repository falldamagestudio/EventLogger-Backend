
'use strict';

module.exports = Object.assign(
  require('./logEvents'),
  require('./saveLogEventsToBigQuery'),
  require('./processAuthenticatedEvent')
);

// exports.logEvents = require('logEvents');
// exports.saveLogEventsToBigQuery = require('saveLogEventsToBigQuery');

