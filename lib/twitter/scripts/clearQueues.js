
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');

var logger = require('tracer').colorConsole( {
  level: 'trace'
} );

var userByIDQueue = Queue('loadTwitterUserMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);
var userNetworkQueue = Queue('loadTwitterUserNetworkMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);
var relationshipQueue = Queue('loadRelationshipToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);

userByIDQueue.count().then(function(count) {
  logger.info("userByIDQueue %d", count);
});
userByIDQueue.empty().then(function() {
  logger.info("userByIDQueue emptied")
});
userByIDQueue.clean(5000);

userNetworkQueue.count().then(function(count) {
  logger.info("userNetworkQueue %d", count);
});
userNetworkQueue.empty().then(function() {
  logger.info("userNetworkQueue emptied")
});
userNetworkQueue.clean(5000);

relationshipQueue.count().then(function(count) {
  logger.info("relationshipQueue %d", count);
});
relationshipQueue.empty().then(function() {
  logger.info("relationshipQueue emptied")
});

relationshipQueue.clean(5000);
