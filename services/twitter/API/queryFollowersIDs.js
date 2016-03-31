
var util = require('util');
var T = require('../../../lib/twit.js');
var _ = require('../../../lib/util.js');
var assert = require('assert');

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "followersIDs",
  mvc: "api",
  function: "query",
  kue: "queryFollowersIDs",
});
var queue = require('../../../lib/kue.js');
var neo4j = require('../../../lib/neo4j.js');
var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

function count_queue(){
  queue.inactiveCount( 'queryFollowersIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
};
setInterval( count_queue, 15 * 1000 );

const metricRelSaved = metrics.counter("rel_saved");
const metricRelError = metrics.counter("rel_error");
const metricStart = metrics.counter("start");
const metricFreshQuery = metrics.counter("freshQuery");
const metricContinuedQuery = metrics.counter("continuedQuery");
const metricFinish = metrics.counter("finish");
const metricQueryError = metrics.counter("queryError");
const metricRepeatQuery = metrics.counter("repeatQuery");
const metricUpdatedTimestamp = metrics.counter("updatedTimestamp");
const metricApiError = metrics.counter("apiError");
const metricApiFinished = metrics.counter("apiFinished");
const metricTxnFinished = metrics.counter("txnFinished");
const metricTxnError = metrics.counter("txnError");

queue.process('queryFollowersIDs', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryFollowersIDs received job %j", job);
  metricStart.increment();
  var user = job.data.user;
  var cursor = job.data.cursor || "-1";
  var promise = null;
  job.data.numReceived = job.data.numReceived || 0;
  if (cursor === "-1"){
    metricFreshQuery.increment();
    promise = checkFollowersIDsQueryTime(job.data.user);
  } else {
    metricContinuedQuery.increment();
    promise = new Promise(function(resolve) { resolve(); });
  }
  promise.then(function() {
    return queryFollowersIDs(user, cursor, job);
  })
  .then(saveFollowers)
  .then(updateFollowersIDsQueryTime)
  .then(function(list) {
    metricFinish.increment();
    done();
  }, function(err) {
    logger.debug("queryFollowersIDs error: %j %j", job, err);
    metricQueryError.increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
    done(err);
  });
});

  function saveFollowers(result) {
    return new Promise(function(resolve, reject) {
      var user = result.user;
      var followers = result.list;
      logger.info("save");

      var uniqueUsers = [];
      for ( var follower of followers ) {
        if ( uniqueUsers.indexOf(follower) === -1 ) {
          uniqueUsers.push(follower);
        }
      }

      var query = {
        statements: [
          {
            statement: "merge (f:twitterUser { id_str: {user}.id_str })",
            parameters: {
              'user': {
                id_str: user.id_str
              }
            }
          }
        ]
      };
      for ( var follower of uniqueUsers ) {
        query.statements.push({
          statement: "match (f:twitterUser { id_str: {user}.id_str }) " +
                     "merge (u:twitterUser { id_str: {follower}.id_str }) " +
                     "merge (u)-[:follows]->(f) ",
          parameters: {
            'user': { id_str: user.id_str },
            'follower': { id_str: follower.id_str }
          }
        });
      }
      var operation = neo4j.operation('transaction/commit', 'POST', query);
      neo4j.call(operation, function(err, result, response) {
        if (!_.isEmpty(err)){
          logger.error("query error: %j", err);
          metricTxnError.increment();
          reject(err);
        } else {
          logger.info("committed");
          metricTxnFinished.increment();
          resolve(result);
        }
      });
    });
  }

  function checkFollowersIDsQueryTime(user){
    return new Promise(function(resolve, reject) {
      var key = util.format("twitter:%s", user.id_str);
      var currentTimestamp = new Date().getTime();
      redis.hgetall(key, function(err, obj) {
        if ( obj & obj.queryFollowersIDsTimestamp ){
          if ( obj.queryFollowersIDsTimestamp > parseInt((+new Date) / 1000) - (60 * 60 ) ) {
              metricRepeatQuery.increment();
              reject( { message: "user recently queried" , timestamp:parseInt((+new Date) / 1000), queryTimestamp: obj.queryFollowersIDsTimestamp } );
          } else {
            resolve(user);
          }
        } else {
          resolve(user);
        }
      });
    });
  }

  function updateFollowersIDsQueryTime(result){
    var user = result.user;
    return new Promise(function(resolve, reject) {
      var key = util.format("twitter:%s", user.id_str);
      var currentTimestamp = new Date().getTime();
      redis.hset(key, "queryFollowersIDsTimestamp", parseInt((+new Date) / 1000), function() {
        metricUpdatedTimestamp.increment();
        resolve();
      });
    });
  }

function queryFollowersIDs(user, cursor, job) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryFollowersIDs");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('followers/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, function (err, data)
      {
        if (!_.isEmpty(err)){
          if (err.message == "Not authorized."){
            queue.create('markUserPrivate', { user: user } ).removeOnComplete(true).save();
            resolve({ user: user, list: [] });
            return;
          } else if (err.message == "User has been suspended."){
            queue.create('markUserSuspended', { user: user } ).removeOnComplete(true).save();
            resolve({ user: user, list: [] });
            return;
          } else {
            logger.error("twitter api error %j %j", user, err);
            metricApiError.increment();
            reject({ message: "unknown twitter error", err: err });
            return;
          }
        }
        logger.trace("Data %j", data);
        logger.debug("queryFollowersIDs twitter api callback");
        logger.info("queryFollowersIDs %s found %d followers", user.screen_name, data.ids.length);

        if (data.next_cursor_str !== '0'){
          var numReceived = job.data.numReceived + data.ids.length;
          queue.create('queryFollowersIDs', { user: user, cursor: data.next_cursor_str, numReceived: numReceived }).attempts(5).removeOnComplete( true ).save();
        }
        metricApiFinished.increment();
        resolve({ user: user, list: data.ids });
      });
    });
  });
};
