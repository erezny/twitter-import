
var util = require('util');
var T = require('../../../lib/twit.js');
var assert = require('assert');

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friendsIDs",
  mvc: "api",
  function: "query",
  kue: "queryFriendsIDs",
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

setInterval( function() {
queue.inactiveCount( 'queryFriendsIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

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

queue.process('queryFriendsIDs', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryFriendsIDs received job %j", job);
  metricStart.increment();
  var user = job.data.user;
  var cursor = job.data.cursor || "-1";
  var promise = null;
  job.data.numReceived = job.data.numReceived || 0;
  if (cursor == "-1"){
    metricFreshQuery.increment();
    promise = checkFriendsIDsQueryTime(job.data.user)
  } else {
    metricContinuedQuery.increment();
    promise = new Promise(function(resolve) { resolve(); });
  }
  promise.then(function() {
    return queryFriendsIDs(user, cursor, job)
  }, function(err) {
    done();
  })
  .then(saveFriends)
  .then(updateFriendsIDsQueryTime)
  .then(function(result) {
    metricFinish.increment();
    done();
  }, function(err) {
    logger.error("queryFriendsIDs error: %j %j", job, err);
    metricQueryError.increment();
    done(err);
  });

});

const cypher = "merge (x:twitterUser { id_str: {user} }) " +
            "merge (y:twitterUser { id_str: {friend} }) " +
            "merge (x)-[r:follows]-(y) ";

function saveFriends(result) {
  return new Promise(function(resolve, reject) {
    var user = result.user;
    var friends = result.list;
    var txn = neo4j.batch();
    logger.info("save");

    for (friend of friends){
      txn.query(cypher, { user: user.id_str, friend: friend } , function(err, results) {
        if (err){
          metricRelError.increment();
        } else {
          metricRelSaved.increment();
        }
      });
    }
    process.nextTick(function() {
      logger.info("commit");
      txn.commit(function (err, results) {
        metricTxnFinished.increment();
        resolve(result);
      });
    })
  });
}

function checkFriendsIDsQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hgetall(key, function(err, obj) {
      if ( obj & obj.queryFriendsIDsTimestamp ){
        if ( obj.queryFriendsIDsTimestamp > parseInt((+new Date) / 1000) - (60 * 60 ) ) {
            metricRepeatQuery.increment();
            reject( { message: "user recently queried" , timestamp:parseInt((+new Date) / 1000), queryTimestamp: obj.queryFriendsIDsTimestamp } );
        } else {
          resolve(user);
        }
      } else {
        resolve(user);
      }
    });
  });
}

function updateFriendsIDsQueryTime(result){
  var user = result.user;
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hset(key, "queryFriendsIDsTimestamp", parseInt((+new Date) / 1000), function() {
      metricUpdatedTimestamp.increment();
      resolve()
    });
  });
}

function queryFriendsIDs(user, cursor, job) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryFriendsIDs");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('friends/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, function (err, data)
      {
        if (err){
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
        logger.debug("queryFriendsIDs twitter api callback");
        logger.info("queryFriendsIDs %s found %d friends", user.screen_name, data.ids.length);

        if (data.next_cursor_str !== '0'){
          var numReceived = job.data.numReceived + data.ids.length;
          queue.create('queryFriendsIDs', { user: user, cursor: data.next_cursor_str, numReceived: numReceived }).attempts(5).removeOnComplete( true ).save();
        }
        metricApiFinished.increment();
        resolve({ user: user, list: data.ids });
      });
    });
  });
};
