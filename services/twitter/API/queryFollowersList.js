
// #refactor:10 write queries
var util = require('util');
var T = require('../../../lib/twit.js');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.followers.api.list"); //turn lib into node module
var queue = require('../../../lib/kue.js');

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 29) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

setInterval( function() {
queue.inactiveCount( 'queryFollowersList', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");
queue.process('queryFollowersList', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryFollowersList received job %j", job);
  metrics.counter("start").increment();
  var user = job.data.user;
  var cursor = job.data.cursor || "-1";
  var promise = null;
  job.data.numReceived = job.data.numReceived || 0;
  if (cursor === "-1"){
    metrics.counter("freshQuery").increment();
    promise = checkFollowersListQueryTime(job.data.user)
  } else {
    metrics.counter("continuedQuery").increment();
    promise = new Promise(function(resolve) { resolve(); });
  }
  promise.then(function() {
    return queryFollowersList(user, cursor)
  }, function(err) {
    done();
  })
  .then(updateFollowersListQueryTime)
  .then(function(list) {
    metrics.counter("finish").increment();
    done();
  }, function(err) {
    logger.error("queryFollowersList error: %j %j", job, err);
    metrics.counter("queryError").increment();
    done(err);
  });

  function checkFollowersListQueryTime(user){
    return new Promise(function(resolve, reject) {
      var key = util.format("twitter:%s", user.id_str);
      var currentTimestamp = new Date().getTime();
      redis.hgetall(key, function(err, obj) {
        if ( obj & obj.queryFollowersListTimestamp ){
          if ( obj.queryFollowersListTimestamp > parseInt((+new Date) / 1000) - (60 * 60 ) ) {
              metrics.counter("repeatQuery").increment();
              reject( { message: "user recently queried" , timestamp:parseInt((+new Date) / 1000), queryTimestamp: obj.queryFollowersListTimestamp } );
          } else {
            resolve(user);
          }
        } else {
          resolve(user);
        }
      });
    });
  }

  function updateFollowersListQueryTime(result){
    var user = result.user;
    return new Promise(function(resolve, reject) {
      var key = util.format("twitter:%s", user.id_str);
      var currentTimestamp = new Date().getTime();
      redis.hset(key, "queryFollowersListTimestamp", parseInt((+new Date) / 1000), function() {
        resolve()
      });
    });
  }

function queryFollowersList(user, cursor) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryFollowersList");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('followers/list', { user_id: user.id_str, cursor: cursor, count: 200 }, function (err, data)
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
            metrics.counter("apiError").increment();
            reject({ message: "unknown twitter error", err: err });
            return;
          }
        }
        logger.trace("Data %j", data);
        logger.debug("queryFollowersList twitter api callback");
        logger.info("queryFollowersList %s found %d followers", user.screen_name, data.users.length);
        for (follower of data.users){
          queue.create('receiveUser', { user: follower } ).removeOnComplete(true).save();
          queue.create('receiveFriend', { user: { id_str: follower.id_str }, friend: { id_str: user.id_str } } ).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          var numReceived = job.data.numReceived + data.users.length;
          queue.create('queryFollowersList', { user: user, cursor: data.next_cursor_str, numReceived: numReceived }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("apiFinished").increment();
        resolve({ user: user, list: data.users });
      });
    });
  });
};

});
