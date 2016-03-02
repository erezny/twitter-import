
// #refactor:10 write queries
var util = require('util');
var T = require('../../../lib/twit.js');

var assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.followers.api.ids"); //turn lib into node module
var queue = require('../../../lib/kue.js');

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
queue.inactiveCount( 'queryFollowersIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

queue.process('queryFollowersIDs', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryFollowersIDs received job %j", job);
  metrics.counter("start").increment();
  var user = job.data.user;
  var cursor = job.data.cursor || "-1";
  var promise = null;
  job.data.numReceived = job.data.numReceived || 0;
  if (cursor === "-1"){
    metrics.counter("freshQuery").increment();
    promise = checkFollowersIDsQueryTime(job.data.user)
  } else {
    metrics.counter("continuedQuery").increment();
    promise = new Promise(function(resolve) { resolve(); });
  }
  promise.then(function() {
    return queryFollowersIDs(user, cursor)
  }, function(err) {
    done();
  })
  .then(updateFollowersIDsQueryTime)
  .then(function(list) {
    metrics.counter("finish").increment();
    done();
  }, function(err) {
    logger.error("queryFollowersIDs error: %j %j", job, err);
    metrics.counter("queryError").increment();
    if (err.message == "Not authorized."){
      //blacklist
      done();
    } else {
      done(err);
    }
  });

  function checkFollowersIDsQueryTime(user){
    return new Promise(function(resolve, reject) {
      var key = util.format("twitter:%s", user.id_str);
      var currentTimestamp = new Date().getTime();
      redis.hgetall(key, function(err, obj) {
        if ( obj & obj.queryFollowersIDsTimestamp && obj.queryFollowersIDsTimestamp > parseInt((+new Date) / 1000) - (60 * 60 * 24) ) {
          resolve(user);
        } else {
          metrics.counter("repeatQuery").increment();
          reject( { message: "user recently queried" } );
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
        metrics.counter("updatedTimestamp").increment();
        resolve()
      });
    });

function queryFollowersIDs(user, cursor) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryFollowersIDs");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('followers/ids', { user_id: user.id_str, cursor: cursor, count: 500, stringify_ids: true }, function (err, data)
      {
        if (err){
          logger.error("twitter api error %j %j", user, err);
          metrics.counter("apiError").increment();
          reject(err);
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryFollowersIDs twitter api callback");
        logger.info("queryFollowersIDs %s found %d followers", user.screen_name, data.ids.length);
        for (follower of data.ids){
          queue.create('receiveFriend', { user: { id_str: follower }, friend: { id_str: user.id_str } } ).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          var numReceived = job.data.numReceived + data.ids.length;
          queue.create('queryFollowersIDs', { user: user, cursor: data.next_cursor_str, numReceived: numReceived }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("apiFinished").increment();
        resolve(data.users);
      });
    });
  });
};

});
