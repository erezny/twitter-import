
// #refactor:10 write queries
var util = require('util');
var Twit = require('twit');
var T = require('../../../lib/twit.js');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friendsIDs",
  mvc: "api",
  function: "query",
  kue: "queryFriendsIDs",
});
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
queue.inactiveCount( 'queryFriendsIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

queue.process('queryFriendsIDs', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryFriendsIDs received job %j", job);
  metrics.counter("start").increment();
  var user = job.data.user;
  var cursor = job.data.cursor || "-1";
  var promise = null;
  job.data.numReceived = job.data.numReceived || 0;
  if (cursor == "-1"){
    metrics.counter("freshQuery").increment();
    promise = checkFriendsIDsQueryTime(job.data.user)
  } else {
    metrics.counter("continuedQuery").increment();
    promise = new Promise(function(resolve) { resolve(); });
  }
  promise.then(function() {
    return queryFriendsIDs(user, cursor)
  }, function(err) {
    done();
  })
  .then(updateFriendsIDsQueryTime)
  .then(function(result) {
    metrics.counter("finish").increment();
    done();
  }, function(err) {
    logger.error("queryFriendsIDs error: %j %j", job, err);
    metrics.counter("queryError").increment();
    done(err);
  });

function checkFriendsIDsQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hgetall(key, function(err, obj) {
      if ( obj & obj.queryFriendsIDsTimestamp ){
        if ( obj.queryFriendsIDsTimestamp > parseInt((+new Date) / 1000) - (60 * 60 ) ) {
            metrics.counter("repeatQuery").increment();
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
      metrics.counter("updatedTimestamp").increment();
      resolve()
    });
  });
}

function queryFriendsIDs(user, cursor) {
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
            metrics.counter("apiError").increment();
            reject({ message: "unknown twitter error", err: err });
            return;
          }
        }
        logger.trace("Data %j", data);
        logger.debug("queryFriendsIDs twitter api callback");
        logger.info("queryFriendsIDs %s found %d friends", user.screen_name, data.ids.length);
        for (friend of data.ids){
          queue.create('receiveFriend', { user: { id_str: user.id_str }, friend: { id_str: friend } } ).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          var numReceived = job.data.numReceived + data.ids.length;
          queue.create('queryFriendsIDs', { user: user, cursor: data.next_cursor_str, numReceived: numReceived }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("apiFinished").increment();
        resolve({ user: user, list: data.ids });
      });
    });
  });
};

});
