
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.friends.controller.receive");
var queue = require('../../../lib/kue.js');
var neo4j = require('../../../lib/neo4j.js');

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

setInterval( function() {
  queue.inactiveCount( 'receiveFriend', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
  }, 15 * 1000 );

var metricRelInBloomfilter = metrics.counter("rel_in_bloomfilter");
var metricFriendNotExist = metrics.counter("friend_not_exist");
var metricUserNotExist = metrics.counter("user_not_exist");
var metricFinish = metrics.counter("finish");
var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");

function receiveFriend (job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  var user = job.data.user;
  var friend = job.data.friend;
  var rel = job.data;
  metrics.counter("start").increment();

  RSVP.hash({ user: lookupNeo4jID(user, rel), friend: lookupNeo4jID(friend, rel) })
  .then(function(results) {
    logger.trace("ready to upsert");
    queue.create('saveFriend', { user: results.user, friend: results.friend } ).removeOnComplete( true ).save();
      metricFinish.increment();
  }, function(err) {
    logger.debug("neo4j user not in redis %j",err);
    metricUserNotExist.increment();
    done(); //avoid retries
  });
};

//var redisCache = [];
function lookupNeo4jID(user, rel){

  return new RSVP.Promise( function(resolve, reject) {
    logger.trace("querying redis");
    redis.hgetall(util.format("twitter:%s", user.id_str), function(err, redisUser) {
      logger.trace("finished querying redis");
      if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
    //    redisCache.push([ user.id_str, { id: parseInt(redisUser.neo4jID) }, 0 ])
        resolve({ id: parseInt(redisUser.neo4jID) });
      } else {
        queue.create('receiveStubUser', { user: user, rel: rel } ).removeOnComplete( true ).save();
        logger.trace("save failed");
        reject(err);
      }
    });
  });
}

queue.process('receiveFriend', 10, receiveFriend );
