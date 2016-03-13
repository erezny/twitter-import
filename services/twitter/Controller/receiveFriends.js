
var util = require('util');
var assert = require('assert');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friends",
  mvc: "controller",
  function: "receive",
  kue: "receiveFriend",
});
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
var redisKey = function(user) { return util.format("twitter:%s", user.id_str); ;
var redisRelKey = function(rel) { return util.format("twitter-friend:%s:%s", rel.user.id_str, rel.friend.id_str) }; };

setInterval( function() {
  queue.inactiveCount( 'receiveFriend', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
  }, 15 * 1000 );

const metricRelExists = metrics.counter("rel_exists");
const metricUserNotExist = metrics.counter("user_not_exist");
const metricFinish = metrics.counter("finish");
const metricStart = metrics.counter("start");
const metricError = metrics.counter("error");

function lookupNeo4jID(user, rel){
  return new RSVP.Promise( function(resolve, reject) {
    redis.hgetall(redisKey(user), function(err, redisUser) {
      if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined" && parseInt(redisUser.neo4jID) > 1){
        resolve({ id: parseInt(redisUser.neo4jID) });
      } else {
        queue.create('receiveStubUser', { user: user, rel: rel } ).removeOnComplete( true ).save();
        metricUserNotExist.increment();
        reject({ message: "user doesn't exist" });
      }
    });
  });
}

function lookupRel(rel){
  return new RSVP.Promise( function(resolve, reject) {
    redis.exists(redisRelKey(rel), function(err, num) {
      if (num >= 1){
        metricRelExists.increment();
        reject({ message: "relationship exists" });
      } else {
        RSVP.hash({ user: lookupNeo4jID(rel.user, rel), friend: lookupNeo4jID(rel.friend, rel) })
        .then(function(results) {
          redis.hset(redisRelKey(rel), "imported", parseInt((+new Date) / 1000));
          queue.create('saveFriend', { user: results.user, friend: results.friend } ).removeOnComplete( true ).save();
          resolve(rel);
        }, function(err) {
          reject(err);
        });
      }
    });
  });
}

function receiveFriend (job, done) {
  logger.trace("received job %j", job);
  metricStart.increment();
  var rel = job.data;

  function finished (result){
    return new RSVP.Promise( function (resolve, reject) {
      metricFinish.increment();
      logger.trace("finish");
      resolve();
    });
  }

  lookupRel(rel)
  .then(finished, function(err) {
    if ( err == {} || err.message == "relationship exists"  ) {
      finished().then(done);
    } else {
      metricError.increment();
      done(err);
    }
  })
  .then(done);

};

queue.process('receiveFriend', 10, receiveFriend );
