
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.stubUsers.controller");
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

var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");
function receiveUser(job, done) {
  logger.trace("received job %j", job);
  metrics.counter("started").increment();
  var user = job.data.user;
  var rel = job.data.rel;

  var key = util.format("twitter:%s", user.id_str);
  redis.hgetall(key, function(err, redisUser) {
    if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
      redis.hget(key, "saveTimestamp", function(err, result) {
        queue.create('receiveFriend', rel ).removeOnComplete( true ).save();
        done();
      });
    } else {
      dostuff(user, done);
    }
  });

};

var metricsFinished = metrics.counter("processFinished");
var metricsError = metrics.counter("processError");
function dostuff(user, done){
  return metricNeo4jTimeMsec.time(upsertStubUserToNeo4j(user))
  .then(function(savedUser) {
    logger.trace("savedUser: %j", savedUser);
    metricsFinished.increment();
    queue.create('receiveFriend', rel ).removeOnComplete( true ).save();
    done();
  }, function(err) {
    logger.error("receiveStubUser error on %j\n%j\n--", job, err);
    metricsError.increment();
    done(err);
  });
}

queue.process('receiveStubUser', 1, receiveUser);

setInterval( function() {
  queue.inactiveCount( 'receiveStubUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
}, 15 * 1000 );

function updateUserSaveTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hset(key, "saveTimestamp", parseInt((+new Date) / 1000), function() {
      resolve(user)
    });
  });
}

function upsertStubUserToNeo4j(user) {
  return function() {
  delete user.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.debug('saving user %s', user.id_str);
    neo4j.save(user, function(err, savedUser) {
      if (err){
        logger.error("neo4j save %s %j", user.id_str, err);
        metrics.counter("neo4j_save_error").increment();
        reject({ err:err, reason:"neo4j save user error" });
        return;
      }
      logger.debug('inserted user %s', savedUser.id_str);
      neo4j.label(savedUser, "twitterUser", function(err, labeledUser) {
        if (err){
          logger.error("neo4j label error %s %j", user.id_str, err);
          metrics.counter("neo4j_label_error").increment();
          reject({ err:err, reason:"neo4j label user error" });
          return;
        }
        redis.hset(util.format("twitter:%s",savedUser.id_str), "neo4jID", savedUser.id, function(err, res) {
          logger.debug('labeled user %s', savedUser.id_str);
          metrics.counter("neo4j_inserted").increment();
          resolve(savedUser);
        });
      });
    });
  });
  }
}
