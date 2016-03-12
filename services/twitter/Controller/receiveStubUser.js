
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');
const kueThreads = parseInt(process.env.KUE_THREADS) || 500;
const neo4jThreads = parseInt(process.env.NEO4J_THREADS) || 100;

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "stubUser",
  mvc: "controller",
  function: "receive",
  kue: "receiveStubUser",
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

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / process.env.NEO4j_LIMIT_NODE_TXPS ) * 60 * 1000);

var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");
function receiveUser(job, done) {
  logger.trace("received job %j", job);
  metrics.counter("started").increment();
  var user = job.data.user;
  var rel = job.data.rel;

  limiter.removeTokens(1, function(err, remainingRequests) {
    var key = util.format("twitter:%s", user.id_str);
    redis.hgetall(key, function(err, redisUser) {
      if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
        redis.hget(key, "saveTimestamp", function(err, result) {
          queue.create('receiveFriend', rel ).removeOnComplete( true ).save();
          done();
        });
      } else {
        dostuff(user, rel, done);
      }
    });
  });

};

var metricsFinished = metrics.counter("processFinished");
var metricsError = metrics.counter("processError");
function dostuff(user, rel, done){
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

queue.process('receiveStubUser', kueThreads, receiveUser);

setInterval( function() {
  queue.inactiveCount( 'receiveStubUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
}, 15 * 1000 );

var txn = neo4j.batch();
var txn_count = 0;
var sem = require('semaphore')(1);
function upsertStubUserToNeo4j(user) {
  return function() {
    delete user.id;
    return new RSVP.Promise( function (resolve, reject) {
      sem.take(function() {//timings
      logger.debug('saving user %s', user.id_str);

      if (txn_count > neo4jThreads ){
        txn_count = 0;
        txn.commit(function(err, results) {
                txn = neo4j.batch();
                sem.leave();
        });
      } else {
        sem.leave();
      }
      txn_count++;

      txn.save(user, function(err, savedUser) {
        if (err){
          logger.error("neo4j save %s %j", user.id_str, err);
          metrics.counter("neo4j_save_error").increment();
          reject({ err:err, reason:"neo4j save user error" });
          return;
        }
        logger.debug('inserted user %s', savedUser.id_str);
        txn.label(savedUser, "twitterUser", function(err, labeledUser) {
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
  });
  }
}
