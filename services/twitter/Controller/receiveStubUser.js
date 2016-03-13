
var util = require('util');
var assert = require('assert');
const kueThreads = parseInt(process.env.KUE_THREADS) || 500;
const neo4jThreads = parseInt(process.env.NEO4J_THREADS) || 100;
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "stubUser",
  mvc: "controller",
  function: "save",
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
const redisKey = function(user) { return util.format("twitter:%s", user.id_str); };

setInterval( function() {
  queue.inactiveCount( 'receiveStubUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
}, 15 * 1000 );

const metricRelExists = metrics.counter("rel_exists");
const metricExists = metrics.counter("exists");
const metricFinish = metrics.counter("finish");
const metricStart = metrics.counter("start");
const metricError = metrics.counter("error");
const metricSaved = metrics.counter("saved");

var txn = neo4j.batch();
setInterval(function() {
    txn.commit();
    txn = neo4j.batch();
} , 5 * 1000);

function upsertStubUserToNeo4j(user, rel) {
  return new RSVP.Promise( function (resolve, reject) {
    var savedUser = txn.save(user, function(err, savedUser) {
      if (err){
        metricExists.increment();
        reject({ err:err, reason:"neo4j save user error" });
      }
    });
    txn.label(savedUser, "twitterUser", function(err, labeledUser) {
      if (err){
        metricError.increment();
        reject({ err:err, reason:"neo4j label user error" });
      } else {
        redis.hset(redisKey(savedUser), "neo4jID", savedUser.id, function(err, res) {
          metricSaved.increment();
          resolve(savedUser);
        });
      }
    });
  });
}

function redisUserCheck(user){
  return new RSVP.Promise( function(resolve, reject) {
    redis.EXISTS(redisKey(user), function(err, results) {
      if (results >= 1){
        metricExists.increment();
        resolve(user);
      } else {
        reject(user);
      }
    });
  });
}

function saveStubUser(job, done) {
  logger.trace("received job %j", job);
  metricStart.increment();
  var user = job.data.user;
  var rel = job.data.rel;

  function finished (result){
    return new RSVP.Promise( function (resolve, reject) {
      metricFinish.increment();
      logger.trace("finish")
      queue.create('receiveFriend', rel ).removeOnComplete( true ).save();
      resolve();
    });
  }

  redisUserCheck(user)
  .then(finished,upsertStubUserToNeo4j)
  .then(finished, done)
  .then(done);
};

queue.process('receiveStubUser', kueThreads, saveStubUser);
