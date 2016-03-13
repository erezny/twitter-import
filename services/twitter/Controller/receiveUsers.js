
var util = require('util');
var assert = require('assert');
const kueThreads = parseInt(process.env.KUE_THREADS) || 500;
const neo4jThreads = parseInt(process.env.NEO4J_THREADS) || 100;
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "user",
  mvc: "controller",
  function: "save",
  kue: "saveUser",
});
var queue = require('../../../lib/kue.js');
var neo4j = require('../../../lib/neo4j.js');
var model = require('../../../lib/twitter/models/user.js');
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
queue.inactiveCount( 'saveUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

const metricFinish = metrics.counter("finish");
const metricStart = metrics.counter("start");
const metricsError = metrics.counter("error");
const metricSaved = metrics.counter("saved");

var txn = neo4j.batch();
setInterval(function() {
    txn.commit();
    txn = neo4j.batch();
} , 5 * 1000);

function upsertUserToNeo4j(user) {
  return new RSVP.Promise( function (resolve, reject) {
    var savedUser = txn.save(user, function(err, savedUser) {
      if (err){
        metricsError.increment();
        reject({ err:err, reason:"neo4j save user error" });
      }
    });
    txn.label(savedUser, "twitterUser", function(err, labeledUser) {
      if (err){
        metricsError.increment();
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

function updateUserSaveTime(user){
  return new Promise(function(resolve, reject) {
    redis.hset(redisKey(user), "saveTimestamp", parseInt((+new Date) / 1000), function() {
      resolve(user)
    });
  });
}

function saveUser(job, done) {
  logger.trace("received job %j", job);
  metricStart.increment();
  var user = filterUser(job.data.user);
  var rel = job.data.rel;

  function finished (result){
    return new RSVP.Promise( function (resolve, reject) {
      metricFinish.increment();
      logger.trace("finish")
      queue.create('receiveFriend', rel ).removeOnComplete( true ).save();
      resolve();
    });
  }
  upsertUserToNeo4j(user)
  .then(updateUserSaveTime, done)
  .then(finished, done)
  .then(done);
};

queue.process('saveUser', kueThreads, saveUser);
queue.process('receiveUser', kueThreads, saveUser);
