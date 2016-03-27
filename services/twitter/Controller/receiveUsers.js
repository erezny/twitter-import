
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
var _ = require('../../../lib/util.js');
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
const metricTxnFinished = metrics.counter("txnFinished");

var txn = neo4j.batch();
setInterval(function() {
    var txn_cmt = txn;
    txn = neo4j.batch();
    txn_cmt.commit(function (err, results) {
      metricTxnFinished.increment();
    });
} , 5 * 1000);

const user_cypher = "merge (x:twitterUser { id_str: {user}.id_str }) " +
            "set x.screen_name = {user}.screen_name, " +
            " x.name = {user}.name, " +
            " x.followers_count = {user}.followers_count, " +
            " x.friends_count = {user}.friends_count, " +
            " x.favourites_count = {user}.favourites_count, " +
            " x.description = {user}.description, " +
            " x.location = {user}.location, " +
            " x.statuses_count = {user}.statuses_count, " +
            " x.protected = {user}.protected " ;

function upsertUserToNeo4j(user) {
  return new RSVP.Promise( function (resolve, reject) {
    var savedUser = txn.query(user_cypher, user, function(err, savedUser) {
      if (!_.isEmpty(err)){
        metricsError.increment();
        reject({ err:err, reason:"neo4j save user error" });
      } else {
        redis.hset(redisKey(savedUser), "neo4jID", savedUser.id, function(err, res) {
          metricSaved.increment();
          resolve(savedUser);
        });
      }
    });
  });
}

function lookupNeo4jID(user){
  return new RSVP.Promise( function(resolve, reject) {
    redis.hgetall(redisKey(user), function(err, redisUser) {
      if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
        user.id = parseInt(redisUser.neo4jID)
        resolve(user);
      } else {
        resolve(user);
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
  var user = model.filterUser(job.data.user);

  function finished (result){
    return new RSVP.Promise( function (resolve, reject) {
      metricFinish.increment();
      logger.trace("finish")
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
