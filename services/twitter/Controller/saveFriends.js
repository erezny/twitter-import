
var util = require('util');
var assert = require('assert');
const kueThreads = parseInt(process.env.KUE_THREADS) || 500;
const neo4jThreads = parseInt(process.env.NEO4J_THREADS) || 100;

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friends",
  mvc: "controller",
  function: "save",
  kue: "saveFriend",
  kueThreads: kueThreads,
  neo4jThreads: neo4jThreads,
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

setInterval( function() {
  queue.inactiveCount( 'saveFriend', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
  }, 15 * 1000 );

const metricFinish = metrics.counter("finish");
const metricStart = metrics.counter("start");
const metricKueTimer = metrics.distribution("kueue_ms");
const metricNeo4jTimer = metrics.distribution("neo4j_ms");
const metricRelFindError = metrics.counter("rel_find_error");
const metricRelAlreadyExists = metrics.counter("rel_already_exists");
const metricRelSaved = metrics.counter("rel_saved");

var txn = neo4j.batch();

setInterval(function() {
    txn.commit();
} , 5 * 1000);

function upsertRelationship(node, friend) {
  assert( typeof(node.id) == "number" );
  assert( typeof(friend.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
      txn_count.inc(function() {
        txn.relate( node.id, "follows", friend.id , function(err, results) {
          if (err){
            if (err.code == "Neo.ClientError.Statement.ConstraintViolation") {
              metricRelAlreadyExists.increment();
              resolve();
            } else {
              logger.error("neo4j save error %j %j", { node: node, friend: friend }, err);
              reject("error");
            }
          } else {
            logger.debug("saved relationship %j", results);

            metricRelSaved.increment();
            resolve();
          }
        });
      });
    });
}

function saveFriend (job, done) {

  var startTime = process.hrtime();
  logger.trace("received job %j", job);

  var user = job.data.user;
  var friend = job.data.friend;
  var rel = job.data;
  metricStart.increment();

  function finished (result){
    return new RSVP.Promise( function (resolve, reject) {
      txn_count.dec();
      metricFinish.increment();
      var diff = process.hrtime(startTime);
      metricKueTimer.add(diff[0] * 1e9 + diff[1]);
      logger.trace("finish")
      resolve();
    });
  }

  upsertRelationship(user, friend).then(finished, done).then(done);
};

queue.process('saveFriend', kueThreads, saveFriend );
