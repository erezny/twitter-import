
var util = require('util');
var assert = require('assert');
const kueThreads = parseInt(process.env.KUE_THREADS) || 4;
const neo4jThreads = parseInt(process.env.NEO4J_THREADS) || 4;

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
var txn_count = 0;
var sem = require('semaphore')(1);
function upsertRelationship(node, friend) {
  assert( typeof(node.id) == "number" );
  assert( typeof(friend.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
    sem.take(function() {//timings

      var startNeo4jTime = process.hrtime();
      logger.trace("query Neo4j %j", [ node, friend ] );

      if (txn_count > kueThreads / 2){
        txn.commit();
        txn_count = 0;
      }
      txn_count += 1;

      txn.queryRaw("start x=node({idx}), n=node({idn}) create unique (x)-[r:follows]->(n) RETURN r",
        { idx: node.id, idn: friend.id }, function(err, results) {

        sem.leave();
        var diff = process.hrtime(startNeo4jTime);
        metricNeo4jTimer.add(diff[0] * 1e9 + diff[1]);

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

  function finished (){
    return new RSVP.Promise( function (resolve, reject) {
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
