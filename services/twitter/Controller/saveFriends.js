
var util = require('util');
var assert = require('assert');
const kueThreads = process.env.KUE_THREADS || 4;
const neo4jThreads = process.env.NEO4J_THREADS || 4;

const metrics = require('../../../lib/crow.js').withTags({
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

function saveFriend (job, done) {
  var startTime = process.hrtime();
  logger.trace("received job %j", job);
  var user = job.data.user;
  var friend = job.data.friend;
  var rel = job.data;
  metricStart.increment();

  function finished (done){
    metricFinish.increment();
    var diff = process.hrtime(startTime);
    metricKueTimer.add(diff[0] * 1e9 + diff[1]);
    logger.trace("finish")
    done();
  }

  upsertRelationship(user, friend).then(finished);

  var sem = require('semaphore')(neo4jThreads);
  function upsertRelationship(node, friend) {
    assert( typeof(node.id) == "number" );
    assert( typeof(friend.id) == "number" );
    return new RSVP.Promise( function (resolve, reject) {
    sem.take(function() {//timings
      var startNeo4jTime = process.hrtime();
      neo4j.queryRaw("start x=node({idx}), n=node({idn}) create unique (x)-[r:follows]->(n) RETURN r",
        { idx: node.id, idn: friend.id }, function(err, results) {
        sem.leave();
        var diff = process.hrtime(startNeo4jTime);
        metricKueTimer.add(diff[0] * 1e9 + diff[1]);
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

};

queue.process('saveFriend', kueThreads, saveFriend );
