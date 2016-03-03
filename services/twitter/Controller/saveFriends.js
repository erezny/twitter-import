
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.friends.controller.save");
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

var metricFinish = metrics.counter("finish");
var metricStart = metrics.counter("start");

function saveFriend (job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  var user = job.data.user;
  var friend = job.data.friend;
  var rel = job.data;
  metricStart.increment();
  upsertRelationship(user, friend).then(function() {
    metricFinish.increment();
    done();
  }, done);
};

queue.process('saveFriend', 4, saveFriend );

var metricRelFindError = metrics.counter("rel_find_error");
var metricRelAlreadyExists = metrics.counter("rel_already_exists");
var metricRelSaved = metrics.counter("rel_saved");
var sem = require('semaphore')(2);
function upsertRelationship(node, friend) {
  assert( typeof(node.id) == "number" );
  assert( typeof(friend.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
  sem.take(function() {
    neo4j.queryRaw("start x=node({idx}), n=node({idn}) create unique (x)-[r:follows]->(n) RETURN r",
      { idx: node.id, idn: friend.id }, function(err, results) {
      sem.leave();
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
