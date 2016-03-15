
"use strict";
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

function count_queue(){
  queue.inactiveCount( 'receiveFriend', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
};
setInterval( count_queue, 15 * 1000 );

const metricRelExists = metrics.counter("rel_exists");
const metricUserNotExist = metrics.counter("user_not_exist");
const metricFinish = metrics.counter("finish");
const metricStart = metrics.counter("start");
const metricError = metrics.counter("error");
const metricTxnFinished = metrics.counter("txnFinished");
const metricRelSaved = metrics.counter("rel_saved");
const metricRelError = metrics.counter("rel_error");

function receiveFriend (job, done) {
  logger.trace("received job %j", job);
  metricStart.increment();
  var rel = job.data;

  function finished (result){
      metricFinish.increment();
      logger.trace("finish");
      done();
  }

  saveFriend(rel)
  .then(finished, finished)

};

queue.process('receiveFriend', 500, receiveFriend );

var txn = neo4j.batch();
setInterval(function() {
  var txn_commit = txn;
  txn = neo4j.batch();
  txn_commit.commit(function (err, results) {
    metricTxnFinished.increment();
  });
} , 30 * 1000);

const cypher = "merge (x:twitterUser { id_str: {user} }) " +
            "merge (y:twitterUser { id_str: {friend} }) " +
            "merge (x)-[r:follows]-(y) ";

function saveFriend(rel) {
  return new Promise(function(resolve, reject) {
    var user = rel.user;
    var friend = rel.friend;

    txn.query(cypher, { user: user.id_str, friend: friend.id_str } , function(err, results) {
      if (err){
        metricRelError.increment();
        reject();
      } else {
        metricRelSaved.increment();
        resolve();
      }
    });

  });
}
