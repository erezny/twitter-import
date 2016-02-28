
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.lists.controller.members");
var queue = require('../../../lib/kue.js');

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

setInterval( function() {
queue.inactiveCount( 'receiveListMembers', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

queue.process('receiveListMembers', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  metrics.counter("start").increment();

  redis.hgetall(util.format("twitter:%s", job.data.user.id_str), function(err, member) {
    if (member && member.neo4jID && member.neo4jID != "undefined"){

      redis.hgetall(util.format("twitterList:%s",job.data.list.id_str), function(err, list) {
        if (list && list.neo4jID && list.neo4jID != "undefined"){
          setListMember({ id: parseInt(member.neo4jID) }, { id: parseInt(list.neo4jID) }).then(done);
        } else {
          metrics.counter("members.rel_list_not_exist").increment();
          logger.error("neo4j list not in redis %j",err);
          done();
        }
      });
    } else {
      queue.create('queryUser', { user: job.data.user } ).attempts(5).removeOnComplete( true ).save();
      metrics.counter("members.rel_user_not_exist").increment();
      logger.debug("neo4j user not in redis %j",err);
      done();
    }
  });
});

var metricRelFindError = metrics.counter("rel_find_error");
var metricRelAlreadyExists = metrics.counter("rel_already_exists");
var metricRelSaveError = metrics.counter("rel_save_error");
var metricRelSaved = metrics.counter("rel_saved");
var sem = require('semaphore')(2);
function setListMember(node, list) {
  assert( typeof(node.id) == "number" );
  assert( typeof(list.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
  sem.take(function() {
    neo4j.queryRaw("start x=node({idx}), n=node({idn}) create unique (x)-[r:includes]->(n) RETURN r",
      { idx: node.id, idn: list.id }, function(err, results) {
      sem.leave();
      if (err){
        logger.error("neo4j save error %j %j", { node: node, list: list }, err);
        metricRelAlreadyExists.increment();
        reject("error");
        return;
      }
      logger.debug("saved relationship %j", results);
      metricRelSaved.increment();
      resolve();
    });
  });
});
}
