
// #refactor:10 write queries
var util = require('util');

var assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.vip.controller");
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
  queue.inactiveCount( 'addVIP', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
  }, 15 * 1000 );

function receiveVIP (job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  var user = job.data.user;
  metrics.counter("start").increment();

  lookupNeo4jID(user)
  .then(function(results) {
    saveVIP(results.user).then(function() {
      metricFinish.increment();
      done();
    }, done);
  }, function(err) {
    logger.debug("neo4j user not in redis %j",err);
    metricUserNotExist.increment();
    done(); //avoid retries
  });
};

function lookupNeo4jID(user){

  return new RSVP.Promise( function(resolve, reject) {
    logger.trace("querying redis");
    redis.hgetall(util.format("twitter:%s", user.id_str), function(err, redisUser) {
      logger.trace("finished querying redis");
      if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
    //    redisCache.push([ user.id_str, { id: parseInt(redisUser.neo4jID) }, 0 ])
        resolve({ id: parseInt(redisUser.neo4jID) });
      } else {
        logger.trace("reject");
        reject();
      }
    });
  });
}

queue.process('receiveVIP', 10, receiveVIP );

var metricRelFindError = metrics.counter("rel_find_error");
var metricRelAlreadyExists = metrics.counter("rel_already_exists");
var metricRelSaveError = metrics.counter("rel_save_error");
var metricRelSaved = metrics.counter("rel_saved");
var sem = require('semaphore')(2);
function saveVIP(node) {
  assert( typeof(node.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
  sem.take(function() {
    neo4j.queryRaw("start x=node({idx}) create unique (x)-[r:follows]->(n:service{type:"VIP"}) RETURN r",
      { idx: node.id, idn: friend.id }, function(err, results) {
      sem.leave();
      if (err){
        logger.error("neo4j save error %j %j", { node: node }, err);
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
