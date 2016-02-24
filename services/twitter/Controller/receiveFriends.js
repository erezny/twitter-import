
// #refactor:10 write queries
var util = require('util');
var Twit = require('twit');
var T = new Twit({
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
  //access_token:         process.env.TWITTER_ACCESS_TOKEN,
  //access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
  app_only_auth:        true
});

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.friends.controller");

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
var limiterMembers = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

process.once( 'SIGTERM', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
});

process.once( 'SIGINT', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
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
  queue.inactiveCount( 'receiveFriend', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
  }, 15 * 1000 );

var metricRelInBloomfilter = metrics.counter("rel_in_bloomfilter");
var metricFriendNotExist = metrics.counter("friend_not_exist");
var metricUserNotExist = metrics.counter("user_not_exist");
var metricFinish = metrics.counter("finish");
var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");

function receiveFriend (job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  var user = job.data.user;
  var friend = job.data.friend;
  metrics.counter("start").increment();

  RSVP.hash({ user: lookupNeo4jID(user), friend: lookupNeo4jID(friend) })
  .then(function(results) {
    logger.trace("ready to upsert");
    metricNeo4jTimeMsec.time(upsertRelationship(results.user, results.friend)).then(function() {
      metricFinish.increment();
      done();
    }, done);
  }, function(err) {
    logger.debug("neo4j user not in redis %j",err);
    metricUserNotExist.increment();
    done(); //avoid retries
  });
};

//var redisCache = [];
function lookupNeo4jID(user){

  return new RSVP.Promise( function(resolve, reject) {
    // for (cache in redisCache){
    //   if (cache[0] == user.id_str){
    //     logger.trace("cached");
    //     resolve(cache[1]);
    //     cache[2]++;
    //     return;
    //   }
    // }
    // if (redisCache.length > 100){
    //   redisCache.sort(function(a,b) {return a[2] - b[2] }).splice(80);
    // }
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

queue.process('receiveFriend', 10, receiveFriend );

var metricRelFindError = metrics.counter("rel_find_error");
var metricRelAlreadyExists = metrics.counter("rel_already_exists");
var metricRelSaveError = metrics.counter("rel_save_error");
var metricRelSaved = metrics.counter("rel_saved");
var sem = require('semaphore')(2);
function upsertRelationship(node, friend) {
  return function() {
  assert( typeof(node.id) == "number" );
  assert( typeof(friend.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
    sem.take( function() {
    neo4j.queryRaw("start x=node({idx}), n=node({idn}) create unique (x)-[r:follows]->(n) RETURN r",
      { idx: node.id, idn: friend.id }, function(err, results) {
        sem.leave();
        if (err){
          logger.error("neo4j save error %j %j", { node: node, friend: friend }, err);
          metricRelSaveError.increment();
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
}
