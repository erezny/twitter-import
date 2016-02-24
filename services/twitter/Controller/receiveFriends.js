
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

const crow = require("crow-metrics");
const request = require("request");
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.friends.controller");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
var limiterMembers = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var BloomFilter = require("bloomfilter").BloomFilter;

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

  // n = 10,000,000, p = 1.0E-10 (1 in 10,000,000,000) → m = 479,252,919 (57.13MB), k = 33
  var relationshipBloom = new BloomFilter(
    8 * 1024 * 1024 * 60, // MB
    33        // number of hash functions.
  );

queue.process('receiveFriend', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  var user = job.data.user;
  var friend = job.data.friend;
  metrics.counter("start").increment();
  var rel_id = util.format("%s:%s", user.id_str, friend.id_str );
  if (relationshipBloom.test(rel_id)) {
    metrics.counter("rel_in_bloomfilter").increment();
    done();
  } else {
    relationshipBloom.add(rel_id);
  }

  redis.hgetall(util.format("twitter:%s", user.id_str), function(err, redisUser) {
    if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
      redis.hgetall(util.format("twitter:%s", friend.id_str), function(err, redisFriend) {
        if (redisFriend && redisFriend.neo4jID && redisFriend.neo4jID != "undefined"){
          upsertRelationship({ id: parseInt(redisUser.neo4jID) }, { id: parseInt(redisFriend.neo4jID) }).then(function() {
            metrics.counter("finish").increment();
            done();
          }, done);
        } else {
          logger.debug("friend not in redis %j",err);
          metrics.counter("friend_not_exist").increment();
          done({ message: "friend not in redis" } );
        }
      });
    } else {
      metrics.counter("user_not_exist").increment();
      logger.debug("neo4j user not in redis %j",err);
      done({ message: "user not in redis" });
    }
  });
});

function upsertRelationship(node, friend) {
  assert( typeof(node.id) == "number" );
  assert( typeof(friend.id) == "number" );
  return new RSVP.Promise( function (resolve, reject) {
      neo4j.queryRaw("start x=node({idx}), n=node({idn}) MATCH (x)-[r:follows]->(n) RETURN r",
        { idx: node.id, idn: friend.id }, function(err, results) {
        if (err){
          logger.error("neo4j find error %j",err);
          metrics.counter("rel_find_error").increment();
          reject("error");
          return;
        }
        logger.trace("neo4j found %j", results);
        if (results.data.length > 0) {
          //TODO search for duplicates and remove duplicates
          logger.debug("relationship found %j", results.data[0][0].metadata.id);
          metrics.counter("rel_already_exists").increment();
          if (results.data.length > 1){
            for (var i = 1; i < results.data.length; i++){
              neo4j.rel.delete(results.data[i][0].metadata.id, function(err) {
                if (!err) logger.debug("deleted duplicate relationship");
              });
            }
          }
          resolve();
          return;
        }
        neo4j.relate(node.id, 'follows', friend.id, function(err, rel) {
          if (err){
            logger.error("neo4j save error %j %j", { node: node, friend: friend }, err);
            metrics.counter("rel_save_error").increment();
            reject("error");
            return;
          }
          logger.debug("saved relationship %j", rel);
          metrics.counter("rel_saved").increment();
          resolve();
        });
      });
  });
}
