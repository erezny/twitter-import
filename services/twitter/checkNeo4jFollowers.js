
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var assert = require('assert');
const crow = require("crow-metrics");
const request = require("request");

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("mongoToNeo4j.followers");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
  process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
  process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
    process.env.NEO4J_PROTOCOL,
    process.env.NEO4J_HOST,
    process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

var MongoClient = require('mongodb').MongoClient,
  test = require('assert');

var twitterCollection  = null;

var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var db = null;

//runtime loop
MongoClient.connect(util.format('mongodb://%s:%s@%s:%d/%s?authMechanism=SCRAM-SHA-1&authSource=%s',
  process.env.MONGO_USER,
  process.env.MONGO_PASSWD,
  process.env.MONGO_HOST,
  process.env.MONGO_PORT,
  process.env.MONGO_DATABASE,
  process.env.MONGO_DATABASE
),
  {
    logger: logger,
    numberOfRetries: 10,
    retryMiliSeconds: 10000
  },
  function(err, db_) {
    if (err){
      process.exit;
    }
  db = db_;

  var openQueries = 0;
  var queryLimit = 1;
  var total = 0;
  var finished = 0;

  metrics.setGauge( "openQueries", function () { return openQueries; });
  metrics.setGauge( "queryLimit", function () { return queryLimit; });
  metrics.setGauge( "total", function () { return total; });
  metrics.setGauge( "finished", function () { return finished; });

  logger.trace("connected to mongo");

  var cursor = db.collection("twitterUsers")
  .find({ 'followers.1': { $exists: true } })
  .project({
      id_str: 1,
      screen_name: 1,
      followers: 1,
      'identifiers.neo4jID': 1,
      followers_count: 1
  }).sort({ followers_count: 1 });

  var stream = cursor.stream();

  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
    setTimeout(function() {
      process.exit();
    }, 60 * 1000);
  });

  cursor.count(function(err, count) {
    logger.debug("Number of users %d", count);
    total = count;
  });

  stream.on('data', function(user) {
    openQueries++;
    if ( openQueries >= queryLimit) {
      stream.pause();
    }
    logger.trace('user from mongo: %s', user.screen_name);
    metrics.counter("users_loaded").increment();

    redis.hgetall(util.format("twitter:%s",user.id_str), function(err, obj) {
      if (obj && obj.neo4jID && typeof(obj.neo4jID) == 'string' && obj.neo4jID.match("[0-9]+")){
        updateFollowers({ id_str: user.id_str, screen_name: user.screen_name, neo4jID: obj.neo4jID, followers: user.followers })
          .then(function(results) {
            logger.trace("relationship Results: %j", results);
            db.collection("twitterUsers").findOneAndUpdate(
              { id_str: user.id_str },
              { $set: { followers: results, "identifiers.neo4j": obj.neo4jID },
                $inc: { 'import.neo4j.friends': 1 } },
              { projection: { id_str: 1, screen_name: 1 } } ).then(function(result) {
                logger.info("completed %s", user.screen_name);
                metrics.counter("users_saved").increment();
                restartQueries();
              }, function(err) {
                  logger.error("mongo error saving %s", user.screen_name);
                  restartQueries();
              });
          }, function(err) {
            logger.error("relationship error: %j", err);
            restartQueries();
          });
        } else {
          logger.debug("user not in neo4j yet %s", user.screen_name);
          metrics.counter("users_not_exist").increment();
          restartQueries();
        }
    });

  });

  function restartQueries(){
    finished++;
    openQueries--;
    metrics.counter("users_finished").increment();
    if (openQueries < queryLimit ) {
      stream.resume();
    }
    if (finished % 10 == 0){
      logger.info("completed %d / %d", finished, total);
    }
  }

});

//requires user.id_str, user.followers = []
function updateFollowers(user){
  assert( typeof(user.neo4jID) == "string" );
  assert( typeof(user.followers) == "object" );
  return new RSVP.Promise( function (resolve, reject) {
    var followersTasks = [];
    for (otherUser of user.followers){
      followersTasks.push(upsertFollowerIfExists(user, otherUser));
    }
    logger.debug("updateFollowers %s: %d", user.screen_name, followersTasks.length);
    RSVP.allSettled(followersTasks).then(function(results) {
      logger.debug("updateFollowers all settled %s", user.screen_name);
      var followersNotFound = [];
      for (result of results) {
        if (result.state == "rejected") {
          followersNotFound.push(result.reason);
        }
      }
      logger.debug("updateFollowers %s Remaining: %d", user.screen_name, followersNotFound.length);
      resolve(followersNotFound);
    })
  });
}

function upsertFollowerIfExists(user, follower){
  assert( typeof(user.neo4jID) == "string" );
  assert( typeof(user.id_str) == "string" );
  assert( typeof(follower) == "string" );
  return new RSVP.Promise( function (resolve, reject) {
    redis.hgetall(util.format("twitter:%s",follower), function(err, obj) {
      if (obj && obj.neo4jID && obj.neo4jID != "undefined"){
        upsertRelationship({ id: obj.neo4jID }, { id: user.neo4jID }).then(function(rel) {
          resolve(follower);
        }, function(err) {
          reject(follower);
        });
      } else {
        metrics.counter("rel_user_not_exist").increment();
        reject(follower);
      }
    });
  });
}

var sem = require('semaphore')(2);

function upsertRelationship(node, friend) {
  assert( typeof(node.id) == "string" );
  assert( typeof(friend.id) == "string" );
  return new RSVP.Promise( function (resolve, reject) {
    sem.take(function() {
      neo4j.query("start x=node({idx}), n=node({idn}) MATCH (x)-[r:follows]->(n) RETURN r",
        { idx: node.id, idn: friend.id }, function(err, rels) {
        if (err){
          logger.error("neo4j find error %j",err);
          metrics.counter("rel_find_error").increment();
          reject("error");
          sem.leave();
          return;
        }
        if (rels.length > 0) {
          //TODO search for duplicates and remove duplicates
          logger.trace("relationship found %j", rel);
          metrics.counter("rel_already_exists").increment();
          resolve(rel);
          sem.leave();
          return;
        }
        neo4j.relate(node.id, 'follows', friend.id, function(err, rel) {
          if (err){
            logger.error("neo4j save error %j %j", { node: node, friend: friend }, err);
            metrics.counter("rel_save_error").increment();
            reject("error");
            sem.leave();
            return;
          }
          logger.debug("saved relationship %j", rel);
          metrics.counter("rel_saved").increment();
          sem.leave();
          resolve(rel);
        });
      })
    });
  });
}
