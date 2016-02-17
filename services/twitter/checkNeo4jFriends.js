
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

const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("mongoToNeo4j.friends");

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
  .find({ 'friends.1': { $exists: true } })
  .project({
      id_str: 1,
      screen_name: 1,
      friends: 1,
  });

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
      if ((obj && obj.neo4jID) || user.identifiers.neo4j){
        var neo4jID;
        if (obj && obj.neo4jID){
          neo4jID = obj.neo4jID;
        } else {
          neo4jID = user.identifiers.neo4j;
        }
          updateFriends({ id_str: user.id_str, screen_name: user.screen_name, neo4jID: neo4jID, friends: user.friends })
          .then(function(results) {
            logger.trace("relationship Results: %j", results);
            db.collection("twitterUsers").findOneAndUpdate(
              { id_str: user.id_str },
              { $set: { friends: results, "identifiers.neo4j": neo4jID },
                $inc: { 'import.neo4j.friends': 1 } },
              { projection: { id_str: 1, screen_name: 1 } } ).then(function(result) {
                logger.info("completed %s", user.screen_name);
                metrics.counter("users_saved").increment();
                restartQueries();
              }, function(err) {
                  logger.err("mongo error saving %s", user.screen_name);
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
      logger.debug("completed %d / %d", finished, total);
    }
  }

});

//requires user.id_str, user.friends = []
function updateFriends(user){
  assert( typeof(user.neo4jID) == "string" );
  assert( typeof(user.friends) == "object" );
  return new RSVP.Promise( function (resolve, reject) {
    var friendsTasks = [];
    for (otherUser of user.friends){
      friendsTasks.push(upsertFriendIfExists(user, otherUser));
    }
    logger.info("updateFriends %s: %d", user.screen_name, friendsTasks.length);
    RSVP.allSettled(friendsTasks).then(function(results) {
      logger.debug("updateFriends all settled %s", user.screen_name);
      var friendsNotFound = [];
      for (result of results) {
        if (result.state == "rejected") {
          friendsNotFound.push(result.reason);
        }
      }
      logger.info("updateFriends %s Remaining: %d", user.screen_name, friendsNotFound.length);
      resolve(friendsNotFound);
    })
  });
}

function upsertFriendIfExists(user, friend){
  assert( typeof(user.neo4jID) == "string" );
  assert( typeof(user.id_str) == "string" );
  assert( typeof(friend) == "string" );
  return new RSVP.Promise( function (resolve, reject) {
    redis.hgetall(util.format("twitter:%s",friend), function(err, obj) {
      if (obj && obj.neo4jID && obj.neo4jID != "undefined"){
        upsertRelationship({ id: user.neo4jID }, { id: obj.neo4jID }).then(function(rel) {
          resolve(friend);
        }, function(err) {
          reject(friend);
        });
      } else {
        metrics.counter("rel_user_not_exist").increment();
        reject(friend);
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
          for (rel of rels){
            if (rel.end == friend.id){
              //TODO search for duplicates and remove duplicates
              logger.trace("relationship found %j", rel);
              metrics.counter("rel_already_exists").increment();
              resolve(rel);
              sem.leave();
              return;
            }
          }
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
          resolve(rel);
          sem.leave();
        });
      })
    });
  });
}
