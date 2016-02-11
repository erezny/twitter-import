
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');
var assert = require('assert');

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

var influx = require('influx')(
  {
    host: process.env.INFLUX_HOST,
    port: parseInt(process.env.INFLUX_PORT),
    protocol: process.env.INFLUX_PROTOCOL,
    username: process.env.INFLUX_USERNAME,
    password: process.env.INFLUX_PASSWORD,
    database: process.env.INFLUX_DATABASE
  }
);

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

  logger.trace("connected to mongo");

  var cursor = db.collection("twitterUsers")
  .find({ 'friends.1': { $exists: true } })
  .project({
      id_str: 1,
      screen_name: 1,
      friends: 1,
  })

  var stream = cursor.stream();

  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
    setTimeout(function() {
      process.exit;
    }, 60 * 1000);
  });

  var openQueries = 0;
  var queryLimit = 1;
  var total = 0;
  var finished = 0;

  cursor.count(function(err, count) {
    logger.info("Number of users %d", count);
    total = count;
  })

  stream.on('data', function(user) {
    openQueries++;
    if ( openQueries >= queryLimit) {
      stream.pause();
    }
    logger.trace('user from mongo: %s', user.screen_name);

    redis.hgetall(util.format("twitter:%s",user.id_str), function(err, obj) {
        if (obj && obj.neo4jID){
          updateFriends({ id_str: user.id_str, screen_name: user.screen_name, neo4jID: obj.neo4jID, friends: user.friends })
          .then(function(results) {
            logger.trace("relationship Results: %j", results);
            db.collection("twitterUsers").findOneAndUpdate(
              { id_str: user.id_str },
              { $set: { friends: results } },
              { projection: { id_str: 1, screen_name: 1 } } ).then(function(result) {
                logger.info("completed %j", result);
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
          restartQueries();
        }
    });

  });

  function restartQueries(){
    finished++;
    openQueries--;
    if (openQueries < queryLimit ) {
      stream.resume();
    }
    if (finished % 10 == 0){
      logger.info("completed %d / %d", finished, total);
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
      if (obj && obj.neo4jID){
        upsertRelationship({ id: user.neo4jID }, { id: obj.neo4jID }).then(function(rel) {
          resolve(friend);
        }, function(err) {
          reject(friend);
        });
      } else {
        reject(friend);
      }
    });
  });
}

var sem = require('semaphore')(1);

function upsertRelationship(node, friend) {
  assert( typeof(node.id) == "string" );
  assert( typeof(friend.id) == "string" );
  return new RSVP.Promise( function (resolve, reject) {
    sem.take(function() {
    neo4j.relationships(node.id, 'out', 'follows', function(err, rels) {
      if (err){
        logger.error("%j",err);
        reject("error");
        sem.leave();
        return;
      }
      if (rels.length > 0) {
        for (rel of rels){
          if (rel.end == friend.id){
            logger.trace("relationship found %j", rel);
            resolve(rel);
            sem.leave();
            return;
          }
        }
      }
      neo4j.relate(node.id, 'follows', friend.id, function(err, rel) {
        if (err){
          logger.error("neo4j save error %j",err);
          reject("error");
          sem.leave();
          return;
        }
        logger.debug("saved relationship %j", rel);
        resolve(rel);
        sem.leave();
      });
    })
  });
  });
}
