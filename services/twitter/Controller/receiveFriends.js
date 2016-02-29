
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.friends.controller");
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
    upsertRelationship(results.user, results.friend).then(function() {
      metricFinish.increment();
      done();
    }, done);
  }, function(err) {
    queue.create('receiveFriend', { user: user, friend: friend } ).removeOnComplete( true ).save();
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
        queue.create("queryUser", { user: user } ).removeOnComplete(true).save();
        logger.trace("save failed");
        reject(err);
      }
    });
  });
}

queue.process('receiveFriend', 40, receiveFriend );

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

var userSem = require('semaphore')(2);
function upsertStubUserToNeo4j(user) {
  delete user.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('Checking stub user %s', user.id_str);
    userSem.take(function() {
      neo4j.find( { id_str: user.id_str }, false, "twitterUser" ,
          function(err, results) {
        if (err){
          userSem.leave();
          logger.error("neo4j find %s %j",user.screen_name, err);
          metrics.counter("neo4j_find_error").increment();
          reject({ err:err, reason:"neo4j find user error" });
          return;
        }
        if (results.length > 0){
          userSem.leave();
          logger.debug("found %j", results);
          //if results.length > 1, flag as error
          redis.hset(util.format("twitter:%s",user.id_str), "neo4jID", results[0].id, function(err, res) { });
          metrics.counter("neo4j_exists").increment();
          resolve(user);
          return;
        }
        logger.debug('stubbing user %s', user.id_str);
        neo4j.save(user, function(err, savedUser) {
          if (err){
            userSem.leave();
            logger.error("neo4j save %s %j", user.id_str, err);
            metrics.counter("neo4j_save_error").increment();
            reject({ err:err, reason:"neo4j save user error" });
            return;
          }
          logger.debug('inserted user %s', savedUser.id_str);
          neo4j.label(savedUser, "twitterUser", function(err, labeledUser) {
            userSem.leave();
            if (err){
              logger.error("neo4j label error %s %j", user.id_str, err);
              metrics.counter("neo4j_label_error").increment();
              reject({ err:err, reason:"neo4j label user error" });
              return;
            }
            redis.hset(util.format("twitter:%s",savedUser.id_str), "neo4jID", savedUser.id, function(err, res) {
              logger.debug('labeled user %s', savedUser.id_str);
              metrics.counter("neo4j_inserted").increment();
              resolve(savedUser);
            });

          });
        });
      });
    });
  })
}
