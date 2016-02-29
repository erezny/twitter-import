
// #refactor:10 write queries
var util = require('util');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.users.controller");
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

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / process.env.NEO4j_LIMIT_NODE_TXPS ) * 60 * 1000);

var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");
function receiveUser(job, done) {
  logger.trace("received job %j", job);
  metrics.counter("processStarted").increment();
  var user = {
    id_str: job.data.user.id_str,
    screen_name: job.data.user.screen_name,
    name: job.data.user.name,
    followers_count: job.data.user.followers_count,
    friends_count: job.data.user.friends_count,
    favourites_count: job.data.user.favourites_count,
    description: job.data.user.description,
    location: job.data.user.location,
    statuses_count: job.data.user.statuses_count,
    protected: job.data.user.protected
  }

  //    var mongo = saveUserToMongo(user);

  limiter.removeTokens(1, function(err, remainingRequests) {
    var key = util.format("twitter:%s", user.id_str);
    redis.hgetall(key, function(err, redisUser) {
      if (redisUser && redisUser.neo4jID && redisUser.neo4jID != "undefined"){
        redis.hget(key, "saveTimestamp", function(err, result) {
          if (result < parseInt((+new Date) / 1000) - 24 * 60 * 60) {
            dostuff(user, done);
          } else {
            done();
          }
        });
      } else {
        dostuff(user, done);
      }
    });
  });
};

var metricsFinished = metrics.counter("processFinished");
var metricsError = metrics.counter("processError");
function dostuff(user, done){
  return metricNeo4jTimeMsec.time(upsertUserToNeo4j(user))
  .then(updateUserSaveTime)
  .then(function(savedUser) {
    logger.trace("savedUser: %j", savedUser);
      //queue.create('queryUserFriends', { user: { id_str: user.id_str } } ).removeOnComplete( true ).save();
      //queue.create('queryUserFollowers', { user: { id_str: user.id_str } } ).removeOnComplete( true ).save();
      metricsFinished.increment();
    done();
  }, function(err) {
    logger.error("receiveUser error on %j\n%j\n--", job, err);
    metricsError.increment();
    done(err);
  });
}

queue.process('receiveUser', 5, receiveUser);

  setInterval( function() {
  queue.inactiveCount( 'receiveUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
  }, 15 * 1000 );

});

function updateUserSaveTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hset(key, "saveTimestamp", parseInt((+new Date) / 1000), function() {
      resolve(user)
    });
  });
}

function upsertUserToNeo4j(user) {
  return function() {
  delete user.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.debug('saving user %s', user.screen_name);
    neo4j.save(user, function(err, savedUser) {
      if (err){
        logger.error("neo4j save %s %j", user.screen_name, err);
        metrics.counter("neo4j_save_error").increment();
        reject({ err:err, reason:"neo4j save user error" });
        return;
      }
      logger.debug('inserted user %s', savedUser.screen_name);
      neo4j.label(savedUser, "twitterUser", function(err, labeledUser) {
        if (err){
          logger.error("neo4j label error %s %j", user.screen_name, err);
          metrics.counter("neo4j_label_error").increment();
          reject({ err:err, reason:"neo4j label user error" });
          return;
        }
        redis.hset(util.format("twitter:%s",savedUser.id_str), "neo4jID", savedUser.id, function(err, res) {
          logger.debug('labeled user %s', savedUser.screen_name);
          metrics.counter("neo4j_inserted").increment();
          resolve(savedUser);
        });
      });
    });
  });
  }
}
