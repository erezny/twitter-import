
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

var db = null;
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
  logger.info("connected to database");

  function saveUserToMongo(user) {
    logger.debug("save user to mongo %s", user.screen_name);
    return db.collection("twitterUsers").update(
      { 'id_str': user.id_str },
      { $set: user ,
      $currentDate: { 'timestamps.updated': { $type: "timestamp" } }
    },
    { upsert: true });
  };

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

    if ( !user.screen_name) {
      done({ reason: "incomplete user data" });
      return;
    }
    logger.debug("receivedUser %s", user.screen_name);

    //    var mongo = saveUserToMongo(user);
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
  return function(){
  delete user.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s %s', user.screen_name, user.id_str);
    neo4j.find( { id_str: user.id_str }, false, "twitterUser" ,
        function(err, results) {
      if (err){
        logger.error("neo4j find %s %j",user.screen_name, err);
        metrics.counter("neo4j_find_error").increment();
        reject({ err:err, reason:"neo4j find user error" });
        return;
      }

      if (results.length > 0){
        logger.debug("found %j", results);
        //if results.length > 1, flag as error
        redis.hset(util.format("twitter:%s",user.id_str), "neo4jID", results[0].id, function(err, res) { });
        metrics.counter("neo4j_exists").increment();
        resolve(user);
        return;
      }

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
  });
  }
}
