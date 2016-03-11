
// #refactor:10 write queries
var util = require('util');
var T = require('../../../lib/twit.js');

var assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.users.api.show");
var queue = require('../../../lib/kue.js');

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 180) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

setInterval( function() {
  queue.inactiveCount( 'queryUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queue.inactive", total);
  });
}, 15 * 1000 );

queue.process('queryUser', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  metrics.counter("start").increment();

  checkUserQueryTime(job.data.user)
  .then(queryUser)
  .then(updateUserQueryTime)
  .then(function(user) {
    metrics.counter("finish").increment();
    done();
  }, function(err) {
    logger.debug("queryUser error %j: %j", job.data, err);
    metrics.counter("queryError").increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
    done(err);
  });
});

function checkUserQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hgetall(key, function(err, obj) {
      if ( obj & obj.queryTimestamp ){
        if ( obj.queryTimestamp > parseInt((+new Date) / 1000) - (60 * 60 * 24 * 7) ) {
            metrics.counter("repeatQuery").increment();
            reject( { message: "user recently queried" , timestamp:parseInt((+new Date) / 1000), queryTimestamp: obj.queryTimestamp } );
        } else {
          resolve(user);
        }
      } else {
        resolve(user);
      }
    });
  });
}

function updateUserQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hset(key, "queryTimestamp", parseInt((+new Date) / 1000), function() {
      resolve()
    });
  });
}

function queryUser(user) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryUser %s", user.id_str);
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('users/show', { user_id: user.id_str }, function(err, data)
      {
        if (err){
          //"message":"User has been suspended.","code":63,
          if (err.code == 50){
            //user doesn't exist
            //queue.create('expireUser', {user: user}).removeOnComplete(true).save();
            reject({user: user, err: err, reason: "user doesn't exist"});
            return;
          } else if (err.message == "User has been suspended."){
            queue.create('markUserSuspended', { user: user } ).removeOnComplete(true).save();
            resolve({ user: user, list: [] });
            return;
          } else {
            logger.error("twitter api error %j %j", user, err);
            metrics.counter("apiError").increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
            reject({ user: user, err: err, reason: "twitter api error" });
            return;
          }
        }
        logger.trace("Data %j", data);
        logger.debug("queryUser twitter api callback");
        var queriedUser = {
          id_str: data.id_str,
          screen_name: data.screen_name,
          name: data.name,
          followers_count: data.followers_count,
          friends_count: data.friends_count,
          favourites_count: data.favourites_count,
          description: data.description,
          location: data.location,
          statuses_count: data.statuses_count,
          protected: data.protected
        }
        queue.create('receiveUser', { user: queriedUser } ).removeOnComplete( true ).save();
        metrics.counter("queryFinished").increment();
        resolve(queriedUser);
      });
    });
  });
};
