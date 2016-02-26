
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

const metrics = require('../../../lib/crow.js').withPrefix("twitter.followers.api.list"); //turn lib into node module
var queue = require('../../../lib/kue.js');

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 29) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

setInterval( function() {
queue.inactiveCount( 'queryFollowersList', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

var metricNeo4jTimeMsec = metrics.distribution("neo4j_time_msec");
queue.process('queryFollowersList', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryFollowersList received job %j", job);
  metrics.counter("start").increment();
  var user = job.data.user;
  var cursor = job.data.cursor || "-1";
  queryFollowersList(user, cursor)
  .then(function(list) {
    metrics.counter("finish").increment();
    done();
  }, function(err) {
    logger.error("queryFollowersList error: %j %j", job, err);
    metrics.counter("queryError").increment();
    if (err.message == "Not authorized."){
      done();
    } else {
      done(err);
    }
  });
});

function queryFollowersList(user, cursor) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryFollowersList");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('followers/list', { user_id: user.id_str, cursor: cursor, count: 200 }, function (err, data)
      {
        if (err){
          logger.error("twitter api error %j %j", user, err);
          metrics.counter("apiError").increment();
          reject(err);
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryFollowersList twitter api callback");
        logger.info("queryFollowersList %s found %d followers", user.screen_name, data.users.length);
        for (follower of data.users){
          queue.create('receiveUser', { user: follower } ).removeOnComplete(true).save();
          queue.create('receiveFriend', { user: { id_str: follower.id_str }, friend: { id_str: user.id_str } } ).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          queue.create('queryFollowersList', { user: user, cursor: data.next_cursor_str }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("apiFinished").increment();
        resolve(data.users);
      });
    });
  });
};
