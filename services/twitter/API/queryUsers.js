
// #refactor:10 write queries
var util = require('util');
var Twit = require('twit');
var T = new Twit({
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
//  access_token:         process.env.TWITTER_ACCESS_TOKEN,
//  access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET,
  app_only_auth:        true
});

var assert = require('assert');

const crow = require("crow-metrics");
const request = require("request");
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.users.query");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 180) * 15 * 60 * 1000);

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

  setInterval( function() {
  queue.inactiveCount( 'queryUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("query.queue.inactive", total);
  });
  }, 15 * 1000 );

queue.process('queryUser', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  queryUser(job.data.user)
  .then(function() {
    updateUserQueryTime(job.data.user);
    done();
  }, function(err) {
    logger.debug("queryUser error %j: %j", job.data, err);
    metrics.counter("queryError").increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
    done(err);
  });
});

function updateUserQueryTime(user){
  var key = util.format("twitter:user:%s", job.user.id_str);
  var currentTimestamp = new Date().getTime();
  redis.hset(key, "queryTimestamp", currentTimestamp);
}

function queryUser(user) {
  return new Promise(function(resolve, reject) {
    logger.info("queryUser %s", user.id_str);
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('users/show', { user_id: user.id_str }, function(err, data)
      {
        if (err){
          logger.error("twitter api error %j %j", user, err);
          metrics.counter("apiError").increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
          reject({ user: user, err: err, reason: "twitter api error" });
          return;
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
