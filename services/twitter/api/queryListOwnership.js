
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
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.lists.api.ownership");

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

setInterval( function() {
queue.inactiveCount( 'queryUserListOwnership', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

queue.process('queryUserListOwnership', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  metrics.counter("start").increment();
  queryUserListOwnership(job.data.user, job.data.cursor)
  .then(function(list) {
    metrics.counter("finish").increment();
    done();
  }, function(err) {
    logger.error("queryUserListOwnership error %j: %j", job.data, err);
    metrics.counter("ownership.queryError").increment();
    done(err);
  });
});

function queryUserListOwnership(user, cursor) {
  return new Promise(function(resolve, reject) {
    logger.info("queryUserListOwnership %s", user.id_str);
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1000 }, function(err, data)
      {
        if (err){
          logger.error("twitter api error %j %j", user, err);
          reject(err);
          metrics.counter("ownership.apiError").increment();
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryUserListOwnership twitter api callback");
        for (list of data.lists){
          var filteredList = {
            id_str: list.id_str,
            name: list.name,
            uri: list.uri,
            subscriber_count: list.subscriber_count,
            member_count: list.member_count,
            mode: list.mode,
            description: list.description,
            slug: list.slug,
            full_name: list.full_name,
            created_at: list.created_at,
            owner: list.user.id_str
          }
          queue.create('receiveUserListOwnership', { list: filteredList } ).attempts(5).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          queue.create('queryUserListOwnership', { user: user, cursor: data.next_cursor_str }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("ownership.apiFinished").increment();
        resolve(data.lists);
      });
    });
  });
};
