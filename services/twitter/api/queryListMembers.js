
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
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.lists.api.members");

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
queue.inactiveCount( 'queryListMembers', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("members.queue.inactive", total);
});
}, 15 * 1000 );

queue.process('queryListMembers', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryListMembers received job %j", job);
  queryListMembers(job.data.list)
  .then(done)
  .catch(function(err) {
    logger.error("queryListMembers error: %j", err);
    metrics.counter("members.queryError").increment();
  });
});

function queryListMembers(list, cursor) {
  return new Promise(function(resolve, reject) {
    logger.info("queryListMembers");
    limiterMembers.removeTokens(1, function(err, remainingRequests) {
      T.get('lists/members', { list_id: list.id_str, cursor: cursor, count: 5000 }, function (err, data)
      {
        if (err){
          logger.error("twitter api error %j", err);
          metrics.counter("members.apiError").increment();
          reject(err);
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryListMembers twitter api callback");
        for (user of data.users){
          queue.create('receiveUser', { user: user } ).removeOnComplete(true).save();
          queue.create('receiveListMembers', { list: list, user: { id_str: user.id_str } } ).attempts(5).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          queue.create('queryListMembers', { list: list, cursor: data.next_cursor_str }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("members.apiFinished").increment();
        resolve(data.lists);
      });
    });
  });
};
