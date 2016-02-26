
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

const metrics = require('../../../lib/crow.js').withPrefix("twitter.lists.api.memberships");

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

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
queue.inactiveCount( 'queryListMemberships', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

queue.process('queryListMemberships', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryListMemberships received job %j", job);
  metrics.counter("start").increment();
  var user = job.data.user;
  checkUserQueryTime(user)
  .then(queryUser)
  .then(doQuery)
  .then(resolve, reject);

  function resolve(){
    metrics.counter("finish").increment();
    updateUserQueryTime(user)
    done();
  }

  function doQuery(){
    return queryListMemberships(user, job.data.cursor)
  }

  function reject(err){
    logger.error("queryListMemberships error: %j", err);
    metrics.counter("queryError").increment();
    done(err);
  };

});

function checkUserQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hgetall(key, processQueryTime);

    function processQueryTime(err, obj){
      if ( !obj || !obj.membershipsQueryTimestamp || obj.membershipsQueryTimestamp > parseInt((+new Date) / 1000) - (60 * 60 * 24) ) {
        resolve(user);
      } else {
        reject( { message: "user recently queried" } );
      }
    }
  });
}

function updateUserQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hset(key, "membershipsQueryTimestamp", parseInt((+new Date) / 1000), function() {
      resolve()
    });
  });
}

function queryListMemberships(list, cursor) {
  return new Promise(function(resolve, reject) {
    logger.info("queryListMemberships");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('lists/memberships', { list_id: list.id_str, cursor: cursor, count: 5000 }, function (err, data)
      {
        if (err){
          logger.error("twitter api error %j", err);
          metrics.counter("apiError").increment();
          reject(err);
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryListMemberships twitter api callback");
        for (list of data.lists){
          var filteredList = filterList(list)
          queue.create('receiveUserListOwnership', { list: filteredList } ).attempts(5).removeOnComplete( true ).save();
          queue.create('receiveListMembers', { list: list, user: { id_str: user.id_str } } ).attempts(2).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          queue.create('queryListMembers', { list: list, cursor: data.next_cursor_str }).attempts(5).removeOnComplete( true ).save();
        }
        metrics.counter("apiFinished").increment();
        resolve(data.lists);
      });
    });
  });
};

function filterList (list) {
  return {
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
  };
}
