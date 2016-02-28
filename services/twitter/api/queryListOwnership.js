
// #refactor:10 write queries
var util = require('util');
var T = require('../../../lib/twit.js');

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.lists.api.ownership");
var queue = require('../../../lib/kue.js');

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
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
