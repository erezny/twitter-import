
// #refactor:10 write queries

var Twit = require('twit');
var T = new Twit({
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
  //access_token:         process.env.TWITTER_ACCESS_TOKEN,
  //access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
  app_only_auth:        true
});

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'trace'
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

process.once( 'SIGTERM', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
});

queue.process('queryUserListOwnership', function(job, done) {
//  logger.info("received job");
  logger.trace("received job %j");
  queryUserListOwnership(job.data.user, job.data.cursor)
  .then(done);
});

function queryUserListOwnership(user, cursor, callback) {
    return new Promise(function(resolve, reject) {
      logger.info("queryUserListOwnership");
      limiter.removeTokens(1, function(err, remainingRequests) {
       T.get('lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1 })
        .then( function (data)
        {
          logger.trace("Data %j", data);
          logger.debug("queryUserListOwnership twitter api callback");
          queue.create('receiveUserListOwnership', { lists: data.lists } ).save();
          if (data.next_cursor_str !== '0'){
            queue.create('queryUserListOwnership', { user: user, cursor: data.next_cursor_str }).save();
          }
          resolve(data.lists);
        }, function (err) {
          logger.error("twitter api error %j", err);
          reject(err);
        });
      });
    });
  };
