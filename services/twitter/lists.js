
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
var queue = kue.createQueue();

queue.process('twitter.queryUserListOwnership', function(job, done) {
  logger.info("received job");
  queryUserListOwnership(job.data.user, job.data.cursor)
  .then(done);
});

function queryUserListOwnership(user, cursor, callback) {
    return new Promise(function(resolve, reject) {
      logger.info("queryUserListOwnership");
      limiter.removeTokens(1, function(err, remainingRequests) {
       T.get('lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1 })
        .then( function (err, data, response)
        {
          logger.debug("queryUserListOwnership twitter api callback");
          if (err) {
            reject(err);
            return;
          }
          queue.create('twitter.receiveUserListOwnership', { lists: data.lists } ).save();
          if (data.next_cursor_str != 0){
          queue.create('twitter.queryUserListOnwership', { user: job.data.user, cursor: data.next_cursor_str }).save();

          }
          cursor = data.next_cursor_str;
          resolve(data.lists);
        });
      });
    });
  };

queue.create('twitter.queryUserListOwnership', {
  user: { id_str: "16876313" }, cursor: "-1"
});
