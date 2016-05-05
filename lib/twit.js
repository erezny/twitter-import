'use strict';
var Twit = require('twit');
var assert = require('assert');
const TwitterQueries = require('./twitter/api.js');
var RateLimiter = require('limiter').RateLimiter;
var _ = require('./util.js');

const tokens = {
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
  //access_token:         process.env.TWITTER_ACCESS_TOKEN,
  //access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
  app_only_auth:        true
};

assert(tokens.consumer_key.length > 0, "Twitter API Consumer key blank");
assert(tokens.consumer_secret.length > 0, "Twitter API Consumer secret blank");

function Twitter(_logger, _metrics, userAuth) {
  _logger.trace("userAuth: %j", userAuth);

  this.logger = _logger;
  this.metrics = _metrics;

  var auth_token = Object.assign({}, tokens);
  if (!_.isEmpty(userAuth)){
    auth_token.app_only_auth = false;
    auth_token.access_token = userAuth.access_token;
    auth_token.access_token_secret = userAuth.access_token_secret;
  }

  this.T = new Twit(auth_token);
  this.queries = new TwitterQueries(this.T, _logger, _metrics, userAuth);

  if (!_.isEmpty(userAuth)){
    //test user auth
    this.queries.accountVerifyCredentials()()
    .then(function(user) {
      _logger.info("user %s login success", user.screen_name );
    },
     function(err) {
       _logger.error("user login error");
     });
  }
}

module.exports = Twitter;
// Resource family	Requests / 15-min window (user auth)	Requests / 15-min window (app auth)
// GET application/rate_limit_status	application	180	180
// GET favorites/list	favorites	15	15
// GET followers/ids	followers	15	15
// GET followers/list	followers	15	30
// GET friends/ids	friends	15	15
// GET friends/list	friends	15	30
// GET friendships/show	friendships	180	15
// GET help/configuration	help	15	15
// GET help/languages	help	15	15
// GET help/privacy	help	15	15
// GET help/tos	help	15	15
// GET lists/list	lists	15	15
// GET lists/members	lists	180	15
// GET lists/members/show	lists	15	15
// GET lists/memberships	lists	15	15
// GET lists/ownerships	lists	15	15
// GET lists/show	lists	15	15
// GET lists/statuses	lists	180	180
// GET lists/subscribers	lists	180	15
// GET lists/subscribers/show	lists	15	15
// GET lists/subscriptions	lists	15	15
// GET search/tweets	search	180	450
// GET statuses/lookup	statuses	180	60
// GET statuses/retweeters/ids	statuses	15	60
// GET statuses/retweets/:id	statuses	15	60
// GET statuses/show/:id	statuses	180	180
// GET statuses/user_timeline	statuses	180	300
// GET trends/available	trends	15	15
// GET trends/closest	trends	15	15
// GET trends/place	trends	15	15
// GET users/lookup	users	180	60
// GET users/show	users	180	180
// GET users/suggestions	users	15	15
// GET users/suggestions/:slug	users	15	15
// GET users/suggestions/:slug/members	users	15	15
