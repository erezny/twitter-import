var Twit = require('twit');
const TwitterQueries = require('./twitter/api.js');

const tokens = {
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
  //access_token:         process.env.TWITTER_ACCESS_TOKEN,
  //access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
  app_only_auth:        true
};

module.exports = function(logger, metrics) {
  var T = new Twit(tokens);
  T.queries = new TwitterQueries(T, logger, metrics);
  T.appAuth =  function() {
    this.setAuth(tokens);
  };
  T.userAuth =  function(acces_token, access_token_secret) {
    var auth_token = tokens;
    auth_token.app_only_auth = false;
    auth_token.access_token = acces_token;
    auth_token.access_token_secret = acces_token_secret;
    this.setAuth(auth_token);
  };
  return T;
};
