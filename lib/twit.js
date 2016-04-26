var Twit = require('twit');
const TwitterQueries = require('./twitter/api.js');
var RateLimiter = require('limiter').RateLimiter;

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
  setAppRateLimiters(T);
  T.appAuth =  function() {
    this.setAuth(tokens);
  };
  T.userAuth =  function(_acces_token, _access_token_secret) {
    var auth_token = Object.assign({}, tokens);
    auth_token.app_only_auth = false;
    auth_token.access_token = _acces_token;
    auth_token.access_token_secret = _access_token_secret;
    this.setAuth(auth_token)
    this.friendsIDsLimiter = per15MinRateLimiter(15);
    this.userLimiter = per15MinRateLimiter(180);
    this.listMembersLimiter = per15MinRateLimiter(180);
    this.userListOwnershipLimiter = per15MinRateLimiter(15);
    this.userListSubscriptions = per15MinRateLimiter(15);
  };
  return T;
};

function per15MinRateLimiter(num){
  return new RateLimiter(1, (1 / (num - 1) ) * 15 * 60 * 1000);
}
function setAppRateLimiters(T){
  T.friendsIDsLimiter = per15MinRateLimiter(15);
  T.userLimiter = per15MinRateLimiter(60);
  T.listMembersLimiter = per15MinRateLimiter(15);
  T.userListOwnershipLimiter = per15MinRateLimiter(15);
  T.userListSubscriptions = per15MinRateLimiter(15);
}
function setUserRateLimiters(T){
  T.friendsIDsLimiter = per15MinRateLimiter(15);
  T.userLimiter = per15MinRateLimiter(180);
  T.listMembersLimiter = per15MinRateLimiter(180);
  T.userListOwnershipLimiter = per15MinRateLimiter(15);
  T.userListSubscriptions = per15MinRateLimiter(15);
}

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
