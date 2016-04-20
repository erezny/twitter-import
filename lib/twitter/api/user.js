
// TODO use id_str

var Twit = require('twit');
var T = null;
var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiterUser = new RateLimiter(1, (1 / 180) * 15 * 60 * 1000);

var logger = null;

module.exports = {

  init: function(config) {
    T = new Twit(config.env.twitter.api);
    logger = config.logger;
  },

  // twitter.api.queryUser(user, callback);
  // params: user.id_str String
  //
  // returns:
  //  err:
  //  results: {
  //    id_str
  //    screen_name
  //    followers_count
  //    friends_count
  //    <others>
  //  }
  //

  queryUser: function (user, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    var api = this;
        logger.trace(user.screen_name + " start queryUser");

    limiterUser.removeTokens(1, function(err, remainingRequests) {
      T.get('users/show', user.screen_name ,  function (err, data, response) {
//        debugger;
        logger.debug(user.screen_name + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        logger.debug(user.screen_name + ' limit reset: ' + new Date(response.headers['x-rate-limit-reset']*1000));
        largeRateLimitRemaining = parseInt(response.headers['x-rate-limit-remaining']);
        largeRateLimitReset     = response.headers['x-rate-limit-reset'];

        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          largeRateLimitTimeout = largeRateLimitReset - Date.now()/1000;
          logger.debug( 'timing out until reset: ' + largeRateLimitTimeout);
        }
        else largeRateLimitTimeout = 0;

        //logger.debug(data.users[0]);
        //logger.debug(response.headers);
        callback(err, data);

      });
    });
  },

  validateUsername: function(username)
  {
    return preg_match('/^[A-Za-z0-9_]{1,15}$/', username);
  },

  findUsernames: function(string)
  {
    var usernames = [];
    //$string = 'RT @username: lorem ipsum @cjoudrey etc...';
    //preg_match_all('/@([A-Za-z0-9_]{1,15})/', $string, $usernames);
    rename(usernames);

  },

};
