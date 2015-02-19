
// #refactor:10 write queries

var Twit = require('twit');
var T = null;
var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1/15)*15*60*1000);
var limiterList = new RateLimiter(1, (1/15)*15*60*1000);
//12 per minute, reset every 15 minutes
var limiterUser = new RateLimiter(1, (1/180)*15*60*1000);

var logger= null;

module.exports = {

  init: function(config){
    T = new Twit(config.env.twitter.api);
    logger = config.logger;
  },

  smallRateLimitRemaining: 1,
  largeRateLimitRemaining: 1,

  largeRateLimitReset: 0,
  smallRateLimitReset: 0,

  largeRateLimitTimeout: 0,
  smallRateLimitTimeout: 0,

  semFollowers: require('semaphore')(1),
  semFriends: require('semaphore')(1),
  semUsers: require('semaphore')(1),

  // TODO refactor max_queried options so that callbacks can limit number of api calls

  // getFollowers
  // params:
  //  user: twitter user object
  //  cursor_str (optional): twitter api cursor location
  //  callback(chunk): function to call when some results are returned
  queryFollowers: function (user, cursor_str, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    var api = this;

    if (typeof(cursor_str) == 'function'){
      logger.trace('adding default cursor_str');
      callback = cursor_str;
      cursor_str = '-1';
    }

    logger.trace(user.id_str + " start getFollowers");

    limiter.removeTokens(1, function(err, remainingRequests){

      // #apiSafety:1 guess rate limit better
      T.get('followers/ids', { id: user.id_str, count: 5000, stringify_ids: 1, cursor: cursor_str },  function (err, data, response) {

        logger.info(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        logger.debug(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);

        var timeout = 0;
        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          timeout = response.headers['x-rate-limit-reset'] - Date.now()/1000;
          logger.debug(user.id_str + ' until reset: ' + timeout);
          if (timeout < 0) timeout = 0;
        }

        var next_cursor_str = (data) ? data.next_cursor_str : cursor_str;
        var results = (data) ? data.ids : [];
        logger.debug(user.id_str + ' next cursor: ' + next_cursor_str + "\t" + cursor_str);

        if (next_cursor_str !== cursor_str && next_cursor_str !== "0" && timeout > 0){
          // refactor:0 change to save directly to database
          callback(err, results, false);
          setTimeout(api.queryFollowers, timeout * 1000, user, next_cursor_str, callback);
        }
        else if (next_cursor_str !== cursor_str && next_cursor_str !== "0"  && timeout === 0){
          // refactor:0 change to save directly to database
          callback(err, results, false, user, next_cursor_str);
        }
        else
        {
          callback(err, results, true, user);
        }

      });

    });

  },


  // getFollowers
  // params:
  //  user: twitter user object
  //  cursor_str (optional): twitter api cursor location
  //  callback(chunk): function to call when some results are returned
  // TODO rename to expandFollowers
  queryFollowersUsers: function (user, cursor_str, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    var api = this;

    if (typeof(cursor_str) == 'function'){
      logger.trace('adding default cursor_str');
      callback = cursor_str;
      cursor_str = '-1';
    }

    logger.trace(user.id_str + " start getFollowersUsers");

    limiterList.removeTokens(1, function(err, remainingRequests){

      // #apiSafety:1 guess rate limit better
      T.get('followers/list', { id: user.id_str, count: 200, skip_status: true, cursor: cursor_str },  function (err, data, response) {

        logger.info(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        logger.debug(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);

        var timeout = 0;
        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          timeout = response.headers['x-rate-limit-reset'] - Date.now()/1000;
          logger.debug(user.id_str + ' until reset: ' + timeout);
          if (timeout < 0) timeout = 0;
        }

        var next_cursor_str = (data) ? data.next_cursor_str : cursor_str;
        var results = (data) ? data.users : [];
        logger.debug(user.id_str + ' next cursor: ' + next_cursor_str + "\t" + cursor_str);
        logger.debug(user.id_str + " Results: " + results.length);

        if (next_cursor_str !== cursor_str && next_cursor_str !== "0" && timeout > 0){
          // refactor:0 change to save directly to database
          callback(err, results, false);
          setTimeout(api.queryFollowersUsers, timeout * 1000, user, next_cursor_str, callback);
        }
        else if (next_cursor_str !== cursor_str && next_cursor_str !== "0"  && timeout === 0){
          // refactor:0 change to save directly to database
          callback(err, results, false, user, next_cursor_str);
        }
        else
        {
          logger.trace(user.id_str + " final Followers callback. Results: " + JSON.stringify(data));
          callback(err, results, true, user);
        }

      });

    });

  },


  // query friends
  // twitter.api.queryFriends(user, function(err, results)
  // params: user.id_str String
  //
  // returns:
  //  err
  //  ids: [id_str, id_str, ...]
  //  finished: Boolean

  queryFriends: function (user, cursor_str, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    var api = this;

    if (typeof(cursor_str) == 'function'){
      logger.trace('adding default cursor_str');
      callback = cursor_str;
      cursor_str = '-1';
    }

    logger.debug(user.id_str + " start queryFriends");

    limiter.removeTokens(1, function(err, remainingRequests)
    {

      T.get('friends/ids', { id: user.id_str , count: 5000, stringify_ids: 1, cursor: cursor_str  },  function (err, data, response)
      {

        logger.info(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);

        var timeout = 0;
        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          logger.debug(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);
          timeout = response.headers['x-rate-limit-reset'] - Date.now()/1000;
          logger.debug(user.id_str + ' until reset: ' + timeout );
          if (timeout < 0) timeout = 0;
        }

        var next_cursor_str = (data) ? data.next_cursor_str : cursor_str;
        var results = (data) ? data.ids : [];
        logger.debug(user.id_str + ' next cursor: ' + next_cursor_str + "\t" + cursor_str);

        if (next_cursor_str !== cursor_str && next_cursor_str !== "0" && timeout > 0){
          callback(err, results, false);
          setTimeout(api.queryFriends, timeout * 1000, user, next_cursor_str, callback);
        }
        else if (next_cursor_str !== cursor_str && next_cursor_str !== "0" && timeout === 0){
          callback(err, results, false, user,  next_cursor_str);
        }
        else {
          logger.trace(user.id_str + " final Friends callback. Results: " + results.length);
//        logger.debug(user.id_str + " query finished, " + JSON.stringify(data));
          callback(err, results, true, user);
        }
      });
    });
  },

  // query followers
  // twitter.api.queryFriends(user, function(err, results)
  // params: user.id_str String
  //
  // returns:
  //  err
  //  users: [{id_str}, {id_str}, ...]
  //  finished: Boolean

  // TODO rename to expandFriends
  queryFriendsUsers: function (user, cursor_str, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    var api = this;

    if (typeof(cursor_str) == 'function'){
      logger.trace('adding default cursor_str');
      callback = cursor_str;
      cursor_str = '-1';
    }

    logger.debug(user.id_str + " start queryFriendsUsers");

    limiterList.removeTokens(1, function(err, remainingRequests)
    {

      T.get('friends/list', { id: user.id_str , count: 200, skip_status:true, cursor: cursor_str  },  function (err, data, response)
      {

        logger.info(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);

        var timeout = 0;
        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          logger.debug(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);
          timeout = response.headers['x-rate-limit-reset'] - Date.now()/1000;
          logger.debug(user.id_str + ' until reset: ' + timeout );
          if (timeout < 0) timeout = 0;
        }

        var next_cursor_str = (data) ? data.next_cursor_str : cursor_str;
        var results = (data) ? data.users : [];
        logger.debug(user.id_str + ' next cursor: ' + next_cursor_str + "\t" + cursor_str);
        logger.debug(user.id_str + " Results: " + results.length);

        if (next_cursor_str !== cursor_str && next_cursor_str !== "0" && timeout > 0){
          callback(err, results, false);
          setTimeout(api.queryFriendsUsers, timeout * 1000, user, next_cursor_str, callback);
        }
        else if (next_cursor_str !== cursor_str && next_cursor_str !== "0" && timeout === 0){
          callback(err, results, false, user, next_cursor_str);
        }
        else {
          logger.trace(user.id_str + " final Friends callback. Results: " + results.length);
//        logger.debug(user.id_str + " query finished, " + JSON.stringify(data));
          callback(err, results, true, user);
        }
      });
    });
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
  // remarks:
  // Self limits to ~12 requests every minute
  //

  queryUser: function (user, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    var api = this;
        logger.trace(user.id_str + " start queryUser");

    limiterUser.removeTokens(1, function(err, remainingRequests) {
      T.get('users/show', { user_id: user.id_str },  function (err, data, response) {
//        debugger;
        logger.info(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        largeRateLimitRemaining = parseInt(response.headers['x-rate-limit-remaining']);
        largeRateLimitReset     = response.headers['x-rate-limit-reset'];

        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          logger.debug(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);
          largeRateLimitTimeout = largeRateLimitReset - Date.now()/1000;
          logger.debug( 'timing out until reset: ' + largeRateLimitTimeout);
        }
        else largeRateLimitTimeout = 0;

        if (! data){
          logger.debug(user.id_str + " no data returned");
          setTimeout(api.queryUser, largeRateLimitTimeout*1000, user, callback);
          return;
        }
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
