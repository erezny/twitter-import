
// #refactor:10 write queries

var Twit = require('twit');

var T = new Twit({
  consumer_key:         '***REMOVED***',
  consumer_secret:      '***REMOVED***',
  access_token:         '***REMOVED***',
  access_token_secret:  '***REMOVED***'
});

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(15, 15*59*1000);
//12 per minute, reset every 15 minutes
var limiterUser = new RateLimiter(12, 59*1000);

//get rid of these friggen things
var sem = require('semaphore')(1);
var semUser = require('semaphore')(1);
var semUserData = require('semaphore')(1);

module.exports = {

  smallRateLimitRemaining: 1,
  largeRateLimitRemaining: 1,

  largeRateLimitReset: 0,
  smallRateLimitReset: 0,

  largeRateLimitTimeout: 0,
  smallRateLimitTimeout: 0,

  // getFollowers
  // params:
  //  user: twitter user object
  //  cursor_str (optional): twitter api cursor location
  //  callback(chunk): function to call when some results are returned
  getFollowers: function (user, cursor_str, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //
    if (typeof(cursor_str) == 'function'){
      console.log('adding default cursor_str');
      callback = cursor_str;
      cursor_str = '0';
    }

    console.log(user.id_str + "start getFollowers");

    limiter.removeTokens(1, function(err, remainingRequests){

      // #apiSafety:1 guess rate limit better
      T.get('followers/ids', { id: user.id_str, count: 5000, stringify_ids: 1 },  function (err, data, response) {

        console.log(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        console.log(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);

        var timeout = 0;
        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          timeout = response.headers['x-rate-limit-reset'] - Date.now();
          console.log(user.id_str + ' until reset: ' + timeout/1000);
        }

        var next_cursor = data.next_cursor_str || sursor_str;
        console.log(user.id_str + ' next cursor: ' + next_cursor);

        if (data.next_cursor_str !== cursor_str){
          // refactor:0 change to save directly to database
          callback(data.ids, false);
          setTimeout(getFollowers, timeout, user, next_cursor, callback);
        }

        // else
        callback(data.ids, true);

      });

    });

  },

  getFollowing: function (user, cursor_str, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //

    if (typeof(cursor_str) == 'function'){
      console.log('adding default cursor_str');
      callback = cursor_str;
      cursor_str = '0';
    }

    console.log(user.id_str + "start getFollowing");

    limiter.removeTokens(1, function(err, remainingRequests)
    {

      T.get('friends/ids', { id: user.is , count: 5000, stringify_ids: 1 },  function (err, data, response)
      {

        console.log(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        console.log(user.id_str + ' limit reset: ' + response.headers['x-rate-limit-reset']);

        var timeout = 0;
        if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
          timeout = response.headers['x-rate-limit-reset'] - Date.now();
          console.log(user.id_str + ' until reset: ' + timeout/1000);
        }

        var next_cursor = data.next_cursor_str || sursor_str;
        console.log(user.id_str + ' next cursor: ' + next_cursor);

        if (data.next_cursor_str !== cursor_str){
          callback(data.ids, false);
          setTimeout(getFollowing, timeout, user, next_cursor, callback);
        }
        //else
        callback(data.ids, true);
      });
    });
  },

  getUser: function (user, callback) {
    //
    //  gather the list of user id's that follow @tolga_tezel
    //
    limiterUser.removeTokens(1, function(err, remainingRequests) {
      T.get('users/show', { user_id: user.id_str },  function (err, data, response) {

        console.log(user.id_str + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        largeRateLimitRemaining = response.headers['x-rate-limit-remaining'];
        largeRateLimitReset     = response.headers['x-rate-limit-reset'];

        if (parseInt(response.headers['x-rate-limit-remaining']) === 0){
          largeRateLimitTimeout = largeRateLimitReset - Date.now();
          console.log( 'timing out until reset: ' + largeRateLimitTimeout/1000);
        }
        else largeRateLimitTimeout = 0;

        if (! data){
          setTimeout(this.getUser, largeRateLimitTimeout, user, callback);
          return;
        }
        //console.log(data.users[0]);
        //console.log(response.headers);
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
