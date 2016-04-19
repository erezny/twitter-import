
var RSVP = require('rsvp');
var _ = require('../util.js');

var logger;
var metrics;
var T;

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

function queryFriendsIDs(user, cursor) {
  return new RSVP.Promise(function(resolve, reject) {
    //T.setAuth(tokens)
    logger.debug("queryFriendsIDs %s %s", user.screen_name, cursor);
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('friends/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, function (err, data)
      {
        logger.trace("queryFriendsIDs twitter api callback");
        if ( !_.isEmpty(err)){
          if (err.message == "Not authorized."){
            //queue.create('markUserPrivate', { user: user } ).removeOnComplete(true).save();
            return;
          } else if (err.message == "User has been suspended."){
            //queue.create('markUserSuspended', { user: user } ).removeOnComplete(true).save();
            return;
          } else {
            logger.error("twitter api error %j %j", user, err);
            metrics.ApiError.increment();
            return;
          }
          reject(err);
        }
        if (data){
          logger.trace("Data %j", data);
          if ( !data.ids) {
            reject();
          } else {
            metrics.ApiFinished.increment();
            resolve(data);
          }
        }
      });
    });
  });
}

function repeatIDQuery(user, query, saveFn) {
    logger.debug("start %s", user.screen_name);
    return new RSVP.Promise(function(resolve, reject) {
      var cursor = cursor || "-1";
      var itemsFound = 0;

      function successHandler(results){
        var queryResults = results.query;
        var jobs = {};
        logger.trace(results);
        if (queryResults.ids && queryResults.ids.length > 0){
          itemsFound += queryResults.ids.length;
          logger.trace(itemsFound);
          jobs.save = saveFn( user, queryResults.ids);
        } else {
          jobs.save = new RSVP.Promise(function(done) {done();});
        }
        if (queryResults.next_cursor_str !== "0"){
          jobs.query = query(user, queryResults.next_cursor_str );
          RSVP.hash(jobs)
          .then(successHandler, errorHandler);
        } else {
          jobs.save.then(function() {
            logger.info("queryFriendsIDs %s found %d", user.screen_name, itemsFound);
            resolve(user);
          }, reject);
        }
      }
      function errorHandler(results) {
        logger.error("%j", results);
        reject();
      }
      RSVP.hash( { query: query(user, cursor) } )
      .then(successHandler, errorHandler);
    });
}

var userLimiter = new RateLimiter(1, (1 / 60) * 15 * 60 * 1000);
function queryUser(id_str_list) {
  return new RSVP.Promise(function(resolve, reject) {
    userLimiter.removeTokens(1, function(err, remainingRequests) {
      T.post('users/lookup', { user_id: id_str_list }, function(err, data)
      {
        if (!_.isEmpty(err)){
          if (err.twitterReply.statusCode == 500) {
            //not a priority, just continue
            resolve(data);
          } else {
            logger.error("twitter api error %j %j", id_str_list, err);
            metrics.counter("apiError").increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
            reject({ err: err, message: "twitter api error" });
            return;
          }
        }
        resolve(data);
      });
    });
  });
}

module.exports = function(_twit, _logger, _metrics) {
  T = _twit;
  logger = _logger;
  metrics = _metrics;
  return {
    friendsIDs: queryFriendsIDs,
    repeatIDQuery: repeatIDQuery,
    user: queryUser
  };
};
