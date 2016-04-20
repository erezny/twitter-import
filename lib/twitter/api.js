
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

function pagedAPIQuery(param, query, saveFn) {
    logger.trace("start %j", param);
    return new RSVP.Promise(function(resolve, reject) {
      var cursor = cursor || "-1";

      function successHandler(results){
        var queryResults = results.query;
        var jobs = {};
        logger.trace(results);
        jobs.save = saveFn( param, queryResults);
        if (queryResults.next_cursor_str !== "0"){
          jobs.query = query(param, queryResults.next_cursor_str );
          RSVP.hash(jobs)
          .then(successHandler, errorHandler);
        } else {
          jobs.save.then(function() {
            resolve(param);
          }, reject);
        }
      }
      function errorHandler(results) {
        logger.error("%j", results);
        reject(results);
      }
      RSVP.hash( { query: query(param, cursor) } )
      .then(successHandler, errorHandler);
    });
}

var userLimiter = new RateLimiter(1, (1 / 60) * 15 * 60 * 1000);
function queryUsers(id_str_list) {
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

var listMembersLimiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
function queryListMembers(list, cursor) {
  return new RSVP.Promise(function(resolve, reject) {
    listMembersLimiter.removeTokens(1, function(err, remainingRequests) {
      T.post('lists/members', { list_id: list.id_str, cursor: cursor, count: 5000 }, function(err, data)
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

var userListOwnershipLimiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
function queryUserListOwnership(user, cursor) {
  return new Promise(function(resolve, reject) {
    logger.debug("queryUserListOwnership %s", user.screen_name);
    userListOwnershipLimiter.removeTokens(1, function(err, remainingRequests) {
      T.get('lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1000 }, function(err, data)
      {
        if (!_.isEmpty(err)){
          logger.error("twitter api error %j %j", user, err);
          reject(err);
          return;
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
    users: queryUsers,
    userListOwnership: queryUserListOwnership,
    listMembers: queryListMembers,
    pagedAPIQuery: pagedAPIQuery
  };
};
