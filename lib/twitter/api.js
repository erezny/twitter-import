
var RateLimiter = require('limiter').RateLimiter;
var RSVP = require('rsvp');
var _ = require('../util.js');

var logger;
var metrics;
var T;

function apiHarness(method, api, params, limiter ) {
  return new RSVP.Promise(function(resolve, reject) {
    //T.setAuth(tokens)
    limiter.removeTokens(1, function(err, remainingRequests) {
      T[method](api, params, function (err, data)
      {
        if ( !_.isEmpty(err)){
          reject(err);
        } else {
          metrics.ApiFinished.increment();
          resolve(data);
        }
      });
    });
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
        logger.debug("%s\t%j\n%j", query.name, param, results);
        reject(results);
      }
      RSVP.hash( { query: query(param, cursor) } )
      .then(successHandler, errorHandler);
    });
}

var friendsIDsLimiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
function queryFriendsIDs(user, cursor) {
  return apiHarness('get', 'friends/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, friendsIDsLimiter);
}

var userLimiter = new RateLimiter(1, (1 / 60) * 15 * 60 * 1000);
function queryUsers(id_str_list) {
  return apiHarness('post', 'users/lookup', { user_id: id_str_list }, userLimiter);
}

var listMembersLimiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
function queryListMembers(list, cursor) {
  return apiHarness('post', 'lists/members', { list_id: list.id_str, cursor: cursor, count: 5000 }, listMembersLimiter);
}

var userListOwnershipLimiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
function queryUserListOwnership(user, cursor) {
  return apiHarness('get', 'lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1000 }, userListOwnershipLimiter);
}

var userListSubscriptions = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
function queryUserListSubscriptions(user, cursor) {
  return apiHarness('get', 'lists/subscriptions', { user_id: user.id_str, cursor: cursor, count: 1000 }, userListSubscriptions);
}

module.exports = function(_twit, _logger, _metrics) {
  T = _twit;
  logger = _logger;
  metrics = _metrics;
  return {
    friendsIDs: queryFriendsIDs,
    users: queryUsers,
    userListOwnership: queryUserListOwnership,
    listMembers: queryListMembers,
    pagedAPIQuery: pagedAPIQuery,
    userListSubscriptions: queryUserListSubscriptions
  };
};
