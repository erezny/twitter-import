'use strict';
var RateLimiter = require('limiter').RateLimiter;
var RSVP = require('rsvp');
var _ = require('../util.js');

function TwitAPI(_twit, _logger, _metrics, _user) {
  this.T = _twit;
  this.logger = _logger;
  this.metrics = _metrics;
  if ( _.isEmpty(_user)) {
    this.limiters = setAppRateLimiters();
  } else {
    this.limiters = setUserRateLimiters();
  }
}

TwitAPI.prototype.apiHarness = function(method, api, params, limiter ) {
  var _this = this;
  return new RSVP.Promise(function(resolve, reject) {
    //T.setAuth(tokens)
    limiter.removeTokens(1, function(err, remainingRequests) {
      _this.T[method](api, params, function (err, data)
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
};

TwitAPI.prototype.pagedAPIQuery = function(param, query, saveFn) {
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
};

TwitAPI.prototype.friendsIDs = function(user, cursor) {
  return this.apiHarness('get', 'friends/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, this.limiters.friendsIDsLimiter);
};

TwitAPI.prototype.users = function(id_str_list) {
  return this.apiHarness('post', 'users/lookup', { user_id: id_str_list }, this.limiters.userLimiter);
};

TwitAPI.prototype.listMembers = function(list, cursor) {
  return this.apiHarness('post', 'lists/members', { list_id: list.id_str, cursor: cursor, count: 5000 }, this.limiters.listMembersLimiter);
};

TwitAPI.prototype.userListOwnership = function(user, cursor) {
  return this.apiHarness('get', 'lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1000 }, this.limiters.userListOwnershipLimiter);
};

TwitAPI.prototype.userListSubscriptions = function(user, cursor) {
  return this.apiHarness('get', 'lists/subscriptions', { user_id: user.id_str, cursor: cursor, count: 1000 }, this.limiters.userListSubscriptions);
};

function per15MinRateLimiter(num){
  return new RateLimiter(1, (1 / (num - 1) ) * 15 * 60 * 1000);
}
function setAppRateLimiters(){
  return {
    friendsIDsLimiter: per15MinRateLimiter(15),
    userLimiter: per15MinRateLimiter(60),
    listMembersLimiter: per15MinRateLimiter(15),
    userListOwnershipLimiter: per15MinRateLimiter(15),
    userListSubscriptions: per15MinRateLimiter(15)
  };
}
function setUserRateLimiters(){
  return {
    friendsIDsLimiter: per15MinRateLimiter(15),
    userLimiter: per15MinRateLimiter(180),
    listMembersLimiter: per15MinRateLimiter(180),
    userListOwnershipLimiter: per15MinRateLimiter(15),
    userListSubscriptions: per15MinRateLimiter(15)
  };
}

module.exports = TwitAPI;
