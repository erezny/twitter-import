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
  var metricsTags = {
    method: method,
    apiEndpoint: api
  };
  return new RSVP.Promise(function(resolve, reject) {
    //T.setAuth(tokens)
    limiter.removeTokens(1, function(err, remainingRequests) {
      _this.T[method](api, params, function (err, data)
      {
        if ( !_.isEmpty(err)) {
          _this.metrics.ApiError.withTags(metricsTags).increment();
          reject(err);
        } else {
          _this.metrics.ApiFinished.withTags(metricsTags).increment();
          resolve(data);
        }
      });
    });
  });
};

TwitAPI.prototype.pagedAPIQuery = function(param, query, saveFn) {
  var logger = this.logger;
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
      logger.error("%s\t%j\n%j", query.name, param, results);
      reject(results);
    }
    RSVP.hash( { query: query(param, cursor) } )
    .then(successHandler, errorHandler);
  });
};

TwitAPI.prototype.friendsIDs = function() {
  var _this = this;
  return function(user, cursor) {
    return _this.apiHarness('get', 'friends/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, _this.limiters.friendsIDsLimiter);
  };
};

TwitAPI.prototype.users = function() {
  var _this = this;
  return function(id_str_list) {
    return _this.apiHarness('post', 'users/lookup', { user_id: id_str_list }, _this.limiters.userLimiter);
  };
};

TwitAPI.prototype.listMembers = function() {
  var _this = this;
  return function(list, cursor) {
    return _this.apiHarness('post', 'lists/members', { list_id: list.id_str, cursor: cursor, count: 5000 }, _this.limiters.listMembersLimiter);
  };
};

TwitAPI.prototype.userListOwnership = function() {
  var _this = this;
  return function(user, cursor) {
    return _this.apiHarness('get', 'lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1000 }, _this.limiters.userListOwnershipLimiter);
  };
};

TwitAPI.prototype.userListSubscriptions = function() {
  var _this = this;
  return function(user, cursor) {
    return _this.apiHarness('get', 'lists/subscriptions', { user_id: user.id_str, cursor: cursor, count: 1000 }, _this.limiters.userListSubscriptions);
  };
};

//returns user info
TwitAPI.prototype.accountVerifyCredentials = function() {
  var _this = this;
  return function() {
    return _this.apiHarness('get', 'account/verify_credentials', { skip_status: true, include_entities: false, include_email: false }, _this.limiters.accountVerifyCredentials);
  };
};

TwitAPI.prototype.userTweets = function() {
  var _this = this;
  return function(user, params, cursor) {
    if (params.since_id) {
      return _this.apiHarness('get', 'statuses/user_timeline',
        { user_id: user.id_str, since_id: params.since_id, count: 200 }, _this.limiters.userTweets);
    } else if (params.max_id) {
      return _this.apiHarness('get', 'statuses/user_timeline',
        { user_id: user.id_str, max_id: params.max_id, count: 200 }, _this.limiters.userTweets);
    } else {
      return _this.apiHarness('get', 'statuses/user_timeline',
        { user_id: user.id_str, count: 200 }, _this.limiters.userTweets);
    }

  };
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
    userListSubscriptions: per15MinRateLimiter(15),
    accountVerifyCredentials: per15MinRateLimiter(15)
  };
}

module.exports = TwitAPI;
