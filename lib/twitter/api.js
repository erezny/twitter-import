
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
        logger.debug("queryFriendsIDs twitter api callback");
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

module.exports = function(_twit, _logger, _metrics) {
  T = _twit;
  logger = _logger;
  metrics = _metrics;
  return {
    friendsIDs: queryFriendsIDs,
  };
};
