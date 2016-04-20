
var RSVP = require('rsvp');
var _ = require('../../util.js');
var model = require('../../../lib/twitter/models/user.js');
var logger;
var metrics;
var neo4j;

function resetFriends(user) {
  return new RSVP.Promise(function(resolve, reject) {
    logger.trace(user);
    var query = {
      statements: [
        {
          statement: "match (u:twitterUser { id_str: {user}.id_str })-[r:follows]->(m:twitterUser) " +
                     "delete r ",
          parameters: {
            'user': {
              id_str: user.id_str
    } } }, {
      statement: "match (u:twitterUser { id_str: {user}.id_str })" +
                 "unset r.friends_imported ",
      parameters: {
        'user': {
          id_str: user.id_str
        } } } ] };
    var operation = neo4j.operation('transaction/commit', 'POST', query);
    neo4j.call(operation, function(err, neo4jresult, neo4jresponse) {
      if (!_.isEmpty(err)){
        logger.error("query error: %j", err);
        metrics.TxnError.increment();
        reject(err);
      } else {
        logger.debug("committed");
        metrics.TxnFinished.increment();
        resolve();
      }
    });
  });
}

function saveFriendsIDs(user, friendsIDs) {
  return new RSVP.Promise(function(resolve, reject) {
      logger.trace(user);
      var query = {
        statements: [
          {
            statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                       "set u.friends_imported = timestamp() ",
            parameters: {
              'user': {
                id_str: user.id_str
      } } } ] };
      for ( var friendID of friendsIDs ) {
        query.statements.push({
          statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                     "merge (f:twitterUser { id_str: {friend}.id_str }) " +
                     "merge (u)-[:follows]->(f) ",
          parameters: {
            'user': { id_str: user.id_str },
            'friend': { id_str: friendID }
          }
        });
      }
      var operation = neo4j.operation('transaction/commit', 'POST', query);
      neo4j.call(operation, function(err, neo4jresult, neo4jresponse) {
        if (!_.isEmpty(err)){
          logger.error("query error: %j", err);
          metrics.TxnError.increment();
          reject(err);
        } else {
          logger.debug("committed");
          metrics.TxnFinished.increment();
          resolve();
        }
      });
    });
}

const user_cypher = "match (y:twitterUser) where y.id_str= {user}.id_str " +
            "set y.analytics_updated = 0, " +
            " y.screen_name = {user}.screen_name, " +
            " y.name = {user}.name, " +
            " y.followers_count = {user}.followers_count, " +
            " y.friends_count = {user}.friends_count, " +
            " y.favourites_count = {user}.favourites_count, " +
            " y.description = {user}.description, " +
            " y.location = {user}.location, " +
            " y.statuses_count = {user}.statuses_count, " +
            " y.listed_count = {user}.listed_count, " +
            " y.protected = {user}.protected, " +
            " y.user_imported = timestamp() " ;

function saveUsers(users) {
  return new RSVP.Promise(function(resolve, reject) {
    logger.trace("save");

    var query = {
      statements: [ ]
    };
    for ( var user of users ) {
      query.statements.push({
        statement: user_cypher,
        parameters: {
          'user':  model.filterUser(user)
        }
      });
    }
    var operation = neo4j.operation('transaction/commit', 'POST', query);
    neo4j.call(operation, function(err, neo4jresult, neo4jresponse) {
      if (!_.isEmpty(err)){
        logger.error("query error: %j", err);
        metrics.TxnError.increment();
        reject(err);
      } else {
        logger.trace(neo4jresult);
        metrics.TxnFinished.increment();
        resolve(neo4jresult);
      }
    });
  });
}

module.exports = function(_neo4j, _logger, _metrics) {
  neo4j = _neo4j;
  logger = _logger;
  metrics = _metrics;
  return {
    saveFriendsIDs: saveFriendsIDs,
    saveUsers: saveUsers,
    resetFriends: resetFriends
  };
};
