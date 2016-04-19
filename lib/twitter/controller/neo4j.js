
var RSVP = require('rsvp');
var _ = require('../../util.js');

var logger;
var metrics;
var neo4j;
function saveFriendsIDs(user, friendsIDs, resolve, reject) {
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

module.exports = function(_neo4j, _logger, _metrics) {
  neo4j = _neo4j;
  logger = _logger;
  metrics = _metrics;
  return {
    saveFriendsIDs: saveFriendsIDs,
  };
};
