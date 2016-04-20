
var RSVP = require('rsvp');
var _ = require('../../util.js');
var model = require('../../../lib/twitter/models/user.js');
var listModel = require('../../../lib/twitter/models/list.js');
var logger;
var metrics;
var neo4j;

function queryRunner(query){
  return new RSVP.Promise(function(resolve, reject) {
    var operation = neo4j.operation('transaction/commit', 'POST', query);
    neo4j.call(operation, function(err, neo4jresult, neo4jresponse) {
      if (!_.isEmpty(err)){
        logger.error("query error: %j", err);
        metrics.TxnError.increment();
        reject(err);
      } else {
        logger.debug("committed");
        logger.trace(neo4jresult, neo4jresponse);
        metrics.TxnFinished.increment();
        resolve();
      }
    });
  });
}

function resetFriends(user) {
  logger.trace(user);
  var query = {
    statements: [
      {
        statement: "match (u:twitterUser { id_str: {user}.id_str })-[r:follows]->(:twitterUser) " +
                   "match (u)-[r:follows]->(:twitterUser) " +
                   "DELETE r ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } }, {
    statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
               "remove u.friends_imported ",
    parameters: {
      'user': {
        id_str: user.id_str
      } } } ] };
  return queryRunner(query);
}

function saveFriendsIDs(user, friendsIDs) {
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

  return queryRunner(query);
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
    return queryRunner(query);
}

function saveLists(user, lists) {
      var query = {
        statements: [
          {
            statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                       "set u.listOwnership_imported = timestamp() ",
            parameters: {
              'user': {
                id_str: user.id_str
      } } } ] };
      for ( var list of lists ) {
        query.statements.push({
          statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                     "merge (f:twitterList { id_str: {list}.id_str }) " +
                     "merge (u)-[:owns]->(f) ",
          parameters: {
            'user': { id_str: list.user.id_str },
            'list': { id_str: list.id_str }
          }
        });
        query.statements.push({
          statement: "merge (y:twitterList { id_str: {list}.id_str }) " +
                      "set " +
                      " y.id_str = {list}.id_str, " +
                      " y.name = {list}.name, " +
                      " y.uri = {list}.uri, " +
                      " y.subscriber_count = {list}.subscriber_count, " +
                      " y.member_count = {list}.member_count, " +
                      " y.mode = {list}.mode, " +
                      " y.description = {list}.description, " +
                      " y.slug = {list}.slug, " +
                      " y.full_name = {list}.full_name, " +
                      " y.created_at = {list}.created_at ",
          parameters: {
            'list': listModel.filterList(list)
          }
        });
      }
      return queryRunner(query);
}
function resetListOwnerships(user) {
  logger.trace(user);
  var query = {
    statements: [
      {
        statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                   "match (u)-[r:owns]->(:twitterList) " +
                   "DELETE r ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } }, {
    statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
               "remove u.listOwnership_imported ",
    parameters: {
      'user': {
        id_str: user.id_str
      } } } ] };
  return queryRunner(query);
}
function resetListMembers(list) {
  logger.trace(list);
  var query = {
    statements: [
      {
        statement: "match (l:twitterList { id_str: {list}.id_str }) " +
                   "match (l)-[r:includes]->(:twitterUser) " +
                   "DELETE r ",
        parameters: {
          'list': {
            id_str: list.id_str
  } } }, {
    statement: "match (l:twitterList { id_str: {list}.id_str }) " +
               "remove l.members_imported ",
    parameters: {
      'list': {
        id_str: list.id_str
      } } } ] };
  return queryRunner(query);
}

function saveListMembers(list, members) {
    logger.trace("save");
    var query = {
      statements: [ ]
    };
    for ( var user of members ) {
      query.statements.push({
        statement: user_cypher,
        parameters: {
          'user':  model.filterUser(user)
        }
      });
      query.statements.push({
        statement: "match (l:twitterList { id_str: {list}.id_str }) " +
                   "merge (u:twitterUser { id_str: {user}.id_str }) " +
                   "merge (l)-[:includes]->(u) ",
        parameters: {
          'user': { id_str: user.id_str },
          'list': { id_str: list.id_str }
        }
      });
    }
    return queryRunner(query);
}

module.exports = function(_neo4j, _logger, _metrics) {
  neo4j = _neo4j;
  logger = _logger;
  metrics = _metrics;
  return {
    saveFriendsIDs: saveFriendsIDs,
    saveUsers: saveUsers,
    resetFriends: resetFriends,
    saveLists: saveLists,
    resetListOwnerships: resetListOwnerships,
    resetListMembers: resetListMembers,
    saveListMembers: saveListMembers
  };
};
