
var RSVP = require('rsvp');
var _ = require('../../util.js');
var models = require('../../../lib/twitter/models.js');

function TwitterNeo4j(_neo4j, _logger, _metrics) {
 this.neo4j = _neo4j;
 this.logger = _logger;
 this.metrics = _metrics;
}

TwitterNeo4j.prototype.queryRunner = function(query, _retry) {
  var retry = _retry || 0;
  var _this = this;
  return new RSVP.Promise(function(resolve, reject) {
    var operation = _this.neo4j.operation('transaction/commit', 'POST', query);
    _this.neo4j.call(operation, function(err, neo4jresult, neo4jresponse) {
      if (!_.isEmpty(err)){
        _this.logger.error("query error: (%d) %j\nquery: %j", retry, err, query);
        _this.metrics.TxnError.increment();
        if (err.code == 'ECONNRESET' && +retry < 3 ){
          queryRunner(query, +retry + 1).then(resolve, reject);
        } else {
          reject(err);
        }
      } else {
        _this.logger.debug("committed");
        _this.logger.trace(neo4jresult, neo4jresponse);
        _this.metrics.TxnFinished.increment();
        resolve(neo4jresult.results);
      }
    });
  });
}

TwitterNeo4j.prototype.resetFriends = function(user) {
  this.logger.trace(user);
  var query = {
    statements: [
      {
        statement: "match (u:twitterUser { id_str: {user}.id_str })" +
                   "optional match (u)-[r:follows]->(:twitterUser) " +
                   "DELETE r ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } } ] };
  return query;
};

TwitterNeo4j.prototype.saveFriendsIDs = function(user, friendsIDs, preQuery) {
  this.logger.trace(user);
  var query = {
    statements: [
      {
        statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                   "set u.friends_imported = timestamp() ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } } ] };
  if (!_.isEmpty(preQuery)){
    Array.prototype.unshift.apply(query.statements, preQuery.statements);
  }
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

  return this.queryRunner(query);
};

TwitterNeo4j.prototype.resetFollowers = function(user) {
  this.logger.trace(user);
  var query = {
    statements: [
      {
        statement: "match (u:twitterUser { id_str: {user}.id_str })" +
                   "optional match (u)<-[r:follows]-(:twitterUser) " +
                   "DELETE r ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } } ] };
  return query;
};

TwitterNeo4j.prototype.saveFollowersIDs = function(user, followersIDs, preQuery) {
  this.logger.trace(user);
  var query = {
    statements: [
      {
        statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                   "set u.followers_imported = timestamp() ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } } ] };
  if (!_.isEmpty(preQuery)){
    Array.prototype.unshift.apply(query.statements, preQuery.statements);
  }
  for ( var followerID of followersIDs ) {
    query.statements.push({
      statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                 "merge (f:twitterUser { id_str: {follower}.id_str }) " +
                 "merge (u)<-[:follows]-(f) ",
      parameters: {
        'user': { id_str: user.id_str },
        'follower': { id_str: followerID }
      }
    });
  }

  return this.queryRunner(query);
};

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

TwitterNeo4j.prototype.saveUsers = function(users) {
    this.logger.trace("save");

    var query = {
      statements: [ ]
    };
    for ( var user of users ) {
      query.statements.push({
        statement: user_cypher,
        parameters: {
          'user':  models.filterUser(user)
        }
      });
    }
    return this.queryRunner(query);
}

TwitterNeo4j.prototype.saveLists = function(user, lists) {
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
            'list': models.filterList(list)
          }
        });
      }
      return this.queryRunner(query);
}
TwitterNeo4j.prototype.resetListOwnerships = function(user) {
  this.logger.trace(user);
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
  return this.queryRunner(query);
}
TwitterNeo4j.prototype.resetListMembers = function(list) {
  this.logger.trace(list);
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
  return this.queryRunner(query);
}

TwitterNeo4j.prototype.saveListMembers = function(list, members) {
    this.logger.trace("save");
    var query = {
      statements: [ {
        statement: "match (l:twitterList { id_str: {list}.id_str }) " +
                   "set l.members_imported = timestamp() ",
        parameters: {
          'list': {
            id_str: list.id_str
  } } } ]
    };
    for ( var user of members ) {
      query.statements.push({
        statement: user_cypher,
        parameters: {
          'user':  models.filterUser(user)
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
    return this.queryRunner(query);
}
TwitterNeo4j.prototype.resetListSubscriptions = function(user) {
  this.logger.trace(user);
  var query = {
    statements: [
      {
        statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                   "match (u)-[r:subscribes_to]->(:twitterList) " +
                   "DELETE r ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } }, {
    statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
               "remove u.listSubscriptions_imported ",
    parameters: {
      'user': {
        id_str: user.id_str
      } } } ] };
  return this.queryRunner(query);
}
TwitterNeo4j.prototype.saveListSubscriptions = function(user, lists) {
      var query = {
        statements: [
          {
            statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                       "set u.listSubscriptions_imported = timestamp() ",
            parameters: {
              'user': {
                id_str: user.id_str
      } } } ] };
      for ( var list of lists ) {
        query.statements.push({
          statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                     "merge (f:twitterList { id_str: {list}.id_str }) " +
                     "merge (u)-[:owns]->(f) ",
          parameters: {
            'user': { id_str: list.user.id_str },
            'list': { id_str: list.id_str }
          }
        });
        query.statements.push({
          statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                     "merge (f:twitterList { id_str: {list}.id_str }) " +
                     "merge (u)-[:subscribes_to]->(f) ",
          parameters: {
            'user': { id_str: user.id_str },
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
            'list': models.filterList(list)
          }
        });
      }
      return this.queryRunner(query);
}

TwitterNeo4j.prototype.saveStatuses = function(user, statuses) {
  var query = {
    statements: [
      {
        statement: "merge (u:twitterUser { id_str: {user}.id_str }) " +
                   "set u.statuses_imported = timestamp() ",
        parameters: {
          'user': {
            id_str: user.id_str
  } } } ] };
      for ( var status of statuses ) {
        Array.prototype.push.apply(query.statements, this.saveStatusQuery(status));
        if (status.retweeted_status) {
          Array.prototype.push.apply(query.statements, this.saveStatusQuery(status.retweeted_status));
          this.saveStatusEntitiesQuery(status.retweeted_status);
        } else {
          this.saveStatusEntitiesQuery(status);
        }
      }
      return this.queryRunner(query);
}

TwitterNeo4j.prototype.saveStatusQuery = function(status) {
    var results = [];
    query.statements.push({
      statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                 "merge (f:twitterStatus { id_str: {status}.id_str }) " +
                 "merge (u)-[:posted]->(f) " +
                 "merge (y:twitterSource { value: {status}.source }) " +
                 "merge (f)-[:source]->(y) ",
      parameters: {
        'user': { id_str: status.user.id_str },
        'status': { id_str: status.id_str }
      }
    });
    if (status.in_reply_to_status_id_str) {
      results.push({
        statement: "match (u:twitterStatus { id_str: {reply}.id_str }) " +
                   "merge (f:twitterStatus { id_str: {status}.id_str }) " +
                   "merge (u)-[:reply_to]->(f) ",
        parameters: {
          'reply': { id_str: status.id_str },
          'status': { id_str: status.in_reply_to_status_id_str }
        }
      });
    }
    if (status.quoted_status_id_str) {
      results.push({
        statement: "merge (u:twitterStatus { id_str: {status}.id_str }) " +
                   "merge (f:twitterStatus { id_str: {quoted}.id_str }) " +
                   "merge (u)-[:quotes]->(f) ",
        parameters: {
          'status': { id_str: status.id_str },
          'quoted': { id_str: status.quoted_status_id_str }
        }
      });
    }
    results.push({
      statement: "merge (f:twitterStatus { id_str: {status}.id_str }) " +
                  "set " +
                  " y.id_str = {status}.id_str, " +
                  " y.created_at = {status}.created_at, " +
                  " y.text = {status}.text, " +
                  " y.retweet_count = {status}.retweet_count, " +
                  " y.possibly_sensitive = {status}.possibly_sensitive, " +
                  " y.lang = {status}.lang ",
      parameters: {
        'status': models.filterStatus(status)
      }
    });
    return results;
}

TwitterNeo4j.prototype.saveStatusUserEntityQuery = function(status, user_entities) {
    var results = user_entities.map(function (user_entity) {
      return {
        statement: "match (u:twitterUser { id_str: {user}.id_str }) " +
                   "merge (s:twitterStatus { id_str: {status}.id_str }) " +
                   "merge (u)-[:mentions]->(s) ",
        parameters: {
          'user': { id_str: user_entity.user.id_str },
          'status': { id_str: status.id_str }
        }
      };
    });
    return results;
}

TwitterNeo4j.prototype.saveStatusHashTagsQuery = function(status, hashtags) {
    var results = hashtags.map(function (hashtag) {
      return {
        statement: "match (u:twitterStatus { id_str: {status}.id_str }) " +
                   "merge (h:twitterHashtag { text: {hashtag}.text }) " +
                   "merge (u)-[:mentions]->(h) ",
        parameters: {
          'status': { id_str: status.id_str },
          'hashtag': { text: hashtag.text }
        }
      };
    });
    return results;
}

TwitterNeo4j.prototype.saveStatusURLSQuery = function(status, urls) {
    var results = urls.map(function (url) {
      return {
        statement: "match (u:twitterStatus { id_str: {status}.id_str }) " +
                   "merge (f:url { expanded_url: {url}.expanded_url }) " +
                   "merge (u)-[:mentions]->(f) ",
        parameters: {
          'status': { id_str: status.id_str },
          'url': { text: url.expanded_url }
        }
      };
    });
    return results;
}

TwitterNeo4j.prototype.saveStatusEntitiesQuery = function(status) {
  var results = [];
  if ((status.entities.hashtags.length || 0 ) > 0) {
    Array.prototype.push.apply(results, saveStatusHashTagsQuery(status, status.entities.hashtags));
  }
  if ((status.entities.user_mentions.length || 0 ) > 0) {
    Array.prototype.push.apply(results, saveStatusUserEntityQuery(status, status.entities.user_mentions));
  }
  return results;
}

TwitterNeo4j.prototype.getVIPNode = function() {
var _this = this;
  var query = {
    statements: [
      {
        statement: "merge (s:service { type: {type} }) " +
                   "return id(s) ",
        parameters: {
          'type': "VIP"
         } } ] };
  return this.queryRunner(query).then(function(results) {
    var IDVIP = results[0].data[0].row[0];
    _this.logger.info("VIP Node %j", IDVIP);
    return IDVIP;
  });
};

module.exports = TwitterNeo4j;
