
var util = require('util');
var _ = require('../util.js');
var RSVP = require('rsvp');
var Twit = require('../twit.js');
var neo4j;
var logger;
var metrics;
var RateLimiter = require('limiter').RateLimiter;

function checkFriendsCount(){
  return "match (n:twitterUser) where id(n) in {nodes} " +
"  optional match friendships=(n)-[:follows]->(l:twitterUser)  " +
"  with n, size(collect (distinct l)) as friends_imported_count " +
"  where friends_imported_count <> n.friends_count " +
"return n, friends_imported_count" ;
}

function checkListMembers(){
  return "match (n:twitterList) where id(n) in {nodes} " +
"  optional match members=(n)-[:includes]->(l:twitterUser)  " +
"  with n, size(collect (distinct l)) as members_imported_count " +
"  where members_imported_count <> n.member_count " +
"return n, members_imported_count" ;
}

function queryTemplate(property_type, since_timestamp, limit, depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body":
      util.format(" ( (! position.endNode().hasProperty('%s_imported')) || (position.endNode().getProperty('%s_imported') < %d) ) && " +
              "position.endNode().hasProperty('%s_count')  && position.endNode().getProperty('%s_count') <= %d && " +
              "((! position.endNode().hasProperty('protected')) || position.endNode().getProperty('protected') == false) ",
              property_type, property_type, since_timestamp, property_type, property_type, limit ),
      "language": "javascript"
    },
    "prune_evaluator": {
      "name": "none",
      "language": "builtin"
    },
    "uniqueness": "node_global",
    "relationships": [ {
      "direction": "out",
      "type": "includes"
    }, {
      "direction": "out",
      "type": "follows"
    }, {
      "direction": "in",
      "type": "oauth"
    } ],
    "max_depth": depth
  };
}

function userGenericTemplate(property_type, since_timestamp, depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body":
      util.format("position.endNode().hasProperty('user_imported') && ( (! position.endNode().hasProperty('%s_imported')) || (position.endNode().getProperty('%s_imported') < %d) ) && " +
              "((! position.endNode().hasProperty('protected')) || position.endNode().getProperty('protected') == false) ",
              property_type, property_type, since_timestamp ),
      "language": "javascript"
    },
    "prune_evaluator": {
      "name": "none",
      "language": "builtin"
    },
    "uniqueness": "node_global",
    "relationships": [ {
      "direction": "out",
      "type": "includes"
    }, {
      "direction": "out",
      "type": "follows"
    }, {
      "direction": "in",
      "type": "oauth"
    } ],
    "max_depth": depth
  };
}

function queryUsers(since_timestamp, depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body": util.format("(! position.endNode().hasProperty('user_imported')) || (position.endNode().getProperty('user_imported') < %d)", since_timestamp),
      "language": "javascript"
    },
    "prune_evaluator": {
      "name": "none",
      "language": "builtin"
    },
    "uniqueness": "node_global",
    "relationships": [ {
      "direction": "out",
      "type": "includes"
    }, {
      "direction": "out",
      "type": "follows"
    }, {
      "direction": "in",
      "type": "oauth"
    } ],
    "max_depth": depth
  };
}

function listMembersTemplate(property_type, since_timestamp, depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body":
      util.format(" position.endNode().hasProperty('subscriber_count')  && " +
              "( (! position.endNode().hasProperty('%s_imported')) || (position.endNode().getProperty('%s_imported') < %d) ) && " +
              "((! position.endNode().hasProperty('mode')) || position.endNode().getProperty('mode') == 'public') ",
              property_type, property_type, since_timestamp ),
      "language": "javascript"
    },
    "prune_evaluator": {
      "body": "position.endNode().hasProperty('subscriber_count')  ",
      "language": "javascript"
    },
    "uniqueness": "node_global",
    "relationships": [ {
      "direction": "out",
      "type": "includes"
    }, {
      "direction": "out",
      "type": "follows"
    }, {
      "direction": "out",
      "type": "owns"
    }, {
      "direction": "in",
      "type": "oauth"
    } ],
    "max_depth": depth
  };
}

function checkNodeAndRun(checkNodeQuery, nodes, runFunction){
  return new RSVP.Promise(function(resolve, reject) {
    var nodeIDs = nodes.map(function(m) {
      return m.metadata.id;
    });
    var twitterUsers = nodes.map(function(m) {
      return m.data;
    });
    logger.trace();
    neo4j.query(checkNodeQuery, { nodes: nodeIDs }, function(err, results) {
      if (!_.isEmpty(err)){
        logger.error("neo4j find error %j",err);
        reject(err);
      } else {
        logger.trace(results);

        var jobs = results.map(function(node) {
          return runFunction(node);
        });
        if (jobs.length === 0 ) {
          resolve();
        } else {
          RSVP.all(jobs).then(resolve, reject);
        }
      }
    });
  });
}

function runGraphTraversal(startNode, pageSize, traversal_query, page_callback){
    var url = util.format('node/%s/paged/traverse/node?pageSize=%d&leaseTime=36000', startNode, pageSize);
    logger.trace('url: %s', url);
    var operation = neo4j.operation(url, 'POST', traversal_query );
    return traverse (page_callback, operation);
}

var traverseLimiter = new RateLimiter(1, 3 * 1000);
function traverse(runPerPage, operation){
  return new RSVP.Promise(function(resolve, reject) {
    var keep_running = new _.CatchSigterm();
    runNextPage(operation);

    function runNextPage(operation){
      logger.trace("run");
      traverseLimiter.removeTokens(1, function(err, remainingRequests) {
        neo4j.call(operation, function(err, results, response) {
          if (!_.isEmpty(err)){
            logger.error(err);
            if (err.neo4jError && err.neo4jError.fullname == 'org.neo4j.graphdb.NotFoundException') {
              resolve();
            } else {
              reject(err);
            }
          } else {
            if (response) {
              var next_page = response.replace(/.*\/db\/data\//, "");
              operation = neo4j.operation(next_page);
            }
            logger.trace(results);
            runPerPage(results).then(function() {
              if (keep_running.get()){
                process.nextTick(runNextPage, operation);
              } else {
                logger.info("shutdown");
                resolve();
              }
            }, function(err) {
              logger.error(err);
              if (err.code == "ECONNRESET") {
                resolve(); //Expect next attempt to work properly
              } else {
                reject(err);
              }
            });
          }
        });
      });
    }
  });
}

function importFriendsIDs(T, startNode){
  var checkNodeQuery = checkFriendsCount();
  var since_timestamp = (new Date).getTime() -  24 * 60 * 60 * 1000 ;
  var traversal_query = queryTemplate("friends",since_timestamp, 25000, 3) ;
  var apiFunction = T.queries.friendsIDs;
  var saveFunction = neo4j.twitter.saveFriendsIDs;
  var processed = 0;
  var runFunction = function(node) {
    logger.debug("queryFriendsIDs %s, currently %d/%d", node.n.screen_name, node.friends_imported_count, node.n.friends_count);

    var saved = 0;
    var saveFunction = function(user, results) {
      logger.trace(results);
      if (results.ids.length > 0){
        logger.debug("found %d friends of user %s", results.ids.length, user.screen_name );
        saved += results.ids.length;
      }
      return neo4j.twitter.saveFriendsIDs(user, results.ids);
    }
    return neo4j.twitter.resetFriends(node.n).then(function() {
      return T.queries.pagedAPIQuery( node.n , apiFunction, saveFunction).then(function() {
        processed += 1;
        logger.debug("processed %d nodes", processed);
        logger.info("found %d friends of user %s", saved, node.n.screen_name );
      }).catch( function(err) {
        if (err.message == 'Not authorized.'){
          logger.info("found %d friends of user %s", saved, node.n.screen_name );
        } else {
          logger.error("unknown Error querying friends of %s: %j", node.n.screen_name, err);
        }
      });
    });
  };
  var checkFunction = function(nodes) {
    return checkNodeAndRun(checkNodeQuery, nodes, runFunction);
  };
  return runGraphTraversal(startNode, 1, traversal_query, checkFunction);
}

function importUsers(T, startNode){
  var since_timestamp = (new Date).getTime() - 7 * 24 * 60 * 60 * 1000 ;
  var traversal_query = queryUsers(since_timestamp, 4) ;
  var apiFunction = T.queries.users;
  var saveFunction = neo4j.twitter.saveUsers;
  var processed = [ 0,0 ];
  var runFunction = function(nodes) {
    var id_str_list = nodes.map(function(m) {
      return m.data.id_str;
    });
    return T.queries.users(id_str_list).then(saveFunction).then(function() {
      processed[0] += +id_str_list.length || 0;
      if (processed[0] >= processed[1]) {
        processed[1] = processed[0] + 1000;
        logger.info("queried %d users", processed[0] );
      }
    }).catch(function(err) {
      var msg = err.message || "";
      if (msg == 'No user matches for specified terms.') {
        //delete all queried users\
        logger.debug(msg);
      } else {
        logger.error("unknown Error", err);
      }
    });
  };
  return runGraphTraversal(startNode, 100, traversal_query, runFunction);
}

function importUserListOwnership(T, startNode){
  var checkNodeQuery = checkFriendsCount();
  var since_timestamp = (new Date).getTime() - 7 * 24 * 60 * 60 * 1000 ;
  var traversal_query = userGenericTemplate("listOwnership",since_timestamp, 3);
  var apiFunction = T.queries.userListOwnership;
  var saveFunction = neo4j.twitter.saveLists;
  var processed = 0;
  var runFunction = function(nodes) {
    var user = nodes[0].data;
    logger.debug("queryUserListOwnership %s", user.screen_name);
    return neo4j.twitter.resetListOwnerships(user).then(function() {
      return apiFunction( user ).then(function(results) {
        logger.trace(results);
        if (results.lists.length > 0){
          logger.info("found %d lists owned by %s", results.lists.length, user.screen_name )
        }
        return saveFunction(user, results.lists).then(function() {
          processed += 1;
          logger.debug("processed %d nodes", processed);
        });
      });
    }).catch(function(err) {
      logger.error("unknown Error", err);
    });
  };
  return runGraphTraversal(startNode, 1, traversal_query, runFunction);
}

function importListMembers(T, startNode){
  var checkNodeQuery = checkListMembers();
  var since_timestamp = (new Date).getTime() - 7 * 24 * 60 * 60 * 1000 ;
  var traversal_query = listMembersTemplate("members", since_timestamp, 3);
  var apiFunction = T.queries.listMembers;
  var processed = 0;
  var runFunction = function(nodes) {
    var list = nodes.n;
    logger.debug("queryListMembers %s", list.full_name);
    var saved = 0;
    var saveFunction = function(list, results) {
      logger.trace(results);
      if (results.users.length > 0){
        logger.debug("found %d users in list %s", results.users.length, list.full_name );
        saved += results.users.length;
      }
      return neo4j.twitter.saveListMembers(list, results.users)
    }
    return neo4j.twitter.resetListMembers(list).then(function() {
      return T.queries.pagedAPIQuery( list , apiFunction, saveFunction).then(function() {
        processed += 1;
        logger.debug("processed %d nodes", processed);
        logger.info("found %d users in list %s", saved, list.full_name );
      });
    }).catch(function(err) {
      if (err.message == 'Sorry, that page does not exist.'){
        //False positives exist, perhaps don't believe unless verified
        logger.error("List doesn't exist");
      } else {
        logger.error("unknown Error", err);
      }
    });
  };
  var checkFunction = function(nodes) {
    return checkNodeAndRun(checkNodeQuery, nodes, runFunction);
  };
  return runGraphTraversal(startNode, 1, traversal_query, checkFunction);
}

function importUserListSubscriptions(T, startNode){
  var checkNodeQuery = checkFriendsCount();
  var since_timestamp = (new Date).getTime() - 7 * 24 * 60 * 60 * 1000 ;
  var traversal_query = userGenericTemplate("listSubscriptions",since_timestamp, 3);
  var apiFunction = T.queries.userListSubscriptions;
  var saveFunction = neo4j.twitter.saveListSubscriptions;
  var processed = 0;
  var runFunction = function(nodes) {
    var saved = 0;
    var user = nodes[0].data;
    logger.debug("ListSubscriptions %s", user.screen_name);
    return neo4j.twitter.resetListSubscriptions(user).then(function() {
      return apiFunction( user ).then(function(results) {
        logger.trace(results);
        if (results.lists.length > 0){
          logger.debug("found %d lists subscribed to by %s", results.lists.length, user.screen_name );
          saved += results.lists.length;
        }
        return saveFunction(user, results.lists).then(function() {
          processed += 1;
          logger.debug("processed %d nodes", processed);
          if (saved > 0){
            logger.info("found %d lists subscribed to by %s", saved, user.screen_name );
          }
        });
      });
    }).catch(function(err) {
      if (err.message == 'Sorry, that page does not exist.'){
        //False positives exist, perhaps don't believe unless verified
        logger.error("List doesn't exist");
      } else {
        logger.error("unknown Error", err);
      }
    });
  };
  return runGraphTraversal(startNode, 1, traversal_query, runFunction);
}

function runAppImports() {
  var T = new Twit(logger, metrics);
  var startNode = '7307455';
  return [
    importFriendsIDs(T, startNode),
    importUsers(T, startNode),
    importUserListOwnership(T, startNode),
    importListMembers(T, startNode),
    importUserListSubscriptions(T, startNode)
  ];
}

function runUserImports(oauth) {
  var userAuth = {
    acces_token: oauth.token,
    acces_token_secret: oauth.secret
  };
  var T = new Twit(logger, metrics, userAuth);
  var startNode = util.format("%d", oauth.id);
  logger.trace(startNode);
  return [
    importFriendsIDs(T, startNode),
    importUsers(T, startNode),
    importUserListOwnership(T, startNode),
    importListMembers(T, startNode),
    importUserListSubscriptions(T, startNode)
  ];
}

function runAllUserImports() {
  return new RSVP.Promise(function(resolve, reject) {
    findOuathInfos().then(function (oauths) {
      logger.trace(oauths);
      var jobs = runUserImports(oauth[0]);
      resolve(jobs);
    });
  });
}
function findOuathInfos() {
  return new RSVP.Promise(function(resolve, reject) {
    neo4j.find({}, true, 'twitterOAuth', function(err, nodes) {
      if (err) {
        logger.error(err);
        reject(err);
      } else {
        resolve(nodes);
      }
    });
  });
}

module.exports = function(_neo4j, _logger, _metrics) {
  neo4j = _neo4j;
  logger = _logger;
  metrics = _metrics;
  process.setMaxListeners(15);

  return {
    traverse: traverse,
    importFriendsIDs: importFriendsIDs,
    importUsers: importUsers,
    importUserListOwnership: importUserListOwnership,
    importListMembers: importListMembers,
    importUserListSubscriptions: importUserListSubscriptions,
    runAppImports: runAppImports,
    runAllUserImports: runAllUserImports
  };
};
