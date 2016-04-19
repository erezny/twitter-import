
var util = require('util');
var assert = require('assert');
var T = require('../../../lib/twit.js');
var _ = require('../../../lib/util.js');
var neo4j = require('../../../lib/neo4j.js');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friendsIDs",
  mvc: "api",
  function: "query",
  kue: "queryFriendsIDs",
});
var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

function saveFriends(user, friendsIDs, resolve, reject) {
    logger.debug("save");
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
}

function repeatQuery(user, query) {
    var cursor = cursor || "-1";
    var itemsFound = 0;
    logger.info("repeatQuery %s", user.screen_name);
    return new RSVP.Promise(function(resolve, reject) {
      function successHandler(results){
        var queryResults = results.query;
        var jobs = {};
        logger.trace(results);
        if (queryResults.ids){
          itemsFound += queryResults.ids.length;
          jobs.save = saveFriends( user, queryResults.ids);
        }
        if (queryResults.next_cursor_str !== "0"){
          jobs.query = query(user, queryResults.next_cursor_str );
          RSVP.hash(jobs)
          .then(successHandler, errorHandler);
        } else {
          logger.info();
          jobs.save.then(function() {
            resolve(itemsFound);
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

function queryFriendsIDs(user, cursor) {
  return new RSVP.Promise(function(resolve, reject) {
    //T.setAuth(tokens)
    logger.info("queryFriendsIDs %s %s", user.screen_name, cursor);
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

findVIPUsers();

function checkNodes(params){
  return "match (n:twitterUser) where id(n) in {nodes} " +
"  optional match friendships=(n)-[:follows]->(l:twitterUser)  " +
"  with n, size(collect (distinct l)) as friends_imported_count where friends_imported_count < n.friends_count " +
"return n, friends_imported_count" ;
}

function queryTemplate(depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body":
      " ( (! position.endNode().hasProperty('friends_imported')) || (position.endNode().getProperty('friends_imported') < 1460328669293) ) && " +
              "position.endNode().hasProperty('friends_count')  && position.endNode().getProperty('friends_count') <= 50000 && " +
              "((! position.endNode().hasProperty('protected')) || position.endNode().getProperty('protected') == false) ",
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
    } ],
    "max_depth": depth
  };
}

function findVIPUsers(){
  var processed = 0;
  var operation = neo4j.operation('node/7307455/paged/traverse/node?pageSize=1&leaseTime=6000', 'POST', queryTemplate(3) );
logger.trace();

  function updateNodes(nodes){
    return new RSVP.Promise(function(resolve, reject) {
      var nodeIDs = nodes.map(function(m) {
        return m.metadata.id;
      });
      var twitterUsers = nodes.map(function(m) {
        return m.data;
      });
      logger.trace();
      neo4j.query(checkNodes(), { nodes: nodeIDs }, function(err, results) {
        if (!_.isEmpty(err)){
          logger.error("neo4j find error %j",err);
          reject(err);
        }
        logger.trace(results);

        function success(num_found) {
          processed += 1;
          logger.info("queryFriendsIDs %s found %d friends", n.screen_name, num_found);
          logger.debug("processed %d nodes", processed);
        }
        function error(err){
          logger.error("error %j", err);
        }
        var jobs = results.map(function(node) {
          return repeatQuery( node.n , queryFriendsIDs)
          .then(success, error);
        });
        if (jobs.length === 0 ) {
          resolve();
        } else {
          RSVP.all(jobs).then(resolve, reject)
        }
      });
    });
  }

  runNextPage(operation, updateNodes);
}

function runNextPage(operation, cb){
  logger.trace("run");
  neo4j.call(operation, function(err, results, response) {
    if (!_.isEmpty(err)){
      if (err.neo4jError && err.neo4jError.fullname == 'org.neo4j.graphdb.NotFoundException') {
        logger.info("paging finished");
      } else {
        logger.error(err);
      }
      setTimeout(findVIPUsers,  4 * 60 * 60 * 1000 );
    } else {
      if (response) {
        var next_page = response.replace(/.*\/db\/data\//, "");
        operation = neo4j.operation(next_page);
      }
      cb(results).then(function() {
        process.nextTick(runNextPage, operation, cb);
      });
    }
  });
}
