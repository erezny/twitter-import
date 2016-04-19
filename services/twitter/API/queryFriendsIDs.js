
var util = require('util');
var assert = require('assert');
var Twit = require('../../../lib/twit.js');
var _ = require('../../../lib/util.js');
const Neo4j = require('../../../lib/neo4j.js');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friendsIDs",
  mvc: "api",
  function: "query",
  kue: "queryFriendsIDs",
});
var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'debug'
} );
var neo4j = new Neo4j(logger, metrics);
var T = new Twit(logger, metrics);

function repeatQuery(user, query) {
    logger.debug("start %s", user.screen_name);
    return new RSVP.Promise(function(resolve, reject) {
      var cursor = cursor || "-1";
      var itemsFound = 0;

      function successHandler(results){
        var queryResults = results.query;
        var jobs = {};
        logger.trace(results);
        if (queryResults.ids && queryResults.ids.length > 0){
          itemsFound += queryResults.ids.length;
          logger.debug(itemsFound);
          jobs.save = neo4j.twitter.saveFriendsIDs( user, queryResults.ids);
        } else {
          jobs.save = new RSVP.Promise(function(done) {done();});
        }
        if (queryResults.next_cursor_str !== "0"){
          jobs.query = query(user, queryResults.next_cursor_str );
          RSVP.hash(jobs)
          .then(successHandler, errorHandler);
        } else {
          jobs.save.then(function() {
            logger.info("queryFriendsIDs %s found %d friends", user.screen_name, itemsFound);
            resolve(user);
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

findVIPUsers();

function checkFriendsCount(){
  return "match (n:twitterUser) where id(n) in {nodes} " +
"  optional match friendships=(n)-[:follows]->(l:twitterUser)  " +
"  with n, size(collect (distinct l)) as friends_imported_count " +
"  where friends_imported_count < n.friends_count " +
"return n, friends_imported_count" ;
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
    } ],
    "max_depth": depth
  };
}

var keep_running = 1;

function findVIPUsers(){
  var processed = 0;
  var operation = neo4j.operation('node/7307455/paged/traverse/node?pageSize=1&leaseTime=6000', 'POST', queryTemplate("friends",1461091867259, 50000, 3) );
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
      neo4j.query(checkFriendsCount(), { nodes: nodeIDs }, function(err, results) {
        if (!_.isEmpty(err)){
          logger.error("neo4j find error %j",err);
          reject(err);
        }
        logger.trace(results);

        function success(user) {
          processed += 1;
          logger.debug("processed %d nodes", processed);
        }
        function error(err){
          logger.error("error %j", err);
        }
        var jobs = results.map(function(node) {
          logger.info("queryFriendsIDs %s, expecting %d", node.n.screen_name, node.n.friends_count);
          return repeatQuery( node.n , T.queries.friendsIDs)
          .then(success, error);
        });
        if (jobs.length === 0 ) {
          resolve();
        } else {
          RSVP.all(jobs).then(resolve, reject);
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
        if (keep_running){
          process.nextTick(runNextPage, operation, cb);
        } else {
          logger.info("shutdown");
          process.exit(0);
        }
      }, function(err) {
        logger.error(err);
      });
    }
  });
}

function interrupt_running( sig ) {
  logger.info("prepare for shutdown");
  keep_running = 0;
}

process.once( 'SIGTERM', interrupt_running);
process.once( 'SIGINT', interrupt_running);
