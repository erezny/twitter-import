
var util = require('util');
var _ = require('../util.js');
var RSVP = require('rsvp');
var neo4j;
var T;
var logger;
var metrics;

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

function traverseVIPS(checkNodeQuery, operation, runFunction){
  var processed = 0;
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
      neo4j.query(checkNodeQuery, { nodes: nodeIDs }, function(err, results) {
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
          return runFunction(node)
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

function importFriendsIDs(){
  var checkNodeQuery = checkFriendsCount();
  var since_timestamp = 1461091867259;
  var operation = neo4j.operation('node/7307455/paged/traverse/node?pageSize=1&leaseTime=6000', 'POST', queryTemplate("friends",since_timestamp, 50000, 3) );
  var apiFunction = T.queries.friendsIDs;
  var saveFunction = neo4j.twitter.saveFriendsIDs;
  var runFunction = function(node) {
    logger.info("queryFriendsIDs %s, currently %d/%d", node.n.screen_name, node.friends_imported_count, node.n.friends_count);
    return T.queries.repeatIDQuery( node.n , apiFunction, saveFunction)
  };
  traverseVIPS(checkNodeQuery, operation, runFunction );
}

var keep_running = 1;
function interrupt_running( sig ) {
  logger.info("prepare for shutdown");
  keep_running = 0;
}

module.exports = function(_neo4j, _T, _logger, _metrics) {
  neo4j = _neo4j;
  T = _T;
  logger = _logger;
  metrics = _metrics;

  process.once( 'SIGTERM', interrupt_running);
  process.once( 'SIGINT', interrupt_running);
  return {
    traverseVIPS: traverseVIPS,
    importFriendsIDs: importFriendsIDs
  };
};
