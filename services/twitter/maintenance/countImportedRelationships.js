
var util = require('util');
var _ = require('../../../lib/util.js');
var assert = require('assert');const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "relationships",
  mvc: "model",
  function: "count",
});
var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var neo4j = require('../../../lib/neo4j.js');

setInterval(findVIPUsers, 24 * 60 * 60 * 1000 );
findVIPUsers();

function updateTemplate(params){
  return "match (n:twitterUser) where id(n) in {nodes} " +
"  optional match followerships=(n)<-[:follows]-(m:twitterUser)  " +
"    where not m.screen_name is null " +
"    with n, size(collect( followerships)) as followers " +
"  optional match friendships=(n)-[:follows]->(l:twitterUser)  " +
"    where not l.screen_name is null " +
"  with n, followers, size(collect (friendships)) as friends " +
"set     n.followers_imported_count = followers, " +
"        n.friends_imported_count = friends, " +
"  	 	  n.analytics_updated = 10 " +
"with n return n" ;
}

var friends_imported_count = 0,
    friends_count = 0,
    followers_imported_count = 0,
    followers_count = 0,
    found = 0;

function countRelationships(twitterUsers){
  return new Promise(function(resolve, reject) {
    var nodeIDs = twitterUsers.map(function(m) {
      return m.metadata.id;
    });
    neo4j.query(updateTemplate(), { nodes: nodeIDs }, function(err, results) {
      if (!_.isEmpty(err)){
        logger.error("neo4j find error %j",err);
        reject();
      }
      logger.info("neo4j found %j", results.map(function(m) {return m.screen_name; }));
      resolve();
    });
  });
}
function queryTemplate(depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "name": "all_but_start_node",
      "language": "builtin"
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
  logger.info("run");
  var operation = neo4j.operation('node/7307455/paged/traverse/node?pageSize=5&leaseTime=6000', 'POST', queryTemplate(4) );
  runNextPage(operation, countRelationships);
}

function runNextPage(operation, cb){
  neo4j.call(operation, function(err, results, response) {
    if (!_.isEmpty(err)){
      if (err.neo4jError.fullname == 'org.neo4j.graphdb.NotFoundException') {
        logger.info("paging finished");
      } else {
        logger.error(err);
      }
    } else {
      if (response) {
        var next_page = response.replace("http://192.168.1.100:25066/db/data/", "");
        operation = neo4j.operation(next_page);
      }
      logger.info("found %d nodes", results.length);
      cb(results).then(function() {
        process.nextTick(runNextPage, operation, cb);
      });
    }
  });
}
