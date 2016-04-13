
var util = require('util');
var assert = require('assert');
var _ = require('../../../lib/util.js');
var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var neo4j = require('../../../lib/neo4j.js');

findVIPUsers();

function queryFriends(depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body": util.format("position.length()==%d", depth),
      "language": "javascript"
    },
    "prune_evaluator": {
      "body": util.format("position.length()>%d", depth),
      "language": "javascript"
    },
    "uniqueness": "node_global",
    "relationships": [ {
      "direction": "out",
      "type": "follows"
    } ],
    "max_depth": depth
  };
}

function queryVIPs(){
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
    } ],
    "max_depth": 1
  };
}

function findVIPUsers(){

  function traverseVIPFriends(vip){
    return new RSVP.Promise(function(resolve, reject) {
      vip = vip[0];
      logger.info('traversing %d friends of VIP: %s', vip.data.friends_count, vip.data.screen_name);
      var nodeStr = util.format('%d', vip.metadata.id);
      function traverse(distance) {
        return runGraphTraversal(nodeStr, 10000, queryFriends(distance), mergeDistancesTemplate(vip, distance));
      }
      traverse(1).then(function() {
        return traverse(2);
      }).then(function() {
        return traverse(3);
      })
      .then(resolve,reject);
    });
  }

  runGraphTraversal('7307455', 1, queryVIPs(), traverseVIPFriends)
  .then(function() {
    logger.info('finished');
  }).catch(function(err) {
    logger.error(err);
  });
}

function mergeDistancesTemplate(vip, distance){
  logger.info('%s', vip.data.screen_name);

  function updateTemplate() {
    return "match (vip:twitterUser) where id(vip) = {vip}.id \n" +
    "merge (vip)<-[:vip]-(dvip:analytics{type:\"vipDistance\"}) \n" +
    "with dvip \n" +
    "match (users:twitterUser) where id(users) in {users} \n" +
    "merge (dvip)<-[r:distance]-(users) \n" +
    "with r \n" +
    "set r.value = {distance}";
  }

  function mergeDistances(twitterUsers) {
      var nodeIDs = twitterUsers.map(function(m) {
        return m.metadata.id;
      });
      return runQuery( { vip: vip.metadata, users: nodeIDs, distance: distance }, updateTemplate());
  }
  return mergeDistances;

}

function runGraphTraversal(startNode, pageSize, traversal, node_callback){
  return new RSVP.Promise(function(resolve, reject) {
    var processed = 0;
    var url = util.format('node/%s/paged/traverse/node?pageSize=%d&leaseTime=36000', startNode, pageSize);
    logger.info('url: %s', url);
    var operation = neo4j.operation(url, 'POST', traversal );
    runNextPage(operation, node_callback, resolve,reject);
  });
}

function runQuery(data, query){
  return new RSVP.Promise(function(resolve, reject) {
    neo4j.queryRaw(query, data, function(err, results) {
      if (!_.isEmpty(err)){
        logger.error(err);
        reject(err);
      } else {
        resolve(results);
      }
    });
  });
}

function runNextPage(operation, cb, resolve, reject){
    neo4j.call(operation, function(err, results, response) {
      if (!_.isEmpty(err)){
        if (err.neo4jError && err.neo4jError.fullname == 'org.neo4j.graphdb.NotFoundException') {
          resolve();
        } else {
          logger.error(error);
          reject(err);
        }
      } else {
        if (response) {
          var next_page = response.replace(/.*\/db\/data\//, "");
          operation = neo4j.operation(next_page);
        }
        cb(results).finally(function() {
          process.nextTick(runNextPage, operation, cb, resolve, reject);
        });
      }
    });
}
