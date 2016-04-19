var util = require('util');
var RSVP = require('rsvp');
var assert = require('assert');
var Twit = require('../../../lib/twit.js');
var _ = require('../../../lib/util.js');
const Neo4j = require('../../../lib/neo4j.js');
var model = require('../../../lib/twitter/models/user.js');
var Services = require('../../../lib/models/services.js');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "user",
  mvc: "api",
  function: "query",
  kue: "queryUser",
});
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var neo4j = new Neo4j(logger, metrics);
var T = new Twit(logger, metrics);
var serviceHandler = new Services(neo4j, T, logger, metrics);

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

function saveUsers(result) {
  return new RSVP.Promise(function(resolve, reject) {
    var users = result;
    logger.info("save");

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
        logger.info("committed");
        metrics.TxnFinished.increment();
        resolve(result);
      }
    });
  });
}

findVIPUsers();

function queryTemplate(depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body": "(! position.endNode().hasProperty('user_imported')) || (position.endNode().getProperty('user_imported') < 1460328669293)",
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

  function updateNodes(twitterUsers){
    var id_str_list = twitterUsers.map(function(m) {
      return m.data.id_str;
    });
    processed += id_str_list.length;
    logger.info("api queried %d", processed);
    return T.queries.user(id_str_list).then(saveUsers);
  }
  var operation = neo4j.operation('node/7307455/paged/traverse/node?pageSize=100&leaseTime=600', 'POST', queryTemplate(4) );
  runNextPage(operation, updateNodes);
}

function runNextPage(operation, cb){
  neo4j.call(operation, function(err, results, response) {
    if (!_.isEmpty(err)){
      if (err.neo4jError && err.neo4jError.fullname == 'org.neo4j.graphdb.NotFoundException') {
        logger.info("paging finished");
      } else {
        logger.error(err);
      }
      //reset
      setTimeout(findVIPUsers, 4 * 60 * 60 * 1000 );
    } else {
      if (response) {
        var next_page = response.replace(/.*\/db\/data\//, "");
        operation = neo4j.operation(next_page);
      }
      logger.trace("found %d nodes", results.length);
      cb(results).finally(function() {
        process.nextTick(runNextPage, operation, cb);
      });
    }
  });
}
