
// #refactor:10 write queries
var util = require('util');
var T = require('../../../lib/twit.js');
var _ = require('../../../lib/util.js');
var assert = require('assert');

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "user",
  mvc: "api",
  function: "query",
  kue: "queryUser",
});

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 60) * 15 * 60 * 1000);
var model = require('../../../lib/twitter/models/user.js');
var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var neo4j = require('../../../lib/neo4j.js');

const metricRelSaved = metrics.counter("rel_saved");
const metricRelError = metrics.counter("rel_error");
const metricStart = metrics.counter("start");
const metricFreshQuery = metrics.counter("freshQuery");
const metricContinuedQuery = metrics.counter("continuedQuery");
const metricFinish = metrics.counter("finish");
const metricQueryError = metrics.counter("queryError");
const metricRepeatQuery = metrics.counter("repeatQuery");
const metricUpdatedTimestamp = metrics.counter("updatedTimestamp");
const metricApiError = metrics.counter("apiError");
const metricApiFinished = metrics.counter("apiFinished");
const metricTxnFinished = metrics.counter("txnFinished");
const metricTxnError = metrics.counter("txnError");

function queryUser(id_str_list) {
  return new Promise(function(resolve, reject) {
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.post('users/lookup', { user_id: id_str_list }, function(err, data)
      {
        if (!_.isEmpty(err)){
            logger.error("twitter api error %j %j", id_str_list, err);
            metrics.counter("apiError").increment(count = 1, tags = { apiError: err.code, apiMessage: err.message });
            reject({ err: err, message: "twitter api error" });
            return;
        }
        resolve(data);
      });
    });
  });
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

function saveUsers(result) {
  return new Promise(function(resolve, reject) {
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
        metricTxnError.increment();
        reject(err);
      } else {
        logger.info("committed");
        metricTxnFinished.increment();
        resolve(result);
      }
    });
  });
}

setInterval(findVIPUsers, 24 * 60 * 60 * 1000 );
findVIPUsers();

function queryTemplate(depth){
  return {
    "order": "breadth_first",
    "return_filter": {
      "body": "! position.endNode().hasProperty('user_imported') || position.endNode().getProperty('user_imported') < timestamp() - (60 * 60 * 24 * 1000) ",
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
  var processed = 0;

  function updateNodes(twitterUsers){
    var id_str_list = twitterUsers.map(function(m) {
      return m.data.id_str;
    });
    processed += id_str_list.length;
    logger.info("api queried %d", processed);
    return queryUser(id_str_list).then(saveUsers);
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
    } else {
      if (response) {
        var next_page = response.replace(/.*\/db\/data\//, "");
        operation = neo4j.operation(next_page);
      }
      logger.trace("found %d nodes", results.length);
      cb(results).then(function() {
        process.nextTick(runNextPage, operation, cb);
      });
    }
  });
}
