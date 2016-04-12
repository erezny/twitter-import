
var util = require('util');
var T = require('../../../lib/twit.js');
var _ = require('../../../lib/util.js');
var assert = require('assert');

const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friendsIDs",
  mvc: "api",
  function: "query",
  kue: "queryFriendsIDs",
});
var neo4j = require('../../../lib/neo4j.js');
var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

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
//
// queue.process('queryFriendsIDs', function(job, done) {
//   //  logger.info("received job");
//   logger.trace("queryFriendsIDs received job %j", job);
//   metricStart.increment();
//   var user = job.data.user;
//   var cursor = job.data.cursor || "-1";
//   var promise = null;
//   job.data.numReceived = job.data.numReceived || 0;
//   if (cursor == "-1"){
//     metricFreshQuery.increment();
//     promise = checkFriendsIDsQueryTime(job.data.user)
//   } else {
//     metricContinuedQuery.increment();
//     promise = new Promise(function(resolve) { resolve(); });
//   }
//   promise.then(function() {
//     return queryFriendsIDs(user, cursor, job)
//   }, function(err) {
//     done();
//   })
//   .then(saveFriends)
//   .then(updateFriendsIDsQueryTime)
//   .then(function(result) {
//     metricFinish.increment();
//     done();
//   }, function(err) {
//     logger.error("queryFriendsIDs error: %j %j", job, err);
//     metricQueryError.increment();
//     done(err);
//   });
//
// });

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
        metricTxnError.increment();
        reject(err);
      } else {
        logger.info("committed");
        metricTxnFinished.increment();
        resolve();
      }
    });

}

function queryFriendsIDs(user, cursor) {

  var cursor = cursor || "-1";
  return new Promise(function(resolve, reject) {
    //T.setAuth(tokens)
    logger.debug("queryFriendsIDs");
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('friends/ids', { user_id: user.id_str, cursor: cursor, count: 5000, stringify_ids: true }, function (err, data)
      {
        if (!_.isEmpty(err)){
          if (err.message == "Not authorized."){
            //queue.create('markUserPrivate', { user: user } ).removeOnComplete(true).save();
            resolve({ user: user, list: [] });
            return;
          } else if (err.message == "User has been suspended."){
            //queue.create('markUserSuspended', { user: user } ).removeOnComplete(true).save();
            resolve({ user: user, list: [] });
            return;
          } else {
            logger.error("twitter api error %j %j", user, err);
            metricApiError.increment();
            reject({ message: "unknown twitter error", err: err });
            return;
          }
        }
        logger.trace("Data %j", data);
        logger.debug("queryFriendsIDs twitter api callback");
        logger.info("queryFriendsIDs %s found %d friends", user.screen_name, data.ids.length);

        // if (data.next_cursor_str !== '0'){
        //   //var numReceived = job.data.numReceived + data.ids.length;
        //   //queue.create('queryFriendsIDs', { user: user, cursor: data.next_cursor_str, numReceived: numReceived }).attempts(5).removeOnComplete( true ).save();
        // }
        metricApiFinished.increment();
        saveFriends( user, data.ids, resolve, reject);
      });
    });
  });
}

setInterval(findVIPUsers, 7 * 24 * 60 * 60 * 1000 );
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
              "position.endNode().hasProperty('friends_count')  && position.endNode().getProperty('friends_count') <= 5000 && " +
              "(! position.endNode().hasProperty('protected') || position.endNode().getProperty('protected')) ",
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

  function updateNodes(nodes){
    return new Promise(function(resolve, reject) {
      var nodeIDs = nodes.map(function(m) {
        return m.metadata.id;
      });
      var twitterUsers = nodes.map(function(m) {
        return m.data;
      });
      //logger.info(twitterUsers);

      neo4j.query(checkNodes(), { nodes: nodeIDs }, function(err, results) {
        if (!_.isEmpty(err)){
          logger.error("neo4j find error %j",err);
          reject();
        }
        logger.info(results);
        var jobs = [];
        for (var n of results) {
          jobs.push( queryFriendsIDs(n.n, "-1"));
        }
        processed += nodeIDs.length;
        logger.info("processed %d nodes", processed);
        RSVP.allSettled(jobs).then(resolve);
      });
    });
  }
  var operation = neo4j.operation('node/7307455/paged/traverse/node?pageSize=1&leaseTime=600', 'POST', queryTemplate(3) );
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
    } else {
      if (response) {
        var next_page = response.replace(/.*\/db\/data\//, "");
        operation = neo4j.operation(next_page);
      }
      logger.debug("found %d nodes", results.length);
      cb(results).then(function() {
        process.nextTick(runNextPage, operation, cb);
      });
    }
  });
}
