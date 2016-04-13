
var util = require('util');
var assert = require('assert');

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'debug'
} );
var neo4j = require('../../lib/neo4j.js');

function deleteProperty(property){
  return new Promise(function(resolve, reject) {
    var query = util.format("match (n:twitterUser) " +
          "using index n:twitterUser(%s) where exists(n.%s) " +
          "with n limit 500000 remove n.%s return count(*) as num", property, property, property);
    function runQuery(){
      return new Promise(function(resolve, reject) {
        neo4j.queryRaw(query, function(err, results, something) {
          if (err){
            logger.error("neo4j find error %j",err);
            reject("error");
            return;
          }
          logger.info("deleted %d", results.data[0][0]);
          resolve(results.data[0][0]);
        });
      });
    }
    runQuery().then(checkRestart);

    function checkRestart(num){
      if (num > 0){
        process.nextTick(run);
      } else {
        resolve();
      }
    }

    function run(){
        runQuery().then(checkRestart);
    }
  });
}

function deleteProperties(properties){
  var job;
  for (var property of properties){
    //umm
  }
}
deleteProperty("friends_imported_count");
