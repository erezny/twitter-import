'use strict';
var RSVP = require('rsvp');
const Neo4j = require('../../lib/neo4j.js');
var Services = require('../../lib/models/services.js');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
const metrics = require('../../lib/crow.js').init("resetEnvironment", {
  api: "twitter",
  function: "maintenance",
}, logger);
var neo4j = new Neo4j(logger, metrics);

var removeAllProperty = function(labelName, propertyName) {
  var limit = 100000;
  var query = {
    statements: [
      {
        statement:
        `match (n:${labelName})
        using index n:${labelName}(${propertyName})
        where has(n.${propertyName})
        with n limit ${limit}
        remove  n.${propertyName}
        with n return count(n)`,
       } ] };
  return new RSVP.Promise(function(resolve, reject) {
    var countRemoved = 0;
    function run(){
      neo4j.queryRunner(query).then(function(results) {
        var count = results[0].data[0].row[0];
        countRemoved += count;
        logger.info("%d removed / %d Total removed / ? remaining", count, countRemoved);
        if (count < limit ){
          resolve();
        } else {
          process.nextTick(run);
        }
      });
    }
    run();
  });
};

var removeAllNodesExceptVIP = function() {
  var limit = 100000;
  var query = {
    statements: [
      { statement:
        `match (n:twitterUser)
        where (not n.screen_name in {users} ) or (not has(n.screen_name))
        with n limit ${limit}
        optional match (n)-[r]-()
        delete n,r
        with n return count(n)`,
        parameters: {
          'users': [ "erezny", "Tuggernuts23" ],
        }
      },
      { statement:
        `match (n:twitterList)
        with n limit ${limit}
        optional match (n)-[r]-()
        delete n,r
        with n return count(n)`,
        parameters: {
        }
       } ] };
  return new RSVP.Promise(function(resolve, reject) {
    var countRemoved = 0;
    function run(){
      neo4j.queryRunner(query).then(function(results) {
        var count = results[0].data[0].row[0] + results[1].data[0].row[0];
        countRemoved += count;
        logger.info("%d removed / %d Total removed / ? remaining", count, countRemoved);
        if (count < limit ){
          resolve();
        } else {
          process.nextTick(run);
        }
      });
    }
    run();
  });
};

removeAllNodesExceptVIP()
.then(function() {
  return removeAllProperty('twitterUser', 'user_imported');
}).then(function() {
  return removeAllProperty('twitterUser', 'friends_imported');
}).then(function() {
  return removeAllProperty('twitterUser', 'followers_imported');
}).then(function() {
  return removeAllProperty('twitterUser', 'listOwnership_imported');
}).then(function() {
  return removeAllProperty('twitterUser', 'listSubscriptions_imported');
}).then(function() {
  return removeAllProperty('twitterList', 'members_imported');
}).then(function() {
  return removeAllProperty('twitterList', 'list_imported');
}).then(function() {
  process.exit(0);
}).catch(function() {
  process.exit(1);
});
