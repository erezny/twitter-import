
var util = require('util');
var assert = require('assert');const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "relationships",
  mvc: "model",
  function: "count",
});
var queue = require('../../../lib/kue.js');

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

setInterval(countRelationships, 5 * 60 * 1000 );

function queryTemplate(params){
  return util.format(
"match (n:twitterUser) where n.analytics_updated is null or n.analytics_updated < 10 with n limit 100000 " +
"  optional match followerships=(n)<-[:follows]-(m:twitterUser)  " +
"    where not m.screen_name is null " +
"    with n, size(collect( followerships)) as followers " +
"  optional match friendships=(n)-[:follows]->(l:twitterUser)  " +
"    where not l.screen_name is null " +
"  with n, followers, size(collect (friendships)) as friends " +
"set     n.followers_imported_count = followers, " +
"        n.friends_imported_count = friends, " +
"  	 	  n.analytics_updated = 10 " +
"with n return sum(n.friends_imported_count) as friends_imported, sum(n.friends_count) as friends_count, " +
"sum(n.followers_imported_count) as followers_imported, sum(n.followers_count) as followers_count"
  );
}

function countRelationships(){
  neo4j.queryRaw(queryTemplate(), function(err, results) {
    if (err){
      logger.error("neo4j find error %j",err);
      return;
    }
    logger.trace("neo4j found %j", results);
  });
}
