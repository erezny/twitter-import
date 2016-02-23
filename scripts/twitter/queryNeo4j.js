
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'debug'
} );
var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

neo4j.queryRaw("MATCH (x:twitterUser{screen_name:\"erezny\"})-[r:follows]->(n) RETURN n", function(err, results) {
  if (err){
    logger.error("neo4j find error %j",err);
    reject("error");
    return;
  }
  logger.trace("neo4j found %j", results);
  for ( users of results.data) {
    //TODO search for duplicates and remove duplicates
    logger.debug("node found %s %s", users[0].data.screen_name, users[0].data.id_str);
    queue.create('queryFriendsList', { user: users[0].data, cursor: "-1" } ).attempts(5).removeOnComplete( true ).save();
  }

})
