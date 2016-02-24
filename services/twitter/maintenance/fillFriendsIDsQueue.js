
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

  setInterval( function() {
    queue.inactiveCount( 'queryFriendsList', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
      if (total <= 2) {
        fillFriendsList();
      }
    });
  }, 15 * 60 * 1000 );

  setInterval( function() {
    queue.inactiveCount( 'queryFriendsIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
      if (total <= 2) {
        fillFriendsIDs();
      }
    });
  }, 15 * 60 * 1000 );

function fillFriendsList(){
  neo4j.queryRaw("match (n:twitterUser) where n.friends_count > 0 and n.friends_count <= 2000 with n limit 10000 " +
    "match p=(n)-[:follows]->(:twitterUser) " +
    "WITH n, count(p) AS friends, count(p)/n.friends_count as ratio " +
    "return n order by ratio limit 10", function(err, results) {
    if (err){
      logger.error("neo4j find error %j",err);
      reject("error");
      return;
    }
    logger.trace("neo4j found %j", results);
    for ( users of results.data) {
      logger.debug("pushing %s %s", users[0].data.screen_name, users[0].data.id_str);
      queue.create('queryFriendsList', { user: users[0].data, cursor: "-1" } ).attempts(2).removeOnComplete( true ).save();
    }
  });
};

function fillFriendsIDs(){
  neo4j.queryRaw("match (n:twitterUser) where n.friends_count > 2000 with n limit 10000 " +
    "match p=(n)-[:follows]->(:twitterUser) " +
    "WITH n, count(p) AS friends, count(p)/n.friends_count as ratio " +
    "return n order by ratio limit 10", function(err, results) {
    if (err){
      logger.error("neo4j find error %j",err);
      reject("error");
      return;
    }
    logger.trace("neo4j found %j", results);
    for ( users of results.data) {
      logger.debug("pushing %s %s", users[0].data.screen_name, users[0].data.id_str);
      queue.create('queryFriendsIDs', { user: users[0].data, cursor: "-1" } ).attempts(2).removeOnComplete( true ).save();
    }
  });
};
