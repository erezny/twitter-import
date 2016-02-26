
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');
const metrics = require('../../../lib/crow.js').withPrefix("twitter.friends.maintenance.fillGraph");
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

  setInterval( function() {
    queue.inactiveCount( 'queryFriendsList', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
      if (total <= 5) {
        fillFriendsList();
      }
    });
  }, 5 * 60 * 1000 );

  setInterval( function() {
    queue.inactiveCount( 'queryFriendsIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
      if (total <= 5) {
        fillFriendsIDs();
      }
    });
  }, 5 * 60 * 1000 );

function queryTemplate(sortDir){
  return util.format("match (n:service{type:"VIP"})--()--(n:twitterUser) " +
    "with collect(distinct n) as n " +
    "match p=(n)-[:follows]->(:twitterUser) " +
    "WITH n, count(p) AS friends, n.friends_count - count(p) as remaining " +
    "return n order by remaining %s limit 5", sortDir);
}

function fillFriendsList(){
  neo4j.queryRaw(queryTemplate("asc"), function(err, results) {
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
  neo4j.queryRaw(queryTemplate("desc"), function(err, results) {
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
