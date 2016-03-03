
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');
const metrics = require('../../../lib/crow.js').withPrefix("twitter.followers.maintenance.fillGraph");
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

checkFillFollowersList()
setInterval(checkFillFollowersList, 30 * 60 * 1000 );
function checkFillFollowersList() {
   queue.inactiveCount( 'queryFollowersList', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
     if (total <= 5) {
       fillFollowersList();
     }
   });
 }
checkfillFollowersIDs()
setInterval(checkfillFollowersIDs, 30 * 60 * 1000 );
function checkfillFollowersIDs() {
    queue.inactiveCount( 'queryFollowersIDs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
      if (total <= 5) {
        fillFollowersIDs();
      }
    });
  }

function queryTemplate(sortDir){
  return util.format("match (s:service{type:\"VIP\"}) " +
    "match (s)--(l:twitterUser) " +
    "with distinct l as l, rand() as r order by r limit 1000 " +
    "match (l)--(n:twitterUser) " +
    "with distinct n as n, rand() as r order by r limit 1000 " +
    "match (n)--(t:twitterUser) " +
    "with distinct t as t, rand() as r order by r limit 1000 " +
    "match p = (t)-[:follows]->(:twitterUser) " +
    "WITH t, count(p) AS followers, t.followers_count - count(p) as remaining " +
    "where remaining > 3 " +
    "return t order by remaining %s limit 100", sortDir);
}

function fillFollowersList(){
  neo4j.queryRaw(queryTemplate("asc"), function(err, results) {
    if (err){
      logger.error("neo4j find error %j",err);
      return;
    }
    logger.trace("neo4j found %j", results);
    for ( users of results.data) {
      logger.debug("pushing %s %s", users[0].data.screen_name, users[0].data.id_str);
      queue.create('queryFollowersList', { user: users[0].data, cursor: "-1" } ).attempts(2).removeOnComplete( true ).save();
    }
  });
};

function fillFollowersIDs(){
  neo4j.queryRaw(queryTemplate("desc"), function(err, results) {
    if (err){
      logger.error("neo4j find error %j",err);
      return;
    }
    logger.trace("neo4j found %j", results);
    for ( users of results.data) {
      logger.debug("pushing %s %s", users[0].data.screen_name, users[0].data.id_str);
      queue.create('queryFollowersIDs', { user: users[0].data, cursor: "-1" } ).attempts(2).removeOnComplete( true ).save();
    }
  });
};
