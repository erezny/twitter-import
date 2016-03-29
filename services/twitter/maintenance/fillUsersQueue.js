
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "users",
  mvc: "model",
  function: "fill",
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

checkfillUsers()
setInterval(checkfillUsers, 30 * 60 * 1000 );
function checkfillUsers() {
    queue.inactiveCount( 'queryUser', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
      if (total <= 180) {
        fillUsers();
      }
    });
  }

function queryTemplate(sortDir){
  return util.format(
    "match (n:twitterUser) " +
    "where not exists(n.screen_name) and not n.protected " +
    "return n " +
    "order by n.weighted_vip_distance desc limit 1800 "
  );
}

function fillUsers(){
  neo4j.queryRaw(queryTemplate(), function(err, results) {
    if (err){
      logger.error("neo4j find error %j",err);
      return;
    }
    logger.trace("neo4j found %j", results);
    for ( users of results.data) {
      logger.debug("pushing %s", users[0].data.id_str);
      queue.create('queryUser', { user: users[0].data } ).attempts(2).removeOnComplete( true ).save();
    }
  });
};
