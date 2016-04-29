

var util = require('util');
var assert = require('assert');
var _ = require('../../../lib/util.js');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "vip",
  mvc: "model",
  function: "stats",
});
var queue = require('../../../lib/kue.js');

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'trace'
} );

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

function queryTemplate(sortDir) {
  return util.format("match (s:service{type:\"VIP\"}) " +
      "match (s)--()--(n:twitterUser) " +
      "with distinct n as n " +
      "match p = (n)-[:follows]->(:twitterUser) " +
      "WITH n, count(p) AS friends, n.friends_count - count(p) as remaining " +
      "return sum(n.friends_count), sum(remaining)");
}

function countVIPFriendsCompleteness(){
  neo4j.queryRaw(queryTemplate(), function(err, results) {
    if (err && err !== {}){
      logger.error("neo4j find error %j %j", err, results);
      return;
    }
    logger.trace("neo4j found %j", results);
    var total = results.data[0][0];
    var remaining = results.data[0][1];
    setGagues(total, remaining);
  });
}

var init = 0;
function setGagues(total, remaining){
  if (init === 0) {
    metrics.setGauge("total", total);
    metrics.setGauge("remaining", remaining);
    init++;
  } else {
    metrics.gauge("total").set(total);
    metrics.gauge("remaining").set(remaining);
  }
}

countVIPFriendsCompleteness();
setInterval(countVIPFriendsCompleteness, 30 * 60 * 1000 );

  // direct graph import inwards, importing users following imported users first

function updateDistances() {
  var query =
    "match (n:twitterUser) with n, rand() as r order by r limit 100000 " +
    "match (v:service{type:\"VIP\"}) with n,v " +
    "optional match path=shortestPath((n)<-[*..20]-(v)), " +
    "               followerships=(n)<-[r1:follows]-(m:twitterUser), " +
    "               friendships=(n)-[r2:follows]->(l:twitterUser) " +
    "where not m.screen_name is null and not l.screen_name is null " +
    "with n, length(path) as distance, " +
    "     length(followerships) as followers , length(friendships) as friends " +
    "set n.vip_distance = distance, " +
    "    n.followers_imported_count = followers, " +
    "    n.friends_imported_count = friends, " +
    "    n.weighted_vip_distance    = sqrt( toFloat(friends*friends) / toFloat(distance*distance) ) ";

  neo4j.queryRaw(query, function(err, results) {
    if (!_.isEmpty(err)){
      logger.error("neo4j find error %j %j", err, results);
      return;
    }
  });
}

updateDistances();
setInterval(updateDistances, 5 * 60 * 1000 );
