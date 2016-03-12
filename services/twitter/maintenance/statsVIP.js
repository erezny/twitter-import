
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');
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

function queryTemplate(sortDir){
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
    setGagues(total, remaining)
  });
};

var init = 0;
function setGagues(total, remaining){
  if (init == 0) {
    metrics.setGauge("total", total);
    metrics.setGauge("remaining", remaining);
    init++;
  } else {
    metrics.gauge("total").set(total);
    metrics.gauge("remaining").set(remaining);
  }
}

countVIPFriendsCompleteness()
setTimeout(countVIPFriendsCompleteness, 30 * 60 * 1000 );
