
// #refactor:10 write queries
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

function queryTemplate(sortDir){
  return util.format(
    " match (n:twitterUser) where n.vip_distance > 0 " +
    "with distinct n.vip_distance as vip_distance_key, count(*) as vip_distance_value " +
    "return vip_distance_key, vip_distance_value order by vip_distance_key "
  );
}

function countVIPFriendsCompleteness(){
  neo4j.queryRaw(queryTemplate(), function(err, results) {
    if (err && err !== {}){
      logger.error("neo4j query %j %j", err, results);
      return;
    }
    for (row of results.data) {
      let vip_distance_key = row[0];
      let vip_distance_value = row[1];
      if (vip_distance_key > 0 && vip_distance_key < 10 )
        setGagues(vip_distance_key, vip_distance_value);
    }
  });
};

var gagues = {};
function setGagues(key, value){
  if ( key > 1 && key < 20 ) {
    if (!gagues[key])  { // I do not know enough ecmascript
      gagues[key] = metrics.setGauge("node.vip_distance_hist", { distance: key }, value);
    }
    metrics.setGauge("node.vip_distance_hist", value);
    init++;
  } else {
    metrics.gauge("remaining").set(remaining);
  }
}

countVIPFriendsCompleteness()
setInterval(countVIPFriendsCompleteness, 30 * 60 * 1000 );
