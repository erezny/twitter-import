
var Twit = require('../../../lib/twit.js');
const Neo4j = require('../../../lib/neo4j.js');
var Services = require('../../../lib/models/services.js');
const metrics = require('../../../lib/crow.js').init("importer", {
  api: "twitter",
  module: "friendsIDs",
  mvc: "api",
  function: "query",
  kue: "queryFriendsIDs",
});
var logger = require('tracer').colorConsole( {
  level: 'debug'
} );
var neo4j = new Neo4j(logger, metrics);
var T = new Twit(logger, metrics);

var serviceHandler = new Services(neo4j, T, logger, metrics);

serviceHandler.importFriendsIDs();
