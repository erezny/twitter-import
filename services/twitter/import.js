'use strict';
var RSVP = require('rsvp');
const Neo4j = require('../../lib/neo4j.js');
var Services = require('../../lib/models/services.js');
const metrics = require('../../lib/crow.js').init("importer", {
  api: "twitter",
  function: "import",
});
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var neo4j = new Neo4j(logger, metrics);

var serviceHandler = new Services(neo4j, logger, metrics);

serviceHandler.runAllUserImports().then(function(jobs) {
  serviceHandler.runAppImports().then(function(appJobs) {
    for (var job of appJobs) {
      jobs.push(job);
    }
    RSVP.allSettled(jobs)
    .then(function() {
      process.nextTick(process.exit, 0);
    }, function(err) {
      logger.error(err);
        process.nextTick(process.exit, 0);
    });
  });
});
