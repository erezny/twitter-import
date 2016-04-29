
'use strict';

var RSVP = require('rsvp');
const Neo4j = require('../lib/neo4j.js');
var Services = require('../lib/models/services.js');
const crow = require('../lib/crow.js');
var assert = require('assert');

describe('neo4j controller', function() {
    // it('should list object keys', function() {
    //   logger.info(Object.keys(Neo4j.prototype));
    //   assert(neo4j);
    // });

    it('should exist', function() {
      assert(Neo4j);
    });

    describe('integration tests', function() {
      var crowMetrics, logger, neo4j;

        before( function() {
          crowMetrics = crow.init("importer", {
            continuousIntegration: "unitTest",
            api: "twitter",
            function: "neo4j",
          });
          logger = require('tracer').colorConsole( {
            level: 'info'
          } );
          neo4j = new Neo4j(logger, crowMetrics);
        });

        it('should construct object', function() {

          assert(neo4j);

        });
    });

  });
