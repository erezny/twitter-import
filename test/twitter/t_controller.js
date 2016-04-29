
'use strict';

var RSVP = require('rsvp');
const Neo4j = require('../../lib/neo4j.js');
var Services = require('../../lib/models/services.js');
const crow = require('../../lib/crow.js');
var assert = require('assert');
const TwitterNeo4j = require('../../lib/twitter/controller/neo4j.js');

describe('twitter.controller', function() {

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
      neo4j = new TwitterNeo4j(logger, crowMetrics);
    });

    it('should construct object', function() {

      assert(neo4j);

    });

    it('should expose saveFriendsIDs', function() {
      assert(neo4j.saveFriendsIDs);
    });
    // module.exports = function(_neo4j, _logger, _metrics) {
    //   neo4j = _neo4j;
    //   logger = _logger;
    //   metrics = _metrics;
    //   return {
    it('should expose saveFriendsIDs', function() {
      assert(neo4j.saveFriendsIDs);
    });

    it('should expose saveUsers', function() {
      assert(neo4j.saveUsers);
    });

    it('should expose resetFriends', function() {
      assert(neo4j.resetFriends);
    });

    it('should expose saveLists', function() {
      assert(neo4j.saveLists);
    });

    it('should expose resetListOwnerships', function() {
      assert(neo4j.resetListOwnerships);
    });

    it('should expose resetListMembers', function() {
      assert(neo4j.resetListMembers);
    });

    it('should expose saveListMembers', function() {
      assert(neo4j.saveListMembers);
    });

    it('should expose saveListSubscriptions', function() {
      assert(neo4j.saveListSubscriptions);
    });

    it('should expose resetListSubscriptions', function() {
      assert(neo4j.resetListSubscriptions);
    });
  });
