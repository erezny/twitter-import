
'use strict';

var RSVP = require('rsvp');
const Neo4j = require('../../lib/neo4j.js');
var Services = require('../../lib/models/services.js');
const crow = require('../../lib/crow.js');
var assert = require('assert');
const TwitterNeo4j = require('../../lib/twitter/controller/neo4j.js');

describe('twitter neo4j controller', function() {

    // it('should list object keys', function() {
    //   logger.info(Object.keys(TwitterNeo4j.prototype));
    //   assert(neo4j);
    //
    // });

    it('should expose queryRunner', function() {
      assert(TwitterNeo4j.prototype.queryRunner);
    });
    it('should expose saveFriendsIDs', function() {
      assert(TwitterNeo4j.prototype.saveFriendsIDs);
    });
    it('should expose saveFriendsIDs', function() {
      assert(TwitterNeo4j.prototype.saveFriendsIDs);
    });
    it('should expose saveUsers', function() {
      assert(TwitterNeo4j.prototype.saveUsers);
    });
    it('should expose resetFriends', function() {
      assert(TwitterNeo4j.prototype.resetFriends);
    });
    it('should expose saveLists', function() {
      assert(TwitterNeo4j.prototype.saveLists);
    });
    it('should expose resetListOwnerships', function() {
      assert(TwitterNeo4j.prototype.resetListOwnerships);
    });
    it('should expose resetListMembers', function() {
      assert(TwitterNeo4j.prototype.resetListMembers);
    });
    it('should expose saveListMembers', function() {
      assert(TwitterNeo4j.prototype.saveListMembers);
    });
    it('should expose saveListSubscriptions', function() {
      assert(TwitterNeo4j.prototype.saveListSubscriptions);
    });
    it('should expose saveStatuses', function() {
      assert(TwitterNeo4j.prototype.saveStatuses);
    });
    it('should expose saveStatusQuery', function() {
      assert(TwitterNeo4j.prototype.saveStatusQuery);
    });
    it('should expose saveStatusUserEntityQuery', function() {
      assert(TwitterNeo4j.prototype.saveStatusUserEntityQuery);
    });
    it('should expose saveStatusHashTagsQuery', function() {
      assert(TwitterNeo4j.prototype.saveStatusHashTagsQuery);
    });
    it('should expose saveStatusURLSQuery', function() {
      assert(TwitterNeo4j.prototype.saveStatusURLSQuery);
    });
    it('should expose saveStatusEntitiesQuery', function() {
      assert(TwitterNeo4j.prototype.saveStatusEntitiesQuery);
    });
    it('should expose getVIPNode', function() {
      assert(TwitterNeo4j.prototype.getVIPNode);
    });
    it('shouldn\'t expose anything else', function() {
      assert.equal(Object.keys(TwitterNeo4j.prototype).length, 17);
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
          neo4j = new TwitterNeo4j(logger, crowMetrics);
        });

        it('should construct object', function() {

          assert(neo4j);

        });
    });

  });
