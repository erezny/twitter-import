
'use strict';

var RSVP = require('rsvp');
const crow = require('../../lib/crow.js');
var assert = require('assert');
const TwitterAPI = require('../../lib/twitter/api.js');

describe('twitter api handler', function() {

    // it('should list object keys', function() {
    //   console.log(Object.keys(TwitterAPI.prototype));
    //   assert(TwitterAPI);
    // });

    describe('prototype', function() {
      it('should expose apiHarness', function() {
        assert(TwitterAPI.prototype.apiHarness);
      });
      it('should expose pagedAPIQuery', function() {
        assert(TwitterAPI.prototype.pagedAPIQuery);
      });
      it('should expose friendsIDs', function() {
        assert(TwitterAPI.prototype.friendsIDs);
      });
      it('should expose users', function() {
        assert(TwitterAPI.prototype.users);
      });
      it('should expose listMembers', function() {
        assert(TwitterAPI.prototype.listMembers);
      });
      it('should expose userListOwnership', function() {
        assert(TwitterAPI.prototype.userListOwnership);
      });
      it('should expose userListSubscriptions', function() {
        assert(TwitterAPI.prototype.userListSubscriptions);
      });
      it('should expose userTweets', function() {
        assert(TwitterAPI.prototype.userTweets);
      });

    });

    describe('unit tests', function() {
      var crowMetrics, logger, api;

        before( function() {
          crowMetrics = crow.init("importer", {
            continuousIntegration: "unitTest",
            api: "twitter",
            function: "neo4j",
          });
          logger = require('tracer').colorConsole( {
            level: 'info'
          } );
          api = new TwitterAPI(null, logger, crowMetrics);
        });

        it('should construct object', function() {

          assert(api);

        });
        describe('apiHarness', function() {
          it('should return a promise', function() {
            assert.equal(typeof(api.apiHarness().then), 'function');
          });
        });
        describe('pagedAPIQuery', function() {
          it('should return a promise', function() {
            assert.equal(typeof(api.pagedAPIQuery().then), 'function');
          });
        });
        describe('friendsIDs', function() {
          it('should return a function', function() {
            assert.equal(typeof(api.friendsIDs()), 'function');
          });
        });
        describe('users', function() {
          it('should return a function', function() {
            assert.equal(typeof(api.users()), 'function');
          });
        });
        describe('listMembers', function() {
          it('should return a function', function() {
            assert.equal(typeof(api.listMembers()), 'function');
          });
        });
        describe('userListOwnership', function() {
          it('should return a function', function() {
            assert.equal(typeof(api.userListOwnership()), 'function');
          });
        });
        describe('userListSubscriptions', function() {
          it('should return a function', function() {
            assert.equal(typeof(api.userListSubscriptions()), 'function');
          });
        });
        describe('userTweets', function() {
          it('should return a function', function() {
            assert.equal(typeof(api.userTweets()), 'function');
          });
        });
    });

  });
