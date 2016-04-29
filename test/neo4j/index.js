
var RSVP = require('rsvp');
const Neo4j = require('../../lib/neo4j.js');
var Services = require('../../lib/models/services.js');
const crow = require('../../lib/crow.js');
var assert = require('assert');

describe('lib neo4j', function() {

  it('should exist', function() {
    assert(Neo4j);
  });

  describe('constructor', function() {

    it('should detect socks proxy', function(done) {

      done();

    });

    it('should return a connection to a Neo4j server', function(done) {

      done();

    });
  });

});
