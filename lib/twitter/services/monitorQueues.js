
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');

var logger = require('tracer').colorConsole( {
  level: 'trace'
} );

var influx = require('influx')(
  {
    host: process.env.INFLUX_HOST,
    port: parseInt(process.env.INFLUX_PORT),
    protocol: process.env.INFLUX_PROTOCOL,
    username: process.env.INFLUX_USERNAME,
    password: process.env.INFLUX_PASSWORD,
    database: process.env.INFLUX_DATABASE
  }
);

var userByIDQueue = Queue('loadTwitterUserMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);
var userNetworkQueue = Queue('loadTwitterUserNetworkMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);
var relationshipQueue = Queue('loadRelationshipToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);


function monitor(){
  userByIDQueue.count().then(function(count) {
    logger.info("userByIDQueue %d", count);
    influx.writePoint("MongoToNeo4j", count, { queue: 'userByIDQueue' }, function(err, res) { } );
  });

  userNetworkQueue.count().then(function(count) {
    logger.info("userNetworkQueue %d", count);
    influx.writePoint("MongoToNeo4j", count, { queue: 'userNetworkQueue' }, function(err, res) { } );
  });

  relationshipQueue.count().then(function(count) {
    logger.info("relationshipQueue %d", count);
    influx.writePoint("MongoToNeo4j", count, { queue: 'relationshipQueue' }, function(err, res) { } );
  });
}

setInterval( monitor, 5 * 1000 );
