
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var assert = require('assert');
const crow = require("crow-metrics");
const request = require("request");

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("mongoToNeo4j.users");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
  process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
  process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
    process.env.NEO4J_PROTOCOL,
    process.env.NEO4J_HOST,
    process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

var MongoClient = require('mongodb').MongoClient,
  test = require('assert');

var twitterCollection  = null;

var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var db = null;

var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});



//runtime loop
MongoClient.connect(util.format('mongodb://%s:%s@%s:%d/%s?authMechanism=SCRAM-SHA-1&authSource=%s',
  process.env.MONGO_USER,
  process.env.MONGO_PASSWD,
  process.env.MONGO_HOST,
  process.env.MONGO_PORT,
  process.env.MONGO_DATABASE,
  process.env.MONGO_DATABASE
),
  {
    logger: logger,
    numberOfRetries: 10,
    retryMiliSeconds: 10000
  },
  function(err, db_) {
    if (err){
      process.exit;
    }
  db = db_;

  logger.trace("connected to mongo");

  var cursor = db.collection("twitterUsers")
  .find( { 'internal.expand_friends': { $gt: 0 } } )
  .project({
      id_str: 1,
      'internal.expand_friends': 1
  }).sort( { 'internal.expand_friends': -1 });

  var stream = cursor.stream();

  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
  });

  var openQueries = 0;
  var queryLimit = 20;
  var total = 0;
  var finished = 0;

  metrics.setGauge( "openQueries", function () { return openQueries; });
  metrics.setGauge( "queryLimit", function () { return queryLimit; });
  metrics.setGauge( "total", function () { return total; });
  metrics.setGauge( "finished", function () { return finished; });

  cursor.count(function(err, count) {
    logger.debug("Number of users %d", count);
    total = count;
  });

  stream.on('data', function(user) {
    logger.debug("adding %j", user);
    queue.create('queryUserListOwnership', {
      user: { id_str: user.id_str }, cursor: "-1"
    }).removeOnComplete( true ).save();

  });

});
