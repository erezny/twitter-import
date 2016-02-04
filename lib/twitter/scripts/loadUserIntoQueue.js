
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');

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
  level: 'trace'
} );

var userByIDQueue = Queue('loadTwitterUserMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);
var userNetworkQueue = Queue('loadTwitterUserNetworkMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);

var db = null;

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

  db.collection("twitterUsers")
  .findOne( { screen_name: "JeffGoins" } , { fields: { id_str: 1 } })
  .then(function(user) {
        logger.trace("adding %s", user.id_str);
        userByIDQueue.add(user).then(function(ret) {
          process.exit();
        });
  }, function(reason) {
        logger.error("error %j", reason);
        process.exit();
  });

});
