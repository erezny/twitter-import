
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');
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
  .find( { "identifiers.neo4j": { $exists: false } })
  .project({
      id_str: 1,
      screen_name: 1,
      name: 1,
      followers_count: 1,
      friends_count: 1,
      favourites_count: 1,
      description: 1,
      location: 1,
      statuses_count: 1
  })

  var stream = cursor.stream();

  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
  });

  var openQueries = 0;
  var queryLimit = 20;
  var total = 0;
  var finished = 0;

  cursor.count(function(err, count) {
    logger.info("Number of users %d", count);
    total = count;
  });

  stream.on('data', function(user) {
    openQueries++;
    if ( openQueries > queryLimit) {
      stream.pause();
    }
    logger.trace('user from mongo: %s', user.screen_name);

    redis.hgetall(util.format("twitter:%s",user.id_str), function(err, obj) {

        if (obj && obj.neo4jID){
        metrics.counter("user_exists").increment();
        restartQueries();
        } else {
          upsertNodeToNeo4j(user)
            .then( function(user) {
              logger.debug('next user');
              metrics.counter("user_saved").increment();
              restartQueries()
            }, function(err) {
              logger.error('userByIDQueue err %j', err);
              metrics.counter("user_error").increment();
              restartQueries()
          });
        }
    });

  });

  function restartQueries(){
    finished++;
    openQueries--;
    if (openQueries < queryLimit / 2 ) {
      stream.resume();
    }
    if (finished % 100 == 0){
      logger.info("completed %d / %d", finished, total);
    }
  }

});

var sem = require('semaphore')(5);

function upsertNodeToNeo4j(node) {
  delete node.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s %s', node.screen_name, node.id_str);
    sem.take(function() {
    neo4j.find( { id_str: node.id_str }, false, "twitterUser" ,
        function(err, results) {
      if (err){
        logger.error("neo4j find %s %j",node.screen_name, err);
        metrics.counter("neo4j_find_error").increment();
        reject(err);
        sem.leave();
        return;
      }

      if (results.length > 0){
        logger.debug("found %j", results);
        //if results.length > 1, flag as error
        redis.hset(util.format("twitter:%s",node.id_str), "neo4jID", results.id, function(err, res) { });
        metrics.counter("neo4j_exists").increment();
        resolve(node);
        sem.leave();
        return;
      }

      neo4j.save(node, "twitterUser", function(err, savedNode) {
        if (err){
          logger.error("neo4j save %s %j", node.screen_name, err);
          metrics.counter("neo4j_save_error").increment();
          reject(err);
          sem.leave();
          return;
        }
        redis.hset(util.format("twitter:%s",node.id_str), "neo4jID", savedNode.id, function(err, res) { });
        logger.debug('inserted user %s', savedNode.screen_name);
        metrics.counter("neo4j_inserted").increment();
        sem.leave();
        resolve(savedNode);
      });
    });
  });
  });
}
