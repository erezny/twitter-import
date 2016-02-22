
// #refactor:10 write queries
var util = require('util');
var Twit = require('twit');
var T = new Twit({
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
//  access_token:         process.env.TWITTER_ACCESS_TOKEN,
//  access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET,
  app_only_auth:        true
});

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const crow = require("crow-metrics");
const request = require("request");
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.users.query");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 180) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

process.once( 'SIGTERM', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
});

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

  var db = null;
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

  queue.process('receiveUser', function(job, done) {
    //  logger.info("received job");
    logger.trace("received job %j", job);
    saveUserToMongo(job.data.user);
    upsertUserToNeo4j(job.data.user)
    .then(function(savedUser) {
      logger.trace("savedUser: %j", savedUser);
      return new RSVP.Promise( function (resolve, reject) {
        queue.create('queryUserFriends', { user: { id_str: job.data.user.id_str } } ).removeOnComplete( true ).save();
        queue.create('queryUserFollowers', { user: { id_str: job.data.user.id_str } } ).removeOnComplete( true ).save();
        metrics.counter("processFinished").increment();
        resolve();
      });
    })
    .then(done)
    .catch(function(err) {
      logger.error("receiveUser error on %j\n%j\n--", job, err);
      metrics.counter("processError").increment();
      done(err);
    });
  });

});

queue.process('queryUser', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  queryUser(job.data.user)
  .then(done)
  .catch(function(err) {
    logger.error("queryUser error %j: %j", job.data, err);
    metrics.counter("queryError").increment();
    done(err);
  });
});

function queryUser(user) {
  return new Promise(function(resolve, reject) {
    logger.info("queryUser %s", user.id_str);
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('users/show', { user_id: user.id_str }, function(err, data)
      {
        if (err){
          logger.error("twitter api error %j %j", user, err);
          reject({ user: user, err: err, reason: "twitter api error" });
          metrics.counter("apiError").increment();
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryUser twitter api callback");
        var user = {
          id_str: data.id_str,
          screen_name: data.screen_name,
          name: data.name,
          followers_count: data.followers_count,
          friends_count: data.friends_count,
          favourites_count: data.favourites_count,
          description: data.description,
          location: data.location,
          statuses_count: data.statuses_count,
          protected: data.protected
        }
        queue.create('receiveUser', { user: user } ).removeOnComplete( true ).save();
        metrics.counter("queryFinished").increment();
        resolve();
      });
    });
  });
};

function saveUserToMongo(user) {
  return db.collection("twitterUsers").update(
    { 'id_str': user.id_str },
    { $set: user ,
    $currentDate: { 'timestamps.updated': { $type: "timestamp" } }
  },
  { upsert: true });
};

function upsertUserToNeo4j(user) {
  delete user.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s %s', user.screen_name, user.id_str);
    neo4j.find( { id_str: user.id_str }, false, "twitterUser" ,
        function(err, results) {
      if (err){
        logger.error("neo4j find %s %j",user.screen_name, err);
        metrics.counter("neo4j_find_error").increment();
        reject({ err:err, reason:"neo4j find user error" });
        return;
      }

      if (results.length > 0){
        logger.debug("found %j", results);
        //if results.length > 1, flag as error
        redis.hset(util.format("twitter:%s",user.id_str), "neo4jID", results[0].id, function(err, res) { });
        metrics.counter("neo4j_exists").increment();
        resolve(user);
        return;
      }

      neo4j.save(user, "twitterUser", function(err, savedUser) {
        if (err){
          logger.error("neo4j save %s %j", user.screen_name, err);
          metrics.counter("neo4j_save_error").increment();
          reject({ err:err, reason:"neo4j save user error" });
          return;
        }
        redis.hset(util.format("twitter:%s",user.id_str), "neo4jID", savedUser.id, function(err, res) { });
        logger.debug('inserted user %s', savedUser.screen_name);
        metrics.counter("neo4j_inserted").increment();
        resolve(savedUser);
      });
    });
  });
}
