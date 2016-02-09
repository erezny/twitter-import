
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');
var assert = require('assert');

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
  .find( )
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
  var queryLimit = 4;
  var total = 0;
  var finished = 0;

  cursor.count(function(err, count) {
    logger.info("Number of users %d", count);
    total = count;
  })

  stream.on('data', function(user) {
    openQueries++;
    if ( openQueries > queryLimit) {
      stream.pause();
    }
    logger.trace('user from mongo: %s', user.screen_name);
    checkVerifyNeo4jObject(user)
      .then( function(user) {
        logger.debug('next user');
        restartQueries()
      }, function(err) {
        logger.error('userByIDQueue err %j', err);
        restartQueries()
          done(Error('error processing user'));
    });
  });

  function restartQueries(){
    finished++;
    openQueries--;
    if (openQueries < queryLimit) {
      stream.resume();
    }
    if (finished %10 == 0){
      logger.info("completed %d / %d", finished, total);
    }
  }

});

function checkVerifyNeo4jObject(data) {
  influx.writePoint("MongoToNeo4j", 1, { function: 'checkVerifyNeo4jObject', id_str: data.id_str, screen_name: data.screen_name }, function(err, res) { });
  logger.trace('checkVerifyNeo4jObject %s', data.screen_name);
  return new RSVP.Promise( function (resolve, reject) {
    var user = data;

    logger.trace('checkVerifyNeo4jObject %s', user.screen_name);

    upsertNodeToNeo4j(user).then( function(userNode) {
        logger.trace('upsertNodeToNeo4j then resolve %s', userNode.screen_name);
        resolve();
    });
  });
}

function upsertNodeToNeo4j(node) {
  delete node.id;
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s %s', node.screen_name, node.id_str);
    influx.writePoint("MongoToNeo4j", 1, { function: 'upsertNodeToNeo4j', id_str: node.id_str, screen_name: node.screen_name }, function(err, res) { });

    neo4j.find( { id_str: node.id_str }, false, "twitterUser" ,
        function(err, results) {
      if (err){
        logger.error("%j",err);
        reject(err);
        return;
      }

      if (results.length > 0){
        logger.debug("found %j", results);
        node.id = results[0].id;
      }

      if (typeof(node.id) == "number"){
        neo4j.save(node, function(err, savedNode) {
          if (err){
            logger.error("%j",err);
            reject(err);
            return;
          }
          logger.debug('saved user %s', savedNode.screen_name);
          resolve(savedNode);
        });
      } else {
        neo4j.save(node, "twitterUser", function(err, savedNode) {
          if (err){
            logger.error("%j",err);
            reject(err);
            return;
          }
          logger.debug('inserted user %s', savedNode.screen_name);
          resolve(savedNode);
        });
      }
    });
  });
}
