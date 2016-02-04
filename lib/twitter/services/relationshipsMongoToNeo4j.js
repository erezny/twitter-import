
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');
var cacheManager = require('cache-manager');
var memoryCache = cacheManager.caching( { store: 'memory', max: 100, ttl: 100 } );

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

var relationshipQueue = Queue('loadRelationshipToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);

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

  relationshipQueue.process(function(job, done) {
    logger.trace('relationshipQueue %j', job.data);
    var node = job.data.user;
    var friend = job.data.friend;

    var node_promise = memoryCache.wrap(node, function() {
        return findUserByIDStr(node);
    });
    var friend_promise = memoryCache.wrap(friend, function() {
        return findUserByIDStr(friend);
    });
    logger.trace('relationshipQueue %s %s', node, friend);

    RSVP.all([ node_promise, friend_promise ]).then( function(results) {
      logger.trace('relationshipQueue resolved  %j %j', results[0],results[1]);
      upsertRelationship(results[0], results[1]).then(function(rel) {
        logger.debug('relationshipQueue finished  %j', rel);
        done(null, rel);
      }, function(err) {
        logger.error('relationshipQueue err %j', err);
        done(err);
      });
    }, function(reason) {
      if (reason === "none"){
        logger.trace('relationshipQueue err %j', reason);
      } else {
        logger.error('relationshipQueue err %j', reason);
      }
      done(reason);
    });

  });

});

//experienced timeouts at 1/50
var debugLimiter = new RateLimiter(1, (1 / 10) * 1000 );

function findUserByIDStr(id_str) {
  return new RSVP.Promise( function (resolve, reject) {
    logger.log('finding node id_str:%s', id_str)

      debugLimiter.removeTokens(1, function(err, remainingRequests) {
        influx.writePoint("MongoToNeo4j", 1, { function: 'findUserByIDStr', id_str: id_str }, function(err, res) { });
        neo4j.find( { id_str: id_str }, false, "twitterUser" ,
            function(err, results) {
          if (err){
            if (err.code === "ECONNRESET"){
              debugLimiter.removeTokens(1, function(err, numRemain) {
                logger.trace("timeout");
              });
            }
            logger.error("%j",err);
            reject(err);
            return;
          }
          if (results.length > 0) {
            logger.trace('found node:%d id_str:%s', results[0].id, id_str)
            resolve(results[0]);
          } else {
            logger.trace('didnt find id_str:%s', id_str)
            //breadth-first search entire connected supernode but not overlap or repeat
            //userByIDQueue.add( { id_str: id_str });
            reject("none");
          }
        });
      });

  });
}

function upsertRelationship(node, friend) {
  return new RSVP.Promise( function (resolve, reject) {
    influx.writePoint("MongoToNeo4j", 1, { function: 'upsertRelationship', id_str: node.id_str, screen_name: node.screen_name }, function(err, res) { });
    influx.writePoint("MongoToNeo4j", 1, { function: 'upsertRelationship', id_str: friend.id_str, screen_name: friend.screen_name }, function(err, res) { });

    neo4j.relationships(node.id, 'out', 'follows', function(err, rels) {
      if (err){
        logger.error("%j",err);
        reject("error");
        return;
      }
      if (rels.length > 0) {
        for (rel of rels){
          if (rel.end == friend.id){
            logger.trace("relationship found %j", rel);
            resolve(rel);
            return;
          }
        }
      }
      neo4j.relate(node.id, 'follows', friend.id, function(err, rel) {
        if (err){
          logger.error("%j",err);
          reject("error");
          return;
        }
        logger.debug("saved relationship %j", rel);
        resolve(rel);
      });
    })
  });
}
