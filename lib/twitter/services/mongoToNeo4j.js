
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

  userByIDQueue.process(function(job, done) {
    influx.writePoint("MongoToNeo4j", 1, { function: 'userByIDQueue', id_str: job.data.id_str }, function(err, res) { });
    logger.debug('userByIDQueue looking for %s', job.data.id_str);
    db.collection("twitterUsers")
    .findOne( { id_str: job.data.id_str } )
    .then(function(user) {
      logger.trace('userByIDQueue found %s', user.screen_name);
      checkVerifyNeo4jObject(parseCopyObject(user))
      .then(createRelationships)
      .then( function(user) {
        logger.debug('next user');
        done();
      }, function(err) {
        logger.error('userByIDQueue err %j', err);
          done(Error('error processing user'));
      });
    }).catch(function(err) {
      logger.error('userByIDQueue err %j', err);
      done("not found");
    });
  });

});

function createRelationships(result) {
  return new RSVP.Promise( function (resolve, reject) {
    influx.writePoint("MongoToNeo4j", 1, { function: 'createRelationships', id_str: result.user.id_str, screen_name: result.user.screen_name }, function(err, res) { });
    var user = result.user;
    var friends = result.friends;
    var followers = result.followers;
    logger.trace('createRelationships: %s', user.screen_name);

    for (var friend of friends) {
      relationshipQueue.add({ user: user.id_str, friend: friend });
    }
    for (var follower of followers) {
      relationshipQueue.add({ user: follower, friend: user.id_str });
    }

    resolve();
  });

}

function parseCopyObject(object){
  return {
    user: filterProperties(object),
    followers: object.followers,
    friends: object.friends,
  };
}

function filterProperties(user_){
  returnValue = {
    id_str: user_.id_str,
    screen_name: user_.screen_name,
    name: user_.name,
    followers_count: user_.followers_count,
    friends_count: user_.friends_count,
    favourites_count: user_.favourites_count,
    description: user_.description,
    location: user_.location,
    statuses_count: user_.statuses_count
  };
  return returnValue;
}

function checkVerifyNeo4jObject(data) {
  influx.writePoint("MongoToNeo4j", 1, { function: 'checkVerifyNeo4jObject', id_str: data.user.id_str, screen_name: data.user.screen_name }, function(err, res) { });
  logger.trace('checkVerifyNeo4jObject %s', data.user.screen_name);
  return new RSVP.Promise( function (resolve, reject) {
    var user = data.user;
    var followers = data.followers;
    var friends = data.friends;

    logger.trace('checkVerifyNeo4jObject %s', user.screen_name);

    upsertNodeToNeo4j(user).then( function(userNode) {
        logger.trace('upsertNodeToNeo4j then resolve %s', userNode.screen_name);
        resolve({
          user: userNode,
          followers: followers,
          friends: friends
        });
    });
  });
}

//id_str array
//some id_str are not in neo4j
function mapArrayToNodes(array) {
  var resultPromiseArray = []
  for (node of array) {
    resultPromiseArray.push(findUserByIDStr(node));
  }
  return resultPromiseArray;
}

function findUserByIDStr(id_str) {
  return new RSVP.Promise( function (resolve, reject) {
    logger.log('finding node id_str:%s', id_str)

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

}

function upsertNodeToNeo4j(node) {
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
          logger.info('saved user %s', savedNode.screen_name);
          resolve(savedNode);
        });
      } else {
        neo4j.save(node, "twitterUser", function(err, savedNode) {
          if (err){
            logger.error("%j",err);
            reject(err);
            return;
          }
          logger.info('inserted user %s', savedNode.screen_name);
          resolve(savedNode);
        });
      }
    });
  });
}
