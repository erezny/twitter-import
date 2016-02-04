
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');

//key: screen_name
//value: {neo4j id, distance_queried}
nodes = {};

//key:
friends = [];

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

var logger = require('tracer').colorConsole('trace');

var db = null;

//runtime loop
MongoClient.connect('mongodb://twitterImport:1EUyVa367T6wnJTzgykwi0s2@zapp:19024/socialGraph?authMechanism=SCRAM-SHA-1&authSource=socialGraph',
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

  logger.trace("connected");

  var stream = db.collection("twitterUsers").find( ).stream();
    // Execute find on all the documents
  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
  });

  stream.on('data', function(data) {
    logger.trace("processing %s", data.screen_name);
      processTwitterUser(stream, data);
  });

});

var openQueries = 0;

function processTwitterUser(stream, user){
  var parallelChecks = 2;

  openQueries++;
  if ( openQueries >= parallelChecks ) {
    stream.pause();
  }
  logger.trace('processTwitterUser %s', user.screen_name);
  checkVerifyNeo4jObject(parseCopyObject(user)).then(function(result) {
    openQueries--;
    logger.trace('next user. Open Queries: %d', openQueries);
    if (openQueries < parallelChecks ){
      stream.resume();
    }
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
logger.trace('checkVerifyNeo4jObject %s', data.user.screen_name);
  return new RSVP.Promise( function (resolve, reject) {
    var user = data.user;
    var followers = data.followers;
    var friends = data.friends;
    logger.trace('checkVerifyNeo4jObject %s', user.screen_name);
    upsertNodeToNeo4j(user).then( function(userNode) {
        logger.info("upserted %s %d", user.screen_name, userNode.id);
        resolve(userNode);
    });
  });
}

function upsertNodeToNeo4j(node) {
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s', node.screen_name);

    neo4j.find( { id_str: node.id_str }, false, "twitterUser" ,
        function(err, results) {
      if (err){
        logger.error("%j",err);
        reject(err);
        return;
      }

      function saveNodeCallback(err, savedNode){
        if (err){
          logger.error("%j",err);
          reject(err);
          return;
        }
        logger.info('saved %s', savedNode.screen_name);
        resolve(savedNode);
      };

      if (results.length > 0){
        logger.debug("found %j", results);
        node.id = results[0].id;
        neo4j.save(node, saveNodeCallback);
      } else {
        neo4j.save(node, "twitterUser", saveNodeCallback);
      }

    });

  });
}
