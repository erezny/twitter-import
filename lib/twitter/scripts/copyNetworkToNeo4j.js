
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

userNetworkQueue.on('ready', function() {
  // Queue ready for job
  // All Redis connections are done
  logger.trace("userNetworkQueue ready");
})

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

  var stream = db.collection("twitterUsers")
  .find( { screen_name: "erezny" } )
  .sort( { followers_count: 1 } )
  .stream();
    // Execute find on all the documents
  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
  });

  stream.on('data', function(data) {
    logger.trace("processing %s", data.screen_name);
    processTwitterUser(stream, data);
  });

  userByIDQueue.process(function(job, done) {
    logger.trace('userByIDQueue looking for %s', job.data.id_str);
    db.collection("twitterUsers")
    .find( { id_str: job.data.id_str } )
    .then(function(user) {
      logger.trace('userByIDQueue found %s', user.screen_name);
      var umm = userNetworkQueue.add(user);
      done();
    }).catch(function(err) {
      logger.trace('userByIDQueue err %j', err);
      done("not found");
    });
  });

  userNetworkQueue.process(function(job, done) {
    // job.data contains the custom data passed when the job was created
    // job.jobId contains id of this job.
    var user = job.data;
    logger.trace('userNetworkQueue %s', user.screen_name);
    checkVerifyNeo4jObject(parseCopyObject(user))
    .then(createRelationships)
    .then( function(user) {
      logger.trace('next user. Open Queries: %d', openQueries);
      done();
    }).catch(function(err) {
        done(Error('error processing user'));
    });
});

});

var openQueries = 0;

function processTwitterUser(stream, user){
  var parallelChecks = 5;

  openQueries++;
  if ( openQueries >= parallelChecks ) {
    stream.pause();
  }
  logger.trace('processTwitterUser %s', user.screen_name);
  checkVerifyNeo4jObject(parseCopyObject(user))
  .then(createRelationships)
  .then( function(user) {
    openQueries--;
    logger.trace('next user. Open Queries: %d', openQueries);
    if (openQueries < parallelChecks ){
      stream.resume();
    }
  });
}

function createRelationships(result) {
  return new RSVP.Promise( function (resolve, reject) {
    var user = result.user;
    var friends = result.friends;
    var followers = result.followers;
    logger.trace('createRelationships: %s', user.screen_name);

    var relationshipPromises = [];

    var friend_callback = upsertFriend(user, friends.length);
    var follower_callback = upsertFollower(user, followers.length);

    for (var friend of friends) {
      relationshipPromises.push(friend.then(friend_callback));
    }
    for (var follower of followers) {
      relationshipPromises.push(follower.then(follower_callback));
    }

    RSVP.allSettled(relationshipPromises).then(function(stuff) {
      logger.trace('finished creating relationships %s', user.screen_name);
      resolve(user);
    });
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
    var followers = mapArrayToNodes(data.followers);
    var friends = mapArrayToNodes(data.friends);

    logger.trace('checkVerifyNeo4jObject %s', user.screen_name);

    upsertNodeToNeo4j(user).then( function(userNode) {
        logger.info("upserted %s %d", user.screen_name, userNode.id);
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

//experienced timeouts at 1/50
var debugLimiter = new RateLimiter(1, (1 / 10) * 1000 );

function findUserByIDStr(id_str) {
  return new RSVP.Promise( function (resolve, reject) {
    logger.log('finding node id_str:%s', id_str)

      debugLimiter.removeTokens(1, function(err, remainingRequests) {
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
            userByIDQueue.add( { id_str: id_str });
            reject("none");
          }
        });
      });

  });
}

function upsertNodeToNeo4j(node) {
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s %s', node.screen_name, node.id_str);

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
      }

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

function upsertFriend(user, totalNum){
  logger.trace("upsert %d Friends %j", totalNum, user);
  var counter = 0;
  return function(friend) {
    counter++;
    logger.trace("upsertFriend %d/%d inner %s %j", counter, totalNum, user.screen_name, friend);
    return upsertRelationship(user, friend);
  }
}

function upsertFollower(user, totalNum){
  logger.trace("upsert %d Followers %j", totalNum, user);
  var counter = 0;
  return function(follower) {
    counter++;
    logger.trace("upsertFollower %d/%d inner %s %j", counter, totalNum, user.screen_name, follower);
    return upsertRelationship(follower, user);
  }
}

function upsertRelationship(node, friend) {
  return new RSVP.Promise( function (resolve, reject) {

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
