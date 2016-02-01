

//use this sample to transfer documents in and out of mongo
//http://mongodb.github.io/node-mongodb-native/2.0/api/Cursor.html#~resultCallback

// #refactor:10 This script is terribly written and will bomb out on people with
// a high number of followers.

// refactor strategy:
// 1) Load seed user, put followers/following id's into uniqued list, put relationship into uniqued list
// 2) For each in list, check neo4j for user, load the user, add relationship,
//    put followers/following ids into uniqued list
// 3) Umm, this is looking very similar to current imp.
// 4) Profit
// 5) Semaphores suck, perhaps update mongo with start/finish info and load all users from mongo into neo4j.
//    This would allow the service to run continuously

//key: screen_name
//value: {neo4j id, distance_queried}
nodes = {};

//key:
friends = [];

var neo4j = require('seraph')();

var MongoClient = require('mongodb').MongoClient,
  test = require('assert');

var twitterCollection  = null;

var events = require('events');
var engine = new events.EventEmitter();

var logger = require('tracer').colorConsole('debug');

var nodesSem = require('semaphore')(10);
var relationshipsSem = require('semaphore')(4);
var querySem = require('semaphore')(2);
var db = null;

//runtime loop
MongoClient.connect('mongodb://localhost:27017/viewer-dev', function(err, db_) {
  db = db_;

  var stream = getMongoStream();
    // Execute find on all the documents
  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
  });

  stream.on('data', function(data) {
      parseCopyObject(data)
      .then(checkVerifyNeo4jObject);
  });

});

function getMongoStream(){
  return db.collection("socialGraph").find(
    {} ).stream();
}

function parseCopyObject(object){
  return new RSVP.Promise( function (resolve, reject) {
    resolve({
      user: filterProperties(object);
      followers: object.followers;
      friends: object.friends;
    });
  });
}

function checkVerifyNeo4jObject(data) {
  return new RSVP.Promise( function (resolve, reject) {
    var user = data.value.user;
    var followers = data.value.followers;
    var friends = data.value.friends;

    var neo4jUserExists = false;
    var relationshipPromises = [];
    //query neo4j

    if ( neo4jUserExists) {

    } else {
      upsertNodeToNeo4j(user).then( function(userNode){
        relationshipPromises = [
          checkVerifyNeo4jRelationships(userNode, followers),
          checkVerifyNeo4jRelationships(userNode, friends)
        ];
        rsvp.allSettled( function(results) {
          resolve();
        });
      });
    }
  });
}

function checkVerifyNeo4jRelationships(node, relationships) {
  return new RSVP.Promise( function (resolve, reject) {
    var neo4jRels = getNeo4jRelationships(node);
    var updatePromises = [];

    neo4jRels.sort(function(a,b){
      return a.id_str > b.id_str;
    }
    relationships.sort(function(a,b){
      return a.id_str > b.id_str;
    }

    for (rel of relationships) {
      var neo4jRelExists = false;
      if (neo4jRelExists){

      } else {

      }
    }
  };
}

function getNeo4jRelationships(node){
  return [];
}

function upsertNodeToNeo4j(node) {
  return new RSVP.Promise( function (resolve, reject) {
    neo4j.save(node, function(err, savedNode)
    {
      if (err){
        logger.error("%j",err);
        reject(err);
      }
      resolve(savedNode);
    });
  });
}

function upsertRelationships(relationships) {
  // body...
  for (rel of relationships){
    upsertRelationship(rel.node, rel.friend);
  }
}

function upsertRelationship(node, friend) {
  // body...
  neo4j.relate(node.id, 'follows', friend.id, function(err, rel){
    if (err){
      logger.error("%j",err);
      return;
    }
    logger.debug("saved relationship %j", rel);
  });
}

function witeNodeMetaToMongo(argument) {
  // body...
}

function filterProperties(user_){
  returnValue = {
    id_str: user_.id_str,
    screen_name: user_.screen_name,
    name: user_.name,
    followers_count: user_.followers_count,
    friends_count: user_.friends_count,
    description: user_.description,
    location: user_.location,
    statuses_count: user_.statuses_count
  };
  return returnValue;
}

//stats
var numRelationships = 0;

// TODO pass relationships to neo4j as an array  on the user objects.

function loadUser(user, parent, relationship){
  //if nodes does not contain screen_name
  if (Object.keys(nodes).indexOf(user.screen_name) == -1){
    //duplicate user object, add to nodes
    nodes[user.screen_name] = filterProperties(user);
    //initialize with no links
    nodes[user.screen_name].relationships = [];
    //one at a time
    nodesSem.take(function (){
      //save a copy of user into neo4j
      neo4j.save(filterProperties(user), function(err, node)
      {
        if (err){
          logger.error("%j",err);
          nodesSem.leave();
          return;
        }
      });
        nodes[node.screen_name].id = node.id;
        logger.info('saved %s', node.screen_name);
        if (parent){
          //notice how this also happens on line 96
          setRelationship(nodes[node.screen_name], nodes[parent.screen_name], relationship);
        }
        else (!parent){
          nodesSem.leave();
          return;
        }
    });
  }
  else {
    //user exists, link back to the parent node
    if (parent){
      nodesSem.take(function(){
        setRelationship(nodes[user.screen_name], nodes[parent.screen_name], relationship);
      });
    }
  }
}

//relationship indicates the direction. if else for a mirror image.
function setRelationship(user, parent, relationship){
  logger.debug("%s, %s, %d", user.screen_name, parent.screen_name, relationship);
  logger.debug("%s, %s, %d", user.id, parent.id, relationship);
  if (relationship == 1)
  {
    if (nodes[user.screen_name].relationships.indexOf(parent.id) == -1){
      neo4j.relate(user.id, 'follows', nodes[parent.screen_name].id, function(err, rel){
        if (err){
          logger.error("%j",err);
          nodesSem.leave();
          return;
        }
        logger.debug("saved relationship %j", rel);
        //remember the link.
        nodes[user.screen_name].relationships.push(parent.id);
        numRelationships++;
        nodesSem.leave();
      });
    }
    else
    {
      nodesSem.leave();
    }
  }
  else
  {
    //
    if (nodes[parent.screen_name].relationships.indexOf(user.id) == -1){
      neo4j.relate( nodes[parent.screen_name].id, 'follows', user.id, function(err, rel){
        if (err){
          logger.error("%j",err);
          nodesSem.leave();
          return;
        }
        logger.debug("saved relationship %j", rel);
        nodes[parent.screen_name].relationships.push(user.id);
        numRelationships++;
        nodesSem.leave();
      });
    }
    else
    {
      nodesSem.leave();
    }
  }
}

var loadSet = {};
var loadQueue = [];

//pesky engines
engine.on('loadNetwork', function(user, distance){
  if (loadSet[user.screen_name]){
    if (loadSet[user.screen_name].distance < distance){
      loadSet[user.screen_name].distance = distance;
    }
  }
  else {
    loadSet[user.screen_name] = { distance: distance};
    loadQueue.push({user: user, distance: distance});
  }
});

setInterval(function(){
  engine.emit('loadTick');
}, 10*1000);

engine.on('loadTick', function(){
  if (nodesSem.queue.length < 20 && loadQueue.length > 0){
    var next = loadQueue.shift();
    logger.debug("%j", next);
    loadNetwork(next.user, next.distance);
  }
});
