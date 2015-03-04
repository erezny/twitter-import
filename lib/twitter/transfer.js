
// #scaffold:0  for db import

// #model:0  neo4j graph nodes/edges

// #api:60 load database

// #api:160 weight nodes based on + - adjustments

// #api:110 weigh edges based on interactions

//use this sample to transfer documents in and out of mongo
//http://mongodb.github.io/node-mongodb-native/2.0/api/Cursor.html#~resultCallback

// A simple example showing the use of the cursor stream function.

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

var numRelationships = 0;

// TODO pass relationships to neo4j as an array  on the user objects.

function loadUser(user, parent, relationship){
  if (Object.keys(nodes).indexOf(user.screen_name) == -1){
    nodes[user.screen_name] = filterProperties(user);
    nodes[user.screen_name].relationships = [];
    nodesSem.take(function (){
      neo4j.save(filterProperties(user), function(err, node)
      {
        if (err){
          logger.error("%j",err);
          nodesSem.leave();
          return;
        }
        nodes[node.screen_name].id = node.id;
        logger.info('saved %s', node.screen_name);
        if (parent){
          setRelationship(nodes[node.screen_name], nodes[parent.screen_name], relationship);
        }
        else{
          nodesSem.leave();

        }
      });
    });
  }
  else {
    if (parent){
      nodesSem.take(function(){
        setRelationship(nodes[user.screen_name], nodes[parent.screen_name], relationship);
      });
    }
  }
}

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

function loadNetwork(user, distance){

  querySem.take(2, function(){
    logger.info("load Network %s, %d", user.screen_name, distance);
    if (distance > 0){
      twitterCollection.find({id_str: {$in: user.friends}}).forEach(
        function(result){
          //logger.info("adding user to queue");
          loadUser(result, user, -1);
          engine.emit('loadNetwork', result, distance-1, user.screen_name, -1);
        },
        function(err){
          logger.info("end friends");
          if (err){
            logger.error("%j", err);
          }
          querySem.leave();
          loadSet[user.screen_name].friends_finished = 1;
        });

      twitterCollection.find({id_str: {$in: user.followers}}).forEach(
        function(result){
          //logger.info("adding user to queue");
          loadUser(result, user, 1);
          engine.emit('loadNetwork', result, distance-1, user.screen_name, 1);
        },
        function(err){
          if (err){
            logger.error("%j", err);
          }
          logger.info("end followers");
          querySem.leave();
          loadSet[user.screen_name].followers_finished = 1;
        });
    }
    else{
      querySem.leave(2);
    }
  });
}

MongoClient.connect('mongodb://localhost:27017/viewer-dev', function(err, db_) {
  db = db_;
  engine.emit('dbReady');
});

engine.once('dbReady', function(){

  twitterCollection = db.collection('socialGraph');
    twitterCollection.find({screen_name: 'erezny'}).next( function(err, seed) {
    if (err){
      logger.error("%j", err);
    }
    loadUser(filterProperties(seed));
    engine.emit('loadNetwork', seed, 2);
  });
});

//keenClient = new Keen(config.env.keen);
setInterval(logStatus, 5*1000);

function logStatus(){
  logger.info("Nodes: %d total, %d / %d workers,\tRelationships: %d\tquerySem: %d",
  Object.keys(nodes).length, nodesSem.queue.length, nodesSem.current,
  numRelationships, querySem.queue.length);
}
