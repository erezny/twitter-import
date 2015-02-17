
var assert = require('assert');

// #refactor:10 use promises

var MongoClient = require('mongodb').MongoClient;
var logger;

var url = 'mongodb://localhost:27017/viewer-dev';
//var events = require('events').EventEmitter;
//var engine = new events();

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,
};

module.exports.init = function (options, callback){
  if (!callback) {
    callback = options;
    options = {};
  }
  if (options.logger) {
    logger = options.logger;
  }
  else{
    logger = require('tracer').colorConsole({level:'debug'});
  }
  MongoClient.connect(url, {server: {socketOptions: {keepAlive: 100}}}, function(err, db) {
    module.exports.db = db;
    module.exports.collection = db.collection('socialGraph');
    module.exports.tweetCollection = db.collection('tweets');
    db.on('close', function(){
      logger.debug("DB connection closed");
    });
    // engine.emit('dbReady');
    callback();
  });
};

// twitter.controller.queryUser(user, callback);

module.exports.queryUser = function(user, callback){
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  var cursor  = this.collection.find(query).limit(1);
  cursor.next(callback);
};

// twitter.controller.queryUser(user, callback);

module.exports.queryUserExists = function(user, callback){
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  var cursor  = this.collection.count(query, callback);
};

// twitter.controller.updateUser(user, callback)
// params:
//  user: {
//    id_str:
//    <others>
//  }
//  callback: function(err);

// #refactor:10 update 1 object etc
module.exports.updateUser = function(user, callback){
  //data['$currentDate']= {'collector.lastSavedDate': { $type: "timestamp"}};
  //  data['$setOnInsert']= { $currentDate: {'collector.insertedDate': { $type: "timestamp"}}};
  this.collection.update({'id_str': user.id_str}, user, {upsert: true}, callback);
};

module.exports.updateTweet = function (tweet, callback){
  if (tweet){
    this.tweetCollection.update({'id_str': tweet.id_str},tweet,{upsert: true}, callback);
  }
  //delete user.status;
};

module.exports.saveFollowers = function(user, followers, callback){

  logger.debug(user.screen_name + " gathered followers: " + followers.length);
  //debugger;

  followers = followers.map( function(follower){
    return {
      id: parseInt(follower),
      id_str: follower,
      service: 'twitter',$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}
    };
  });

  this.collection.insertMany(followers, {} ,function(err, result){
        if (err){ logger.debug(err);}
        //logger.debug("added a follower, " + err);
  } );

  this.collection.update({'id': user.id},
    {$set:{'followers': followers},
    $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
    function(err, result){
      logger.debug(user.screen_name + " added followers to user, " + err);
      callback(err, result);
  });
};


module.exports.saveFollowing = function(user, following, callback)
{

  logger.debug(user.screen_name + " gathered following: " + following.length);
  //debugger;

  following = following.map( function(follower){
    return {
      id: parseInt(follower),
      id_str: follower,
      service: 'twitter',$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}
    };
  });

  this.collection.insertMany(following, {} ,function(err, result){
        if (err){ logger.debug(err);}
        //logger.debug("added a follower, " + err);
  } );

  this.collection.update({id: user.id},
    {$set:{'following': following},
    $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
    function(err, result){
      logger.debug(user.screen_name + " added following to user, " + err);
      callback(err, result);
  });
};
