
var assert = require('assert');

var MongoClient = require('mongodb').MongoClient;
var logger;
var url;

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,
  listCollection: null
};

module.exports.init = function (config, callback, that) {
  if (!callback) {
    callback = config;
    config = {};
  } else {
    logger = config.logger;
    url = config.env.twitter.controller.url;
  }
  MongoClient.connect(url, {server: {socketOptions: {keepAlive: 100}}}, {w:1}, function(err, db) {
    if (err) {
      console.log("%j", err);
      process.exit();
    }
    module.exports.db = db;
    module.exports.collection = db.collection('twitterUsers');
    module.exports.tweetCollection = db.collection('tweets');
    module.exports.listCollection = db.collection('twitterLists');
    db.on('close', function() {
      logger.debug("DB connection closed");
    });
    // engine.emit('dbReady');
    callback(that);
  });
};

// twitter.controller.queryUser(user, callback);

module.exports.queryUser = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  var cursor  = this.collection.find(query).limit(1);
  cursor.next(callback);
};

// twitter.controller.lock(user, callback);

module.exports.lockUser = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  this.collection.findOneAndUpdate(query,
     {$inc: {'locks.query_user': 1}},{new: 1},
    callback);
};

// twitter.controller.unlockUser(user, callback);

module.exports.unlockUser = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  this.collection.findOneAndUpdate({'id_str': user.id_str},
  {$set: {'locks.query_user': 0,
    'locks.query_friends':0,'locks.expand_friends':0,
      'locks.query_followers':0,'locks.expand_followers':0
    }}, callback);
};

module.exports.finishedQueryFriends = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  this.collection.findOneAndUpdate({'id_str': user.id_str},
  {$set: {'locks.query_user': 0,
    'locks.query_friends':0,'locks.expand_friends':0,
      'locks.query_followers':0,'locks.expand_followers':0,
      'state.query_friends':0
    }}, callback);
};

module.exports.finishedQueryFollowers = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  this.collection.findOneAndUpdate({'id_str': user.id_str},
  {$set: {'locks.query_user': 0,
    'locks.query_friends':0,'locks.expand_friends':0,
      'locks.query_followers':0,'locks.expand_followers':0,
      'state.query_followers':0
    }}, callback);
};

module.exports.finishedExpandFriends = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  this.collection.findOneAndUpdate({'id_str': user.id_str},
  {$set: {'locks.query_user': 0,
    'locks.query_friends':0,'locks.expand_friends':0,
      'locks.query_followers':0,'locks.expand_followers':0,
      'state.expand_friends':0
    }}, callback);
};

module.exports.finishedExpandFollowers = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  this.collection.findOneAndUpdate({'id_str': user.id_str},
  {$set: {'locks.query_user': 0,
    'locks.query_friends':0,'locks.expand_friends':0,
      'locks.query_followers':0,'locks.expand_followers':0,
      'state.expand_followers':0
    }}, callback);
};
// twitter.controller.unlockUser(user, callback);

module.exports.saveUnlockUser = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = { "id_str": user.id_str };
  user.locks.query_user = 0;
  user.locks.query_friends = 0;
  user.locks.expand_friends = 0;
  user.locks.query_followers = 0;
  user.locks.expand_followers = 0;
  this.collection.findOneAndReplace({'id_str': user.id_str},
   user, {upsert: true}, callback);
};

// twitter.controller.queryUser(user, callback);
module.exports.queryUserExists = function(user, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  logger.trace('Querying mongo for ' + user.id_str);
  var query = {"id_str": user.id_str};
  var cursor  = this.collection.count(query, callback);
};

// twitter.controller.countDetailedFriends(user, callback);
// count number of
module.exports.countExistingUsers = function(id_str_list, callback) {
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  this.collection.count({id_str: {$in: id_str_list}}, function(err, count) {
    if(err){return callback(-1);}
    return callback(count);
  });
};

// twitter.controller.updateUser(user, callback)
// params:
//  user: {
//    id_str:
//    <others>
//  }
//  callback: function(err);

// #refactor:10 update 1 object etc
module.exports.saveUser = function(user, callback) {
  //data['$currentDate']= {'collector.lastSavedDate': { $type: "timestamp"}};
  //  data['$setOnInsert']= { $currentDate: {'collector.insertedDate': { $type: "timestamp"}}};
  this.collection.findOneAndReplace({'id_str': user.id_str}, user, {upsert: true}, callback);
};

module.exports.updateTweet = function (tweet, callback) {
  if (tweet) {
    this.tweetCollection.update({'id_str': tweet.id_str},tweet,{upsert: true}, callback);
  }
  //delete user.status;
};

module.exports.saveFollowers = function(user, followers, callback) {

  logger.debug(user.screen_name + " gathered followers: " + followers.length);
  //debugger;

  this.collection.update({ 'id_str': user.id_str },
    { $addToSet:{ 'followers': { $each: followers } },
    $currentDate: { 'collector.lastSavedDate': { $type: "timestamp" } } },
    function(err, result) {
      logger.debug(user.screen_name + " added followers to user, " + err);
      callback(err, result);
  });
};

module.exports.saveFriends = function(user, friends, callback)
{

  logger.debug(user.screen_name + " gathered friends: " + friends.length);
  //debugger;

  this.collection.update({ 'id_str': user.id_str },
    { $addToSet:{ 'friends': { $each: friends } },
    $currentDate: { 'collector.lastSavedDate': { $type: "timestamp" } } },
    function(err, result) {
      logger.debug(user.screen_name + " added followers to user, " + err);
      callback(err, result);
  });
};

module.exports.countRemaining = function(callback){
  this.collection.count({
  $or: [
    { 'state.query_followers': 1 },
    { 'state.query_friends': 1 },
    { 'state.expand_followers': { $gt: 0 } },
    { 'state.expand_friends': { $gt: 0 } }
  ] }, callback);
};
