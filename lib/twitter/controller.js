
var assert = require('assert');

// #refactor:10 use promises

var MongoClient = require('mongodb').MongoClient;

var url = 'mongodb://localhost:27017/viewer-dev';
//var events = require('events').EventEmitter;
//var engine = new events();

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,
};

module.exports.init = function (callback){

  MongoClient.connect(url, function(err, db) {
    module.exports.db = db;
    module.exports.collection = db.collection('socialGraph');
    module.exports.tweetCollection = db.collection('tweets');
    // engine.emit('dbReady');
    callback();
  });
};

// twitter.controller.queryUser(user, callback);

module.exports.queryUser = function(user, callback){
  // #strengthen:0 query on id or screenname
  assert(this.collection !== null, 'database not ready');
  console.log('Querying mongo for ' + user.id_str);
  debugger;
  this.collection.findOne({id: user.id}, callback);
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

  console.log(user.screen_name + " gathered followers: " + followers.length);
  //debugger;

  followers = followers.map( function(follower){
    return {
      id: parseInt(follower),
      id_str: follower,
      service: 'twitter',$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}
    };
  });

  this.collection.insertMany(followers, {} ,function(err, result){
        if (err){ console.log(err);}
        //console.log("added a follower, " + err);
  } );

  this.collection.update({'id': user.id},
    {$set:{'followers': followers},
    $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
    function(err, result){
      console.log(user.screen_name + " added followers to user, " + err);
      callback(err, result);
  });
};


module.exports.saveFollowing = function(user, following, callback)
{

  console.log(user.screen_name + " gathered following: " + following.length);
  //debugger;

  following = following.map( function(follower){
    return {
      id: parseInt(follower),
      id_str: follower,
      service: 'twitter',$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}
    };
  });

  this.collection.insertMany(following, {} ,function(err, result){
        if (err){ console.log(err);}
        //console.log("added a follower, " + err);
  } );

  this.collection.update({id: user.id},
    {$set:{'following': following},
    $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
    function(err, result){
      console.log(user.screen_name + " added following to user, " + err);
      callback(err, result);
  });
};
