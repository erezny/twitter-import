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

module.exports.init = function (error){

  MongoClient.connect(url, function(err, db) {
    module.exports.db = db;
    module.exports.collection = db.collection('socialGraph');
    module.exports.tweetCollection = db.collection('tweets');
    // engine.emit('dbReady');
  });
};


  module.exports.queryUser = function(user, callback){
    // #strengthen:0 query on id or screenname
    this.collection.findOne({'id': user.id}, callback);
  };


  // #refactor:10 update 1 object etc
  module.exports.updateUser = function(user, callback){
    //data['$currentDate']= {'collector.lastSavedDate': { $type: "timestamp"}};
    //  data['$setOnInsert']= { $currentDate: {'collector.insertedDate': { $type: "timestamp"}}};
    this.collection.update({'id': user.id}, user, {upsert: true}, callback);
  };

  module.exports.updateTweet = function (tweet){
    if (tweet){
      this.tweetCollection.update({'id': tweet.id},tweet,{upsert: true}, function(err, result){
        console.log(user.id + "added tweet");
      });
    }
    //delete user.status;
  };

  module.exports.saveFollowers = function(user){
    // #get:0  only unique results
    var followers = [];
    var unique = {};
    for (var i in user.followers){
      if ( typeof(unique[user.followers[i]]) == "undefined"){
        followers.push.apply(user.followers[i]);
      }
      unique[user.followers[i]] = 0;
    }

    console.log(user.screen_name + " gathered followers: " + user.followers.length);
    //debugger;

    user.followers = followers.map( function(follower){
      return {
        id: follower,
        service: 'twitter'
      };
    });

    user.followers.forEach(function(follower){
      collection.update({id: follower.id, service: follower.service},
        {$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
        {upsert: true},function(err, result){
          if (err){ console.log(err);}
          //console.log("added a follower, " + err);
        } );
      });

      collection.update({'id': user.id},
      {$set:{'followers': user.followers},
      $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
      function(err, result){
        console.log(user.screen_name + " added followers to user, " + err);
        sem.leave();
      } );
    };


    module.exports.saveFollowing = function(user){

      // #get:0  only unique results
      var following = [];
      var unique = {};
      for (var i in user.following){
        if ( typeof(unique[user.following[i]]) == "undefined"){
          following.push.apply(user.following[i]);
        }
        unique[user.following[i]] = 0;
      }
      console.log(user.id + " gathered following: " + user.following.length);
      //debugger;

      // #add:0  other necessary fields
      user.following = following.map( function(friend){
        return {
          id: friend,
          service: 'twitter'
        };
      });

      // #commit:0  each to database
      user.following.forEach(function(friend){
        collection.update({id: friend.id, service: friend.service},
          {$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
          {upsert: true},function(err, result){
            if (err){ console.log(err);}
            //console.log("added a friend, " + err);
          } );
        });

        // #update:0  original user
        collection.update({'id': user.id},
        {$set:{'following': user.following},
        $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
        function(err, result){
          console.log(user.id + " added following to user, " + err);
          sem.leave();
          semUser.leave();
        });
      };
