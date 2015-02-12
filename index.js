var Twit = require('twit');

var T = new Twit({
  consumer_key:         '***REMOVED***',
  consumer_secret:      '***REMOVED***',
  access_token:         '***REMOVED***',
  access_token_secret:  '***REMOVED***'
});

var MongoClient = require('mongodb').MongoClient;

var url = 'mongodb://localhost:27017/viewer-dev';
var db = null;
var collection = null;
var tweetCollection = null;

var RateLimiter = require('limiter').RateLimiter;
var limiter = new RateLimiter(5, 5*60*1000);
var limiterUser = new RateLimiter(12, 60*1000);

var sem = require('semaphore')(1);
var semUser = require('semaphore')(1);
var semUserData = require('semaphore')(1);

var docs = [];

var getFollowers = function (user, cursor_str, array, callback) {
  //
  //  gather the list of user id's that follow @tolga_tezel
  //
  console.log(user + "start getFollowers");
  limiter.removeTokens(1, function(err, remainingRequests){
    T.get('followers/ids', { id: user, count: 5000, stringify_ids: 1 },  function (err, data, response) {
      console.log(user + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
      console.log(user + ' limit reset: ' + response.headers['x-rate-limit-reset']);
      //console.log(data);
      var timeout = 0;
      if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
        timeout = response.headers['x-rate-limit-reset'] - Date.now();
        console.log(user + ' until reset: ' + timeout/1000);
      }

      console.log(user + ' next cursor: ' + data.next_cursor_str);
      array.push.apply(array, data.ids);
      //console.log(data.users[0]);
      //console.log(response.headers);
      //debugger;
      if (data.next_cursor_str !== cursor_str){
        setTimeout(getFollowers, timeout, user, data.next_cursor_str, array, callback);
      }
      else
      callback(array, user);
    });
  });
};

var getFollowing = function (user, cursor_str, array, callback) {
  //
  //  gather the list of user id's that follow @tolga_tezel
  //
  console.log(user + "start getFollowing");
  limiter.removeTokens(1, function(err, remainingRequests) {
    T.get('friends/ids', { id: user, count: 5000, stringify_ids: 1 },  function (err, data, response) {
      console.log(user + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
      console.log(user + ' limit reset: ' + response.headers['x-rate-limit-reset']);

      var timeout = 0;
      if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
        timeout = response.headers['x-rate-limit-reset'] - Date.now();
        console.log(user + ' until reset: ' + timeout/1000);
      }

      console.log(user + ' next cursor: ' + data.next_cursor_str);
      array.push.apply(array, data.ids);
      //console.log(data.users[0]);
      //console.log(response.headers);
      //debugger;
      if (data.next_cursor_str !== cursor_str){
        setTimeout(getFollowing, timeout, user, data.next_cursor_str, array, callback);
      }
      else
      callback(array, user);
    });
  });
};

var getUser = function (user, cursor_str, callback) {
  //
  //  gather the list of user id's that follow @tolga_tezel
  //
  limiterUser.removeTokens(1, function(err, remainingRequests) {
    T.get('users/show', { user_id: user },  function (err, data, response) {
      console.log(user + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
      console.log(user + ' limit reset: ' + response.headers['x-rate-limit-reset']);

      var timeout = 0;
      if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
        timeout = response.headers['x-rate-limit-reset'] - Date.now();
        console.log(user + ' until reset: ' + timeout/1000);
      }

      //console.log(data.users[0]);
      //console.log(response.headers);
      callback(data, user);
    });
  });
};



var saveFollowers = function(array, screen_name){
  //get only unique results
  var followers = [];
  var unique = {};
  for (var i in array){
    if ( typeof(unique[array[i]]) == "undefined"){
      followers.push(array[i]);
    }
    unique[array[i]] = 0;
  }

  console.log(screen_name + " gathered followers: " + followers.length);
  //debugger;

  followers = followers.map( function(follower){
    return {
      id: follower,
      service: 'twitter'
    };
  });

  followers.forEach(function(follower){
    collection.update({id: follower.id, service: follower.service},
      {$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
      {upsert: true},function(err, result){
        if (err){ console.log(err);}
        //console.log("added a follower, " + err);
      } );
    });

    collection.update({'screen_name': screen_name},
    {$set:{'followers': followers},
    $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
    function(err, result){
      console.log(screen_name + " added followers to user, " + err);
      sem.leave();
    } );
  };


  var saveFollowing = function(array, screen_name){

    //get only unique results
    var following = [];
    var unique = {};
    for (var i in array){
      if ( typeof(unique[array[i]]) == "undefined"){
        following.push(array[i]);
      }
      unique[array[i]] = 0;
    }
    console.log(screen_name + " gathered following: " + following.length);
    //debugger;

    //add other necessary fields
    following = following.map( function(friend){
      return {
        id: friend,
        service: 'twitter'
      };
    });

    //commit each to database
    following.forEach(function(friend){
      collection.update({id: friend.id, service: friend.service},
        {$currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
        {upsert: true},function(err, result){
          if (err){ console.log(err);}
          //console.log("added a friend, " + err);
        } );
      });

      //update original user
      collection.update({'screen_name': screen_name},
      {$set:{'following': following},
      $currentDate: {'collector.lastSavedDate': { $type: "timestamp"}}},
      function(err, result){
        console.log(screen_name + " added following to user, " + err);
        sem.leave();
        semUser.leave();
      });
    };

var saveUser = function(data, user_id){
  //update user
  var recentTweet = data.status;
  if (recentTweet){
    tweetCollection.update({'id': recentTweet.id},recentTweet, function(err, result){
      console.log(data.id + "added tweet");
    });
  }
  delete data.status;
  //data['$currentDate']= {'collector.lastSavedDate': { $type: "timestamp"}};
//  data['$setOnInsert']= { $currentDate: {'collector.insertedDate': { $type: "timestamp"}}};
  collection.update({'id': user_id}, data,
  function(err, result){
    console.log(data.screen_name + " saved user, " + err);
    semUserData.leave();
  });

};



MongoClient.connect(url, function(err, db_) {
  db = db_;
  collection = db.collection('socialGraph');
  tweetCollection = db.collection('tweets');

  var Process = function(){
    docs.forEach(function(doc){
      console.log(doc);
      if (typeof(doc.screen_name) == 'undefined'){
        //debugger;
        semUserData.take( function(){
          debugger;
          getUser(doc.id, "0", saveUser);
        });
      }
      else {
      semUser.take( function() {
      //  //debugger;
        console.log(doc.id);
        if (typeof(doc.followers) == 'undefined' || doc.followers.length === 0){
          sem.take( function(){
            console.log(doc.screen_name + 'before start getFollowers');
            getFollowers(doc.id, "0", [], saveFollowers);
          });
        }
        if (typeof(doc.following) == 'undefined' || doc.following.length === 0){
          sem.take( function(){
            getFollowing(doc.id, "0", [], saveFollowing);
          });
        }
        else {
          semUser.leave();
        }
      });
    } //else
  }); //for
  };

//  //debugger;
  collection.find({},
    {id: 1, screen_name: 1, followers: 1, following: 1}
  ).toArray(function(err, docs_){
    if (err) {
      console.log(err);
      return;
    }
    console.log("found " + docs_.length + " docs");
    docs = docs_.map( function(doc){
      return {
        id: doc.id,
        screen_name: doc.screen_name || null,
        followers: doc.followers || [],
        following: doc.following || []
      };
    });
    Process();
    /*
    if (docs[i].lists.length === 0){
    GetLists(user, "0", [], function (array){
    //save array
  });
}
*/

  }); // collection

});
