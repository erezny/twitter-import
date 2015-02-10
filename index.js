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

var RateLimiter = require('limiter').RateLimiter;
var limiter = new RateLimiter(2, 2*60*1000);

var sem = require('semaphore')(1);
var semUser = require('semaphore')(1);

var GetFollowers = function (user, cursor_str, array, callback) {
  //
  //  gather the list of user id's that follow @tolga_tezel
  //
  limiter.removeTokens(1, function(err, remainingRequests){
      T.get('followers/list', { screen_name: user, skip_status: 1, count: 200 },  function (err, data, response) {
        console.log(user + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
        console.log(user + ' limit reset: ' + response.headers['x-rate-limit-reset']);

      var timeout = 0;
      if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
        timeout = response.headers['x-rate-limit-reset'] - Date.now();
        console.log(user + ' until reset: ' + timeout/1000);
      }

      console.log(user + ' next cursor: ' + data.next_cursor_str);
      array.push.apply(array, data.users);
      //console.log(data.users[0]);
      //console.log(response.headers);
      debugger;
      if (data.next_cursor_str !== cursor_str){
        setTimeout(GetFollowers, timeout, user, data.next_cursor_str, array, callback);
      }
      else
        callback(array, user);
    });
  });
};

var GetFollowing = function (user, cursor_str, array, callback) {
  //
  //  gather the list of user id's that follow @tolga_tezel
  //
  limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('friends/list', { screen_name: user, skip_status: 1, count: 200 },  function (err, data, response) {
      console.log(user + ' limit remaining: ' + response.headers['x-rate-limit-remaining']);
      console.log(user + ' limit reset: ' + response.headers['x-rate-limit-reset']);

      var timeout = 0;
      if (parseInt(response.headers['x-rate-limit-remaining']) == 0){
        timeout = response.headers['x-rate-limit-reset'] - Date.now();
        console.log(user + ' until reset: ' + timeout/1000);
      }

      console.log(user + ' next cursor: ' + data.next_cursor_str);
      array.push.apply(array, data.users);
      //console.log(data.users[0]);
      //console.log(response.headers);
      debugger;
      if (data.next_cursor_str !== cursor_str){
        setTimeout(GetFollowing, timeout, user, data.next_cursor_str, array, callback);
      }
      else
        callback(array, user);
    });
  });
};





var saveFollowers = function(array, screen_name){
    //get only unique results
    var followers = [];
    var unique = {};
    for (var i in array){
      if ( typeof(unique[array[i].screen_name]) == "undefined"){
        followers.push(array[i]);
      }
      unique[array[i].screen_name] = 0;
    }

    console.log(screen_name + " gathered followers: " + followers.length);
    debugger;

    followers = followers.map( function(follower){
      follower.service = 'twitter';
      follower.followers = [];
      follower.following = [];
      follower.lists = [];
      return follower;
    });

    followers.forEach(function(follower){
      collection.update({screen_name: follower.screen_name, service: follower.service},
        follower, {upsert: true},function(err, result){
          if (err){ console.log(err);}
          //console.log("added a follower, " + err);
      } );
    });

    followers = followers.map(function(follower){
        return {
          id: follower.id,
          name: follower.name,
          screen_name: follower.screen_name};
    });

    collection.update({'screen_name': screen_name},
      {$set:{'followers': followers}}, function(err, result){
        console.log(screen_name + " added followers to user, " + err);
        sem.leave();
    } );
};


var saveFollowing = function(array, screen_name){

    //get only unique results
    var following = [];
    var unique = {};
    for (var i in array){
      if ( typeof(unique[array[i].screen_name]) == "undefined"){
        following.push(array[i]);
      }
      unique[array[i].screen_name] = 0;
    }
    console.log(screen_name + " gathered following: " + following.length);
    debugger;

    //add other necessary fields
    following = following.map( function(friend){
      friend.service = 'twitter';
      friend.following = [];
      friend.following = [];
      friend.lists = [];
      return friend;
    });

    //commit each to database
    following.forEach(function(friend){
      collection.update({screen_name: friend.screen_name, service: friend.service},
        friend, {upsert: true},function(err, result){
          if (err){ console.log(err);}
          //console.log("added a follower, " + err);
      } );
    });

    //reduce object space
    following = following.map(function(friend){
        return {
          id: friend.id,
          name: friend.name,
          screen_name: friend.screen_name};
    });

    //update original user
    collection.update({'screen_name': screen_name},
      {$set:{'following': following}}, function(err, result){
        console.log(screen_name + " added following to user, " + err);
        sem.leave();
        semUser.leave();
    });
};


MongoClient.connect(url, function(err, db) {
  collection = db.collection('socialGraph');

  collection.find().toArray(function(err, docs){
    for (var i in docs){
      if (typeof(docs[i].screen_name) == 'undefined'){
        continue;
      }
      semUser.take( function() {
        debugger;
        console.log(docs[i].screen_name);
        if (typeof(docs[i].followers) == 'undefined' || docs[i].followers.length === 0){
          sem.take( function(){
            GetFollowers(docs[i].screen_name, "0", [], saveFollowers);
          });
        }
        if (typeof(docs[i].following) == 'undefined' || docs[i].following.length === 0){
          sem.take( function(){
            GetFollowing(docs[i].screen_name, "0", [], saveFollowing);
          });
        }
        else {
          semUser.leave();
        }
      });
/*
      if (docs[i].lists.length === 0){
        GetLists(user, "0", [], function (array){
          //save array
        });
      }
*/

    }
  });
});
