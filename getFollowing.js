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



var username = 'erezny';

var userObj = {
    service: 'twitter',
    screen_name: username,
    followers: [],
    following: [],
    lists: [],
};


var GetFollowing = function (user, cursor_str, callback) {
  //
  //  gather the list of user id's that follow @tolga_tezel
  //
  T.get('friends/list', { screen_name: user, skip_status: 1, count: 200 },  function (err, data, response) {
    console.log('limit remaining: ' + response.headers['x-rate-limit-remaining']);
    console.log('next cursor: ' + data.next_cursor_str);
    userObj.following.push.apply(userObj.following, data.users);
    //console.log(data.users[0]);
    //console.log(response.headers);
    console.log(user);
    if (data.next_cursor_str != cursor_str){
      GetFollowing(user, data.next_cursor_str, callback);
    }
    else
      callback();
  });
};

MongoClient.connect(url, function(err, db_) {
  db = db_;
  collection = db.collection('socialGraph');

/*
  collection.update({'screen_name': userObj.screen_name, service: 'twitter'},
    userObj, {upsert: true}, function(err, result){
      console.log("added original user, " + err);
    } );
*/

  GetFollowing(username, '0', function(){

    //get only unique results
    var following = [];
    var unique = {};
    for (var i in userObj.following){
      if ( typeof(unique[userObj.following[i].screen_name]) == "undefined"){
        following.push(userObj.following[i]);
      }
      unique[userObj.following[i].screen_name] = 0;
    }
    console.log("gathered following: " + following.length);


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
    userObj.following = following.map(function(friend){
        return {
          id: friend.id,
          name: friend.name,
          screen_name: friend.screen_name};
    });

    //update original user
    collection.update({'screen_name': userObj.screen_name},
      {$set:{'following': userObj.following}}, function(err, result){
        console.log("added following to original user, " + err);
        process.exit();
    } );
  });

});
