// #refactor:10 use promises

exports = {

  // #refactor:10 make private
  MongoClient: require('mongodb').MongoClient,
  url: 'mongodb://localhost:27017/viewer-dev',
  db: null,
  collection: null,
  tweetCollection: null,

  // #refactor:10 fold into update with schema
  // #refactor:10 update 1 object etc

  updateUser: function(user){


  },

  saveFollowers: function(user){
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
    },


    saveFollowing: function(user){

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
      },

  saveUser: function(user){
    // #update:0  user
    var recentTweet = user.status;
    if (recentTweet){
      tweetCollection.update({'id': recentTweet.id},recentTweet, function(err, result){
        console.log(user.id + "added tweet");
      });
    }
    delete user.status;
    //data['$currentDate']= {'collector.lastSavedDate': { $type: "timestamp"}};
  //  data['$setOnInsert']= { $currentDate: {'collector.insertedDate': { $type: "timestamp"}}};
    collection.update({'id': user.id}, user,
    function(err, result){
      console.log(user.id + " saved user, " + err);
      semUserData.leave();
    });

  }

}
