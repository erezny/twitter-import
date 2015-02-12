
// #refactor:10 pull in dependencies

var assert = require('assert');
var config = require('./config/');
var twitter = require('./lib/twitter/');
var util = require('./lib/util.js');
var events = require('events');
var engine = new events.EventEmitter();
var friendsSem = require('semaphore')(1);


var docs = [];
var seed = {
  screen_name: 'erezny',
  id: 16876313
};

var openEvents = 0;
engine.on('newEvent', function(){
  openEvents++;
});

engine.on('finishedEvent', function(){
  openEvents--;
  if ( ! openEvents ){
    console.log('closed db connection');
    twitter.controller.db.close();
  }
});


// #refactor:10 get arguments

twitter.controller.init(function(){

  engine.emit('dbready');
});

engine.once('dbready', function(){
  engine.emit('newEvent');
  twitter.controller.queryUser(seed, function(err, user){
      assert(err === null, 'query returned an error');
      //this should give us followers/following
      //console.log(user);
      assert(typeof(user.followers)!=='Array');
      assert(typeof(user.following)!=='Array');

      for (i in user.following){
        if (user.following[i].id === null){
          continue;
        }
        engine.emit('checkUser', user.following[i]);
        engine.emit('newEvent');
        console.log('emit checkUser'+ user.following[i].id);
      }
      for (i in user.followers){
        if (user.followers[i].id === null){
          continue;
        }
        engine.emit('checkUser', user.followers[i]);
        engine.emit('newEvent');
        console.log('emit checkUser'+ user.followers[i].id);
      }
      engine.emit('finishedEvent');
  });
});

engine.on('checkUser', function(user){

  console.log('on checkUser ' + user.id);

  engine.emit('newEvent');
  twitter.controller.queryUser(user, function(err, user){
    //console.log(err);
    //console.log(user);
    console.log('queried from database ' + (user.screen_name || user.id));

    if (! user.screen_name){

      console.log(user.id + ' no screen name, look it up.');

      engine.emit('newEvent');
      engine.emit('queryUser', user);

    }
    else {
      console.log(user.screen_name + " found in database. find followers");
    }

    if ( user.followers === null || user.followers.length == 0 ||
        ( user.followers_count - user.followers.length ) == -1)
    {
      console.log(user.id + "need to query followers");
      engine.emit('newEvent');
      engine.emit('queryFollowers', user);
    }

    if ( user.following === null || user.following.length == 0 ||
        (user.following_count - user.following.length ) == -1)
    {
      console.log(user.id + "need to query following");
      engine.emit('newEvent');
      engine.emit('queryFollowers', user);
    }

    engine.emit('finishedEvent');
  });

  engine.emit('finishedEvent');
});

// #refactor:10 run queries.

engine.on('queryUser', function(user){
  twitter.api.getUser(user, function(err, user_){

    console.log(user_.screen_name + " queried from twitter");

    engine.emit('newEvent');
    twitter.controller.updateUser(user_, function(err, result){
      console.log("saved " + user_.screen_name);
      engine.emit('finishedEvent');
    });

    engine.emit('finishedEvent');
  });
});

engine.on('queryFollowers', function(user)
{
  friendsSem.take(function(){
    var followers = [];
    twitter.api.getFollowers(user, function(results, finished){

      followers.push.apply(followers, results);

      if (finished){
        //save
        followers = util.uniqArray(followers, function(follower) {return follower.id;});
        twitter.conroller.saveFollowers(user, followers);
        engine.emit('finishedEvent');
        friendsSem.leave();
      }

    });
  });
});

engine.on('queryFollowing', function(user)
{
  friendsSem.take(function(){

    var following = [];
    twitter.api.getFollowing(user, function(results, finished){

      following.push.apply(following, results);

      if (finished){
        //save
          following = util.uniqArray(following, function(friend) {return friend.id;});
          twitter.controller.saveFollowing(user, following);
          engine.emit('finishedEvent');
          friendsSem.leave();
      }

    });
  });

});

// #refactor:5 listen on api
