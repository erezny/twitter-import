
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
  id: 16876313,
  'id_str': '16878313'
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

engine.emit('newEvent');

// #refactor:10 get arguments

twitter.controller.init(function(){

  engine.emit('dbready');
});

engine.once('dbready', function(){

  engine.emit('newEvent');
  //debugger;
  twitter.controller.queryUser(seed, function(err, user){
      assert(err === null, 'query returned an error');
      console.log(err);
      //this should give us followers/following
      console.log(user);
      if (! user){
        engine.emit('finishedEvent');
        engine.emit('checkUser', user);
        return;
      }

      if ( ! ("folowing" in user || "followers" in user) ){
        engine.emit('checkUser', user);
      }
      for (var i in user.following){
        if (user.following[i].id_str === null){
          continue;
        }
        engine.emit('checkUser', user.following[i]);
        engine.emit('newEvent');
        console.log('emit checkUser'+ user.following[i].id_str);
      }
      for (var j in user.followers){
        if (user.followers[j].id_str === null){
          continue;
        }
        engine.emit('checkUser', user.followers[i]);
        engine.emit('newEvent');
        console.log('emit checkUser '+ user.followers[i].id_str);
      }
      engine.emit('finishedEvent');
  });
});

engine.on('checkUser', function(user){

  console.log('on checkUser ' + user.id_str);
  getAllUserInfo(user);

});

var getAllUserInfo = function(user){

  engine.emit('newEvent'); //query user event
  twitter.controller.queryUser(user, function(err, user){
    //console.log(err);
    console.log(user);

    if (! user){
      engine.emit('finishedEvent');
      return;
    }

    var alert_user;
    if ("screen_name" in user){
      alert_user = user.screen_name;
    }
    else
      alert_user = user.id_str;

    console.log('queried from database ' + (user.screen_name || user.id_str));

    if (! user.screen_name){

      console.log(user.id_str + ' no screen name, look it up.');

      engine.emit('newEvent');
      engine.emit('queryUser', user);

    }
    else {
      console.log(user.screen_name + " found in database. find followers");
    }

    if ( (! user.followers) || user.followers.length == 0 ||
        ( user.followers_count - user.followers.length ) == -1)
    {
      console.log(user.id_str + "need to query followers");
      engine.emit('newEvent');
      engine.emit('queryFollowers', user);
    }

    if ( ( ! user.following) || user.following.length == 0 ||
        (user.following_count - user.following.length ) == -1)
    {
      console.log(user.id_str + "need to query following");
      engine.emit('newEvent');
      engine.emit('queryFollowers', user);
    }

    engine.emit('finishedEvent'); //query user event
  });

  engine.emit('finishedEvent');
};

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
        followers = util.uniqArray(followers);
        twitter.controller.saveFollowers(user, followers, function(){

          engine.emit('finishedEvent');
          friendsSem.leave();
        });
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
          following = util.uniqArray(following);
          twitter.controller.saveFollowing(user, following, function(){
            engine.emit('finishedEvent');
            friendsSem.leave();
          });
      }

    });
  });

});

// #refactor:0 get all engine calls down here, call functions up there

// #refactor:5 listen on api
