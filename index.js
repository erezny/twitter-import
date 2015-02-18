
// #refactor:10 pull in dependencies

var assert = require('assert');
var config = require('./config/');
config.init();
var twitter = require('./lib/twitter/');
var util = require('./lib/util.js');
var events = require('events');
var engine = new events.EventEmitter();
var FriendsFollowersSem = require('semaphore')(2);
var FollowersSem = require('semaphore')(2);
var FriendsSem = require('semaphore')(2);
var BloomFilter = require('bloomfilter').BloomFilter;

var logger = require('tracer').colorConsole(config.env.logger);
config.logger = logger;
//logger.debug('hello %s',  'world', 123);

var docs = {};
var seed = {
  screen_name: 'erezny',
  id: 16876313,
  'id_str': '16876313',
  followers: [],
  friends: [],
  internal: {
    query_user: 0,
    query_followers: 1,
    query_friends: 1,
    expand_followers: 2,
    expand_friends: 2,
  }
};
docs[seed.id_str] = seed;

var blooms = {};
blooms.willQuery = new BloomFilter(
  4 * 1024 * 1024 * 256, // number of MB to allocate in  bits.
  20        // number of hash functions.
);

blooms.haveQueried = new BloomFilter(
  4 * 1024 * 1024 * 256, // number of bits to allocate.
  20        // number of hash functions.
);

blooms.purged = new BloomFilter(
  4 * 1024 * 1024 * 256, // number of bits to allocate.
  20        // number of hash functions.
);


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

twitter.init(config, function(){

  engine.emit('dbready');
});


engine.once('dbready', function(){

  twitter.logger.init(config, twitter.controller.db);
  //query_mongo_user(seed);
  engine.emit('get_next_user');
});

var queryDBList = [];

engine.on('get_next_user', function () {
  logger.debug("Fetching next user");
  twitter.controller.collection.find(
    {$or: [{'state.query_followers': true},
          {'state.query_friends': true},
          {'state.expand_followers': {$gt: 0}},
          {'state.expand_friends': {$gt: 0}}]}).sort(
            {followers_count: 1}
          ).skip(3).next(function(err, results){
    if (! results){
      logger.error( "no more users to query, must be close to done");
    }
    engine.emit("query_mongo_user", results);
  });
});

engine.on('query_mongo_user', function(data){
  logger.trace('engine.query_mongo_user received %j', data);
  // twitter.controller.queryUser(user, callback);
  query_mongo_user(data);
});


function query_mongo_user (data) {
  if ( ! data ) return;
  logger.info('db lookup for user %s', data.id_str);
  twitter.controller.queryUser(data, function(err, results)
  // query user from database
  {
//    debugger;
    logger.trace('%j', err);
    logger.trace('%j', results);

    var foundSomething = 1;
    if( ! results){
      assert(true, 'expected to find ' + data.id_str + ' in database');
      foundSomething = 0 ;
      logger.info('User not found in database, %s', data.id_str);
      blooms.willQuery.add(data.id_str);
      engine.emit('query_twitter_user', data);
      return;
    }
    logger.info('queried user %s from database', results.id_str);
    logger.debug('query date: %s, compare date %s, results',
      new Date(results.internal.user_queried),
      new Date(Date.now() - 24*60*60*1000),
      new Date(results.internal.user_queried) < (new Date(Date.now() - 24*60*60*1000)));

    if (new Date(results.internal.user_queried) < (new Date(Date.now() - 24*60*60*1000))){
      // too old, requery
      engine.emit('query_twitter_user', data);

    }


      logger.trace('queried from database: %j', results);
      docs[results.id_str] = results;

//      FriendsFollowersSem.take(function(){
      logger.info('expanding network of %s', results.id_str);
      // query friends and followers if needed
      //if we don't have the correct number of followers, query it
//      debugger;
      if (docs[results.id_str].state.query_friends || docs[results.id_str].state.expand_friends)
      {
        if ( (docs[results.id_str].friends_count - docs[results.id_str].friends.length) > 0 &&
              docs[results.id_str].friends_count < 5000){
          docs[results.id_str].friends = [];
          engine.emit('query_twitter_friends', results);
        }
      }

      if (docs[results.id_str].state.query_followers || docs[results.id_str].state.expand_followers){
        if ( (docs[results.id_str].followers_count - docs[results.id_str].followers.length) > 0 &&
              docs[results.id_str].followers_count  < 20000 ){
          docs[results.id_str].followers = [];
          engine.emit('query_twitter_followers', results);
        }
      }
//      });

  });
}


// query_twitter_user
// parameters:
  // data: {
  //  id_str: id to query
  //  internal.expand_followers: how deep to continue searching for followers.
  //                    0 don't even do this one
  //                    1 look up basic info on followers
  //                    2 look up follower's followers
  //  internal.expand_following: see above
  //  }

var queryUserList = [];
engine.on('query_twitter_user', function(data){
  logger.trace('engine.query_twitter_user received %j', data);
  query_twitter_user(data);
});

function query_twitter_user(data){
  logger.debug('fn query_twitter_user received %s', data.id_str);

  var id_str = data.id_str;
  var expand_followers = data.internal.expand_followers;
  var expand_friends = data.internal.expand_friends;

  logger.info('query_twitter_user %s, %d, %d', data.id_str, expand_followers, expand_friends);

  //as close to the query as possible,
  //don't query if we already have already queried the user
  if (blooms.haveQueried.test(data.id_str)){
    return;
  }

  //query user from twitter
  twitter.api.queryUser(data, function(err, results)
  {

    //make sure this query doesn't happen again and either
    //overwrite followers/following in the store or
    //waste api calls regathering information
    blooms.haveQueried.add(id_str);

    //if there's an error, get out now
    // TODO add test for valid data
    if (err){
      logger.error("twitter api error querying %s, %s", id_str, err);
      return;
    }

    //strip extra fields
    delete results.status;

    //add internal fields
    results.internal = {
      user_queried: new Date(),
      query_user: 0,
      query_followers: (expand_followers > 0),
      query_friends: (expand_friends > 0),
      expand_followers: expand_followers,
      expand_friends: expand_friends,
    };

    results.state = {
      query_followers: (expand_followers > 0),
      query_friends: (expand_friends > 0),
      expand_followers: expand_followers,
      expand_friends: expand_friends,
    };

    //throw away anything we have.
    results.friends = [];
    results.followers = [];

    //save user into memory
    docs[results.id_str] = results;
    engine.emit('accumulate_user_changes', {id_str: results.id_str});


    if (docs[results.id_str].internal.query_friends)
    {
      if ( (docs[results.id_str].friends_count - docs[results.id_str].friends.length) > 5 &&
            docs[results.id_str].friends_count < 5000){
        docs[results.id_str].friends = [];
        engine.emit('query_twitter_friends', results);
      }
    }

    if (docs[results.id_str].internal.query_followers){
      if ( (docs[results.id_str].followers_count - docs[results.id_str].followers.length) > 5 &&
            docs[results.id_str].followers_count  < 20000 ){
        docs[results.id_str].followers = [];
        engine.emit('query_twitter_followers', results);
      }
    }

  });

}

var queryFriendsList = [];

engine.on('query_twitter_friends', function(data){
  logger.debug('engine.query_twitter_friends received %j', data);
  queryFriendsList.push(data);
  FriendsSem.take(2, function(){
    FriendsFollowersSem.take(2, function(){
      logger.trace('spawned query_twitter_friends');
      query_twitter_friends(queryFriendsList.shift());
    });
  });
});

function query_twitter_friends(data){

  logger.debug('run query_twitter_friends %s', data.id_str);
  data.internal= docs[data.id_str].internal;
  var id_str = data.id_str;
  //query followers
  twitter.api.queryFriendsUsers(data, callback_query_twitter_friends);
  twitter.api.queryFriends(data, callback_query_twitter_friends_ids);

}

function callback_query_twitter_friends(err, results, finished, data, next_cursor_str)
  {
    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", data.id_str, err);
      return;
    }

    results.forEach(function(user){
      // TODO overwrite internal if values greater than current
          twitter.controller.queryUserExists(user, function(err, results){
            if (results){
              return;
            }
          //save user
          //strip extra fields
          delete user.status;

          //add internal fields
          user.internal = {
            user_queried: new Date(),
            query_user: 0,
            query_followers: (data.internal.expand_followers - 1 > 0)? 1 : 0,
            query_friends: (data.internal.expand_friends - 1 > 0)? 1 : 0,
            expand_followers: data.internal.expand_followers - 1,
            expand_friends: data.internal.expand_friends - 1,
          };

              user.state = {
              query_followers: (data.internal.expand_followers - 1 > 0)? 1 : 0,
              query_friends: (data.internal.expand_friends - 1 > 0)? 1 : 0,
              expand_followers: data.internal.expand_followers - 1,
              expand_friends: data.internal.expand_friends - 1,
              };


          user.friends = [];
          user.followers = [];
          //save user into memory
          docs[user.id_str] = user;


          //save results to database
          engine.emit('accumulate_user_changes', {id_str: user.id_str, purge: 1});

        });

    });

    //finished no matter what right now

    //when finished
    if (finished){
    FriendsFollowersSem.leave();
    FriendsSem.leave();
	  docs[data.id_str].state.expand_friends = 0;
    engine.emit('accumulate_user_changes', {id_str: data.id_str});

    }
    else
    {
      twitter.api.queryFriendsUsers(data,next_cursor_str, callback_query_twitter_friends);
    }


}

function callback_query_twitter_friends_ids(err, results, finished, data, next_cursor_str)
  {
    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", data.id_str, err);
      return;
    }

    //append set of friends to data's friends array
    docs[data.id_str].friends = docs[data.id_str].friends.concat(
        results
      );
    logger.debug('query_twitter_friends 2 %s, new: %d, accumulated: %d/%d',
      data.id_str,
      results.length,
      docs[data.id_str].friends.length,
      docs[data.id_str].friends_count);

    //when finished
    if (finished){

      //enforce unique values
      docs[data.id_str].friends = util.uniqArray(docs[data.id_str].friends);
      logger.info('query_twitter_friends %s total: %d', data.id_str, docs[data.id_str].friends.length);

      //set that we're finishe dquerying friends
      docs[data.id_str].state.query_friends = 0;

      //save results to database
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      FriendsFollowersSem.leave();
      FriendsSem.leave();
    }
    else
    {
      twitter.api.queryFriends(data,next_cursor_str, callback_query_twitter_friends_ids);
    }


}

var queryFollowersList = [];
engine.on('query_twitter_followers', function(data){
  logger.debug('engine.query_twitter_followers received %j', data);
  queryFollowersList.push(data);
  FollowersSem.take(2, function(){
    FriendsFollowersSem.take(2, function() {
      logger.trace('spawned query_twitter_followers');
      query_twitter_followers(queryFollowersList.shift());
    });
  });
});

function query_twitter_followers(data)
{

  logger.debug('run query_twitter_followers %s', data.id_str);
  data.internal= docs[data.id_str].internal;
  var id_str = data.id_str;
  //query followers
  twitter.api.queryFollowersUsers(data, callback_query_twitter_followers);

  twitter.api.queryFollowers(data, callback_query_twitter_followers_ids);
}

function callback_query_twitter_followers(err, results, finished, data, next_cursor_str)
  {
    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", id_str, err);
      return;
    }

    results.forEach(function(user){
      // TODO overwrite internal if values greater than current
          twitter.controller.queryUserExists(user, function(err, results){
            if (results){
              return;
            }
          //save user
          //strip extra fields
          delete user.status;

          //add internal fields
          user.internal = {
            user_queried: new Date(),
            query_user: 0,
            query_followers: (data.internal.expand_followers - 1 > 0) ? 1 : 0,
            query_friends: (data.internal.expand_friends - 1 > 0) ? 1: 0,
            expand_followers: data.internal.expand_followers - 1,
            expand_friends: data.internal.expand_friends - 1,
          };

              user.state = {
              query_followers: (data.internal.expand_followers - 1 > 0)? 1 : 0,
              query_friends: (data.internal.expand_friends - 1 > 0)? 1 : 0,
              expand_followers: data.internal.expand_followers - 1,
              expand_friends: data.internal.expand_friends - 1,
              };
          user.friends = [];
          user.followers = [];

          //save user into memory
          docs[user.id_str] = user;

          //save results to database and purge
          engine.emit('accumulate_user_changes', {id_str: user.id_str, purge: 1});
        });

    });


    //when finished
    if (finished){
      FriendsFollowersSem.leave();
      FollowersSem.leave();
      logger.info("finished saving new followers of %s as users", data.id_str);
      docs[data.id_str].state.expand_followers = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
    }
    else
    {

     twitter.api.queryFollowersUsers(data,next_cursor_str, callback_query_twitter_followers);
      //I don't need that many of these yet.
    }

  }


  function callback_query_twitter_followers_ids(err, results, finished, data, next_cursor_str)
    {
      //if there's an error, get out now
      // TODO add test for valid data
      if (err)
      {
        logger.error("twitter api error querying %s, %s", id_str, err);
        return;
      }

      //append set of followers to data's followers array
      docs[data.id_str].followers =
        docs[data.id_str].followers.concat(results);

      logger.debug('query_twitter_followers 2 %s, new: %d, accumulated: %d/%d',
        data.id_str,
        results.length,
        docs[data.id_str].followers.length,
        docs[data.id_str].followers_count);

      //when finished
      if (finished){
        //enforce unique values
        docs[data.id_str].followers = util.uniqArray(docs[data.id_str].followers);

        logger.info('query_twitter_followers %s total: %d', data.id_str, docs[data.id_str].followers.length);

        //set that we're finishe dquerying followers
        docs[data.id_str].state.query_followers = 0;

        //save results to database
        engine.emit('accumulate_user_changes', {id_str: data.id_str});
        FriendsFollowersSem.leave();
        FollowersSem.leave();
      }
      else
      {

        twitter.api.queryFollowers(data,next_cursor_str, callback_query_twitter_followers);
      }

    }

engine.on('check_remove_doc', function(data){
  logger.debug('check for purge %s, %j', data.id_str, docs[data.id_str].state);
  if (  data.purge ){

      logger.debug("purging %s", data.id_str);
       delete docs[data.id_str];
  }
  else if( ! ( docs[data.id_str].state.expand_followers ||
      docs[data.id_str].state.expand_friends ||
      docs[data.id_str].state.query_follwers ||
      docs[data.id_str].state.query_friends ))
  {
    logger.debug("purging %s", data.id_str);
     delete docs[data.id_str];
    engine.emit('get_next_user');
  }
});

engine.on('accumulate_user_changes', function(data){
  var id_str = data.id_str;
  logger.trace('accumulate_user_changes %s', data.id_str );
  //save user object
  twitter.controller.updateUser(docs[data.id_str], function(err){

    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", id_str, err);
      return;
    }

    logger.trace('successfully saved user %s', data.id_str);

    logger.trace('%j', docs[data.id_str]);

    engine.emit('check_remove_doc', data);


  });

});

setInterval(function() {
  logger.info('Stats: store db/api followers/following :%d %d/%d %d/%d',
    Object.keys(docs).length,
    queryDBList.length,
    queryUserList.length,
    queryFriendsList.length,
    queryFollowersList.length);
}, 10*1000);

/*
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
*/
