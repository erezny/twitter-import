
// #refactor:10 pull in dependencies

var assert = require('assert');
var config = require('./config/');
config.init('dev');
var twitter = require('./lib/twitter/');
var util = require('./lib/util.js');
var events = require('events');
var engine = new events.EventEmitter();
var FriendsFollowersSem = require('semaphore')(1);
var FollowersSem = require('semaphore')(1);
var FriendsSem = require('semaphore')(1);
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
  query_mongo_user(seed);
});

var queryDBList = [];
engine.on('query_mongo_user', function(data){
  logger.trace('engine.query_mongo_user received %j', data);
  // twitter.controller.queryUser(user, callback);

  twitter.controller.queryUserExists(data, function(err, results){
    if (results){
      queryDBList.push(data);
    }
    else
    {
      engine.emit("query_twitter_user", data);
    }
  });
});

setInterval(function() {
  logger.trace('spawn query_mongo_user... ');
  if (queryDBList.length > 0 && queryFriendsList.length < 10 &&
    queryFollowersList.length < 10 && Object.keys(docs).length < 20) {
    logger.debug('spawned query_mongo_user');
    query_mongo_user(queryDBList.shift());
  }
}, 2*1000);


function query_mongo_user (data) {
  logger.info('db lookup for user %s', data.id_str);
  twitter.controller.queryUser(data, function(err, results)
  // query user from database
  {
//    debugger;
    logger.trace('%j', err);
    logger.trace('%j', results);

    var foundSomething = 1;
    if( ! results){
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
      results.internal = {
        // user_queried: new Date(),
        query_user: 0,
        expand_followers: data.internal.expand_followers,
        expand_friends: data.internal.expand_friends,
        query_followers: (data.internal.expand_followers > 0) ,
        query_friends: (data.internal.expand_friends > 0),
      };

      //throw away anything we have.
      results.friends = results.friends || [];
      results.followers = results.friends || [];

      //logger.trace('%j', results);
      docs[results.id_str] = results;

      queryTemplate = {
        expand_friends: docs[results.id_str].internal.expand_friends - 1,
        expand_followers: docs[results.id_str].internal.expand_followers - 1,
      };

//      FriendsFollowersSem.take(function(){
      logger.info('expanding network of %s', results.id_str);
      // query friends and followers if needed
      //if we don't have the correct number of followers, query it
//      debugger;
      if (docs[results.id_str].internal.query_friends)
      {
        if ( (docs[results.id_str].friends_count - docs[results.id_str].friends.length) > 5 &&
              docs[results.id_str].friends_count < 20000){
          docs[results.id_str].friends = [];
          engine.emit('query_twitter_friends', {id_str: results.id_str});
        }
        else if (docs[results.id_str].internal.expand_friends)
        {
          docs[results.id_str].internal.query_friends = 0;
          docs[results.id_str].friends.forEach(safeQuery, queryTemplate);
        }
      }

      if (docs[results.id_str].internal.query_followers){
        if ( (docs[results.id_str].followers_count - docs[results.id_str].followers.length) > 5 &&
              docs[results.id_str].followers_count  < 20000 ){
          docs[results.id_str].followers = [];
          engine.emit('query_twitter_followers', {id_str: results.id_str});
        }
        else if (docs[results.id_str].internal.expand_followers)
        {
          docs[results.id_str].internal.query_followers = 0;
          docs[results.id_str].followers.forEach(safeQuery, queryTemplate);
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
  queryUserList.push(data);
});

setInterval(function() {
  logger.trace('spawn query_twitter_user... ');
  if (queryUserList.length > 0) {
    logger.trace('spawned query_twitter_user');
    query_twitter_user(queryUserList.shift());
  }
}, (1/180)*15*60*1000);

function query_twitter_user(data){
  logger.debug('fn query_twitter_user received %j', data);

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

    //throw away anything we have.
    results.friends = [];
    results.followers = [];

    //save user into memory
    docs[results.id_str] = results;

    //save results to database
    engine.emit('accumulate_user_changes', {id_str: results.id_str});
    if (results.internal.query_followers || results.internal.query_friends){
      queryDBList.push(results);
    }
  });

}

var queryFriendsList = [];
engine.on('query_twitter_friends', function(data){
  logger.debug('engine.query_twitter_friends received %j', data);
  queryFriendsList.push(data);
});

setInterval(function() {
  logger.trace('spawn query_twitter_friends... ');
  if (queryFriendsList.length >0) {
    FriendsSem.take(function(){
      FriendsFollowersSem.take(function(){
        logger.trace('spawned query_twitter_friends');
        query_twitter_friends(queryFriendsList.shift());
      });
    });
  }
}, 5*1000); // 15 every 15 minutes

function query_twitter_friends(data){

  logger.debug('run query_twitter_friends %s', data.id_str);

  var id_str = data.id_str;
  //query followers
  twitter.api.queryFriendsUsers(data, callback_query_twitter_friends);

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

    //append set of friends to data's friends array
    docs[data.id_str].friends = docs[data.id_str].friends.concat(
        results.map(function(user){
          return user.id_str;
        })
      );
    results.forEach(function(user){
      // TODO overwrite internal if values greater than current
        if (blooms.haveQueried.test(user.id_str)){
          //do nothing
        }
        else if (blooms.willQuery.test(user.id_str)){
          //do nothing
        }
        else {
          //save user
          //strip extra fields
          delete user.status;

          //add internal fields
          user.internal = {
            user_queried: new Date(),
            query_user: 0,
            query_followers: (docs[data.id_str].internal.expand_followers - 1 > 0),
            query_friends: (docs[data.id_str].internal.expand_friends - 1 > 0),
            expand_followers: docs[data.id_str].internal.expand_followers - 1,
            expand_friends: docs[data.id_str].internal.expand_friends - 1,
          };

          //throw away anything we have.
          user.friends = [];
          user.followers = [];

          //save user into memory
          docs[user.id_str] = user;

          //save results to database
          engine.emit('accumulate_user_changes', {id_str: user.id_str});
          if (user.internal.query_followers || user.internal.query_friends){
            queryDBList.push(user);
          }
        }
    });
    logger.debug('query_twitter_friends 2 %s, new: %d, accumulated: %d/%d',
      data.id_str,
      results.length,
      docs[data.id_str].friends.length,
      docs[data.id_str].friends_count);

    //when finished
    if (finished){

      //enforce unique values
      docs[data.id_str].friends = util.uniqArray(docs[data.id_str].friends);

      //set that we're finishe dquerying friends
      docs[data.id_str].internal.query_friends--;

      //save results to database
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      FriendsFollowersSem.leave();
      FriendsSem.leave();
    }
    else
    {
      twitter.api.queryFriendsUsers(data,next_cursor_str, callback_query_twitter_friends);
    }


}

var queryFollowersList = [];
engine.on('query_twitter_followers', function(data){
  logger.debug('engine.query_twitter_followers received %j', data);
  queryFollowersList.push(data);
});

setInterval(function() {
  logger.trace('spawn query_twitter_followers... ');
  if (queryFollowersList.length > 0) {
    FollowersSem.take(function(){
    FriendsFollowersSem.take(function() {
        logger.trace('spawned query_twitter_followers');
        query_twitter_followers(queryFollowersList.shift());
      });
    });
  }
}, 5*1000);

function query_twitter_followers(data)
{

  logger.debug('run query_twitter_followers %s', data.id_str);

  var id_str = data.id_str;
  //query followers
  twitter.api.queryFollowersUsers(data, callback_query_twitter_followers);
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

    //append set of followers to data's followers array
    docs[data.id_str].followers = docs[data.id_str].followers.concat(
        results.map(function(user){
          return user.id_str;
        })
      );
    results.forEach(function(user){
      // TODO overwrite internal if values greater than current
        if (blooms.haveQueried.test(user.id_str)){
          //do nothing
        }
        else if (blooms.willQuery.test(user.id_str)){
          //do nothing
        }
        else {
          blooms.haveQueried.add(user.id_str);
          blooms.willQuery.add(user.id_str);
          //save user
          //strip extra fields
          delete user.status;

          //add internal fields
          user.internal = {
            user_queried: new Date(),
            query_user: 0,
            query_followers: (docs[data.id_str].internal.expand_followers - 1 > 0),
            query_friends: (docs[data.id_str].internal.expand_friends - 1 > 0),
            expand_followers: docs[data.id_str].internal.expand_followers - 1,
            expand_friends: docs[data.id_str].internal.expand_friends - 1,
          };

          //throw away anything we have.
          user.friends = [];
          user.followers = [];

          //save user into memory
          docs[user.id_str] = user;

          //save results to database
          engine.emit('accumulate_user_changes', {id_str: user.id_str});
          if (user.internal.query_followers || user.internal.query_friends){
            queryDBList.push(user);
          }
        }
    });

    logger.debug('query_twitter_followers 2 %s, new: %d, accumulated: %d/%d',
      data.id_str,
      results.length,
      docs[data.id_str].followers.length,
      docs[data.id_str].followers_count);

    //when finished
    if (finished){

      logger.debug('query_twitter_followers %s total: %d', data.id_str, docs[data.id_str].followers.length);

      //enforce unique values
      docs[data.id_str].followers = util.uniqArray(docs[data.id_str].followers);

      logger.debug('query_twitter_followers %s total: %d', data.id_str, docs[data.id_str].followers.length);

      //set that we're finishe dquerying followers
      docs[data.id_str].internal.query_followers--;

      //save results to database
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      FriendsFollowersSem.leave();
      FollowersSem.leave();
    }
    else
    {

      twitter.api.queryFollowersUsers(data,next_cursor_str, callback_query_twitter_followers);
    }

  }

function safeQuery(id_str)
{
  logger.trace("Might query " + id_str);
  // if we haven't already queried this users
  if (! blooms.willQuery.test(id_str))
  {
    blooms.willQuery.add(id_str);
    var queryObj = {
      id_str: id_str,
      internal: {
        query_user: 1,
        expand_friends: this.expand_friends,
        expand_followers: this.expand_followers,
      },
    };
    //query it
    engine.emit('query_mongo_user',queryObj);
  }

}

engine.on('accumulate_user_changes', function(data){
  var id_str = data.id_str;
  logger.trace('accumulate_user_changes %s, %d, %d, %d', data.id_str,
        docs[data.id_str].internal.query_user ,
        docs[data.id_str].internal.query_followers ,
        docs[data.id_str].internal.query_friends
  );
  logger.trace('current user stats: %s, followers: %d, friends: %d',
        data.id_str,
        docs[data.id_str].followers.length,
        docs[data.id_str].friends.length);

        // if any queries are not complete, get out.
  var user_not_finished = docs[data.id_str].internal.query_user ||
              docs[data.id_str].internal.query_followers ||
              docs[data.id_str].internal.query_friends ;

  //if we just grabbed it from the api, save and throw out,
  //and queue up for later
  var only_api = ! docs[data.id_str].internal.query_user &&
                          docs[data.id_str].internal.query_followers &
                          docs[data.id_str].internal.query_friends ;

  //save user object
  twitter.controller.updateUser(docs[data.id_str], function(err){

    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", id_str, err);
      return;
    }

    logger.trace('successfully saved user %s', data.id_str);

    // if we just queried the api, throw out the cached copy,
    // we'll query from the database when we need it again.
    if (only_api){
        delete docs[data.id_str];
        return;
    }

    // if any queries are not complete, get out.
    if ( user_not_finished || blooms.purged.test(data.id_str)) {
      // nothing to do right now.
      return;
    }

    blooms.purged.add(data.id_str);

    logger.trace('accumulate_user_changes 2 %s', data.id_str);

    queryTemplate = {
      expand_friends: docs[data.id_str].internal.expand_friends - 1,
      expand_followers: docs[data.id_str].internal.expand_followers - 1,
    };

    logger.trace('%j', docs[data.id_str]);

    // expand network

    // query additional friends
    if (docs[data.id_str].internal.expand_friends){

      docs[data.id_str].friends.forEach(safeQuery, queryTemplate);

    }

    // query additional followers
    if (docs[data.id_str].internal.expand_followers){

      docs[data.id_str].followers.forEach(safeQuery, queryTemplate);

    }

    // trash old data;
    setTimeout(function(){
      delete docs[data.id_str];
    }, 10000);

    logger.debug('finished with user %s', data.id_str);

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
