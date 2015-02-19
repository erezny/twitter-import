
// #refactor:10 pull in dependencies

var assert = require('assert');

// TODO can the init be removed by using a constructor?
var config = require('./config/');
config.init();

var twitter = require('./lib/twitter/');
var util = require('./lib/util.js');

var events = require('events');
var engine = new events.EventEmitter();

var RSVP = require('rsvp');

var queryFriendsSem = require('semaphore')(1);
var queryFollowersSem = require('semaphore')(1);

// TODO change logger type based on config file
var logger = require('tracer').colorConsole(config.env.logger);
config.logger = logger;

var docs = {};

// TODO: take seed from command line or API
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

var openEvents = 0;
engine.on('newEvent', function()
{
  openEvents++;
});

engine.on('finishedEvent', function()
{
  openEvents--;
  if ( ! openEvents ){
    console.log('closed db connection');
    twitter.controller.db.close();
  }
});

engine.emit('newEvent');

// this is the start button for the engine.
twitter.init(config, function(){
  engine.emit('newEvent');
  engine.emit('dbready');
});

// #refactor:10 can this get scoped into the engine?
var workerCursor = null;

engine.once('dbready', function(){

  twitter.logger.init(config, twitter.controller.db);

  workerCursor = twitter.controller.collection.find(
    {$or: [{'state.query_followers': true},
    {'state.query_friends': true},
    {'state.expand_followers': {$gt: 0}},
    {'state.expand_friends': {$gt: 0}}]}).sort(
      {followers_count: 1}
    );


    //query_mongo_user(seed);
    engine.emit('get_next_user');

  });

  var queryDBList = [];

  engine.on('get_next_user', function(){
    get_next_user();
  });

  function get_next_user()
  {
    logger.debug("Fetching next user");

    //next reconnects if necessary, wonder if it should auto disconnect
    workerCursor.next(function(err, results){
      if (! results){
        logger.error( "no more users to query, must be close to done");
      }
      engine.emit("query_mongo_user", results);
    });
  }

  engine.on('query_mongo_user', function(data)
  {
    logger.trace('engine.query_mongo_user received %j', data);
    // twitter.controller.queryUser(user, callback);
    query_mongo_user(data);
  });


  var seedTemplate = {
    internal: {
      query_followers: 1,
      query_friends: 1,
      expand_followers: 1,
      expand_friends: 1,
    }
  };

  function query_mongo_user (data)
  {

    logger.info('db lookup for user %s', data.id_str);
    twitter.controller.queryUser(data, function(err, results)
    // query user from database
    {
      logger.trace('%j', err);
      logger.trace('%j', results);

      docs[results.id_str] = results;

      if( ! results)
      {
        logger.info('User not found in database, %s', data.id_str);
        query_twitter_user(results);
        return;
      }

      logger.info('queried user %s from database', results.id_str);
      logger.debug('query date: %s, compare date %s, results',
      new Date(results.internal.user_queried),
      new Date(Date.now() - 24*60*60*1000),
      new Date(results.internal.user_queried) < (new Date(Date.now() - 24*60*60*1000)));

      if (new Date(results.internal.user_queried) < (new Date(Date.now() - 24*60*60*1000)))
      {
        // too old, requery
        query_twitter_user(results);
        return;
      }

      runQueries(results);

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

  function query_twitter_user(data){
    logger.info('query_twitter_user received %s', data.id_str);

    var id_str = data.id_str;
    var expand_followers = data.internal.expand_followers;
    var expand_friends = data.internal.expand_friends;

    //query user from twitter
    twitter.api.queryUser(data, function(err, results)
    {

      //if there's an error, get out now
      // TODO add test for valid data
      if (err)
      {
        logger.error("twitter api error querying %s: %s", id_str, err);
        engine.emit('get_next_user');
        return;
      }

      scrapeUser(data, data, 0);

      runQueries(data);

    });

  }

  // TODO exchange limit on being able to query for a limit on the number queried

  //fans out into 4 functions
  //twitter.api.queryFriends
  //twitter.api.queryFollowers
  //twitter.api.expandFriends
  //twitter.api.expandFollowers

  function runQueries(user){

    if (user.state.query_friends && user.friends_count > 0)
    {
      logger.trace('will query_friends: %s', user.id_str);
      queryFriendsSem.take(function()
      {
        twitter.api.queryFriends(user, callback_query_twitter_friends_ids);
      });
    }
    else
    {
      logger.trace('will not query_friends: %s', user.id_str);
      user.state.query_friends = 0;
    }

    if ( user.state.expand_friends  && user.friends_count < 20000 && user.friends_count > 0)
    {
      logger.trace('will expand_friends: %s', user.id_str);
      queryFriendsSem.take(function(){
        twitter.api.queryFriendsUsers(user, callback_query_twitter_friends);
      });
    }
    else
    {
      logger.trace('will not expand_friends: %s', user.id_str);
      user.state.expand_friends = 0;
    }


    if ( user.state.query_followers  && user.followers_count > 0)
    {
      logger.trace('will query_followers: %s', user.id_str);
      queryFollowersSem.take( function()
      {
        twitter.api.queryFollowersUsers(user, callback_query_twitter_followers_ids);
      });
    }
    else
    {
      logger.trace('will not query_followers: %s', user.id_str);
      user.state.query_followers = 0;
    }

    if (user.state.expand_followers && user.followers_count < 20000  && user.followers_count > 0)
    {
      logger.trace('will expand_followers: %s', user.id_str);
      queryFollowersSem.take( function()
      {
        twitter.api.queryFollowersUsers(user, callback_query_twitter_followers);
      });
    }
    else
    {
      logger.trace('will not expand_followers: %s', user.id_str);
      user.state.expand_followers = 0;
    }

  }

  // scrapeUser(user, partent);
  //
  // If a user exists, update info and query parameters
  // If one does not exist, create it
  // purge may be null=0=false, or (true)
  // needs the full object so that it can update the object.

  function scrapeUser(user, parent, purge)
  {
    if (! purge) {
      purge = 0;
    }
    // TODO overwrite internal if values greater than current

    twitter.controller.queryUserExists(user, function(err, results){

      //strip extra fields
      delete user.status;

      if (results && typeof(results.friends) == 'array' && typeof(results.followers) == 'array'){
        user.friends = results.friends;
        user.followers = results.followers;
      }
      else {
        user.friends = [];
        user.followers = [];
      }
      if (user.id_str == parent.id_str) {
        user.internal = {
          user_queried: new Date(),
          query_followers: (parent.internal.query_followers)? 1 : 0,
          query_friends: (parent.internal.query_friends )? 1 : 0,
          expand_followers: parent.internal.expand_followers,
          expand_friends: parent.internal.expand_friends ,
        };

        // TODO add estimated number of users remainingto query
        user.state = {
          query_followers: (parent.internal.expand_followers ) ? 1 : 0,
          query_friends: (parent.internal.expand_friends ) ? 1 : 0,
          expand_followers: parent.internal.expand_followers ,
          expand_friends: parent.internal.expand_friends ,
        };

      }
      else {
        //add internal fields
        user.internal = {
          user_queried: new Date(),
          query_user: 0,
          query_followers: (parent.internal.expand_followers - 1 > 0)? 1 : 0,
          query_friends: (parent.internal.expand_friends - 1 > 0)? 1 : 0,
          expand_followers: parent.internal.expand_followers - 1,
          expand_friends: parent.internal.expand_friends - 1,
        };

        user.state = {
          query_followers: (parent.internal.expand_followers - 1 > 0)? 1 : 0,
          query_friends: (parent.internal.expand_friends - 1 > 0)? 1 : 0,
          expand_followers: parent.internal.expand_followers - 1,
          expand_friends: parent.internal.expand_friends - 1,
        };
      }

      //overwrite in-memory copy with current copy
      docs[user.id_str] = user;

      //save results to database
      engine.emit('accumulate_user_changes', {id_str: user.id_str, purge: purge});
    });

  }

  function callback_query_twitter_friends(err, results, finished, data, next_cursor_str)
  {
    logger.trace('last: %s', finished);
    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", data.id_str, err);

      // TODO retry api call on twitter error
      docs[data.id_str].state.expand_friends = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      querySem.leave();
      return;
    }

    results.forEach(function(user) {
      scrapeUser(user, data, 1);
    });

    // TODO: include counter
    logger.info("finished saving new friends of %s as users", data.id_str);

    if (finished)
    {
      docs[data.id_str].state.expand_friends = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      queryFriendsSem.leave();

    }
    else
    {
      twitter.api.queryFriendsUsers(data,next_cursor_str, callback_query_twitter_friends);
    }


  }

  function callback_query_twitter_friends_ids(err, results, finished, data, next_cursor_str)
  {
    logger.trace('last: %s', finished);

    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s,\t%s", data.id_str, err);
      docs[data.id_str].state.query_friends = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      querySem.leave();
      return;
    }

    //append set of friends to data's friends array
    docs[data.id_str].friends = docs[data.id_str].friends.concat(results );
    logger.debug(' %s\tnew: %d\taccumulated: %d\t/ %d',
    data.id_str,
    results.length,
    docs[data.id_str].friends.length,
    docs[data.id_str].friends_count);

    //when finished
    if (finished)
    {
      docs[data.id_str].friends = util.uniqArray(docs[data.id_str].friends);
      docs[data.id_str].state.query_friends = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      queryFriendsSem.leave();
    }
    else
    {
      twitter.api.queryFriends(data,next_cursor_str, callback_query_twitter_friends_ids);
    }

  }

  function callback_query_twitter_followers(err, results, finished, data, next_cursor_str)
  {
    logger.trace('last: %s', finished);

    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", data.id_str, err);

      // TODO retry api call on twitter error
      docs[data.id_str].state.expand_followers = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      querySem.leave();
      return;
    }

    results.forEach(function(user){
      scrapeUser(user, data, 1);

    });

    logger.info("queried followers of %s as users", data.id_str);

    if (finished)
    {
      docs[data.id_str].state.expand_followers = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      queryFollowersSem.leave();
    }
    else
    {
      twitter.api.queryFollowersUsers(data,next_cursor_str, callback_query_twitter_followers);
    }

  }


  function callback_query_twitter_followers_ids(err, results, finished, data, next_cursor_str)
  {
    logger.trace('last: %s', finished);
    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s, %s", data.id_str, err);
      docs[data.id_str].state.query_followers = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      querySem.leave();
      return;
    }

    //append set of followers to data's followers array
    docs[data.id_str].followers = docs[data.id_str].followers.concat(results);

    logger.info('query_twitter_followers_ids %s, new: %d, accumulated: %d/%d',
    data.id_str,
    results.length,
    docs[data.id_str].followers.length,
    docs[data.id_str].followers_count);

    if (finished)
    {
      docs[data.id_str].followers = util.uniqArray(docs[data.id_str].followers);
      docs[data.id_str].state.query_followers = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      queryFollowersSem.leave();
    }
    else
    {
      twitter.api.queryFollowers(data,next_cursor_str, callback_query_twitter_followers);
    }

  }

  engine.on('check_remove_doc', function(data){
    logger.trace('check for purge %s, %j', data.id_str, docs[data.id_str].state);

    if (  data.purge )
    {
      logger.trace("purging %s", data.id_str);
      delete docs[data.id_str];
    }
    else if( ! ( docs[data.id_str].state.expand_followers ||
      docs[data.id_str].state.expand_friends ||
      docs[data.id_str].state.query_follwers ||
      docs[data.id_str].state.query_friends ))
      {
        logger.info("purging %s", data.id_str);
        delete docs[data.id_str];
        engine.emit('get_next_user');
      }
      else
      {
        logger.debug("not purging %s", data.id_str);
      }
    });

    engine.on('accumulate_user_changes', function(data){
      var id_str = data.id_str;
      logger.debug('accumulate_user_changes %s', data.id_str );
      //save user object
      twitter.controller.updateUser(docs[data.id_str], function(err){

        // TODO add test for valid data
        if (err)
        {
          logger.error("error updating user in db %s, %s", id_str, err);
          //
        }
        else {
          logger.trace('successfully saved user %s', data.id_str);
        }
        logger.trace('%j', docs[data.id_str]);

        engine.emit('check_remove_doc', data);

      });

    });
