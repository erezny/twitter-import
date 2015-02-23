
// #refactor:10 pull in dependencies

var assert = require('assert');

// TODO can the init be removed by using a constructor?
var config = null;

var parent = null;
var twitter = null;
var util = require('../../util.js');

var engine = null;

var queryFriendsSem = require('semaphore')(1);
var queryFollowersSem = require('semaphore')(1);

// TODO change logger type based on config file
var logger = null;

var docs = {};

// TODO styleguide:
// Friends before followers, query before expand.

// TODO: take seed from command line or API
var seed = {
    "id": 195085926,
    "id_str": "195085926",
    "name": "podcasts",
    "uri": "/erezny/lists/podcasts",
    "subscriber_count": 0,
    "member_count": 3,
    "mode": "public",
    "description": "",
    "slug": "podcasts",
    "full_name": "@erezny/podcasts",
    "created_at": "Thu Feb 12 05:51:08 +0000 2015",
    "following": true,
    "user": {
      "id": 16876313,
      "id_str": "16876313",
      "name": "Elliott Rezny",
      "screen_name": "erezny",
    }
  }
docs[seed.id_str] = seed;


module.exports.init = function (config_, parent_)
{
  config = config_;
  parent = parent_;
  logger = config.logger;
  twitter = parent;
  engine = parent.engine;
  logger.debug('users engine init');

  engine.on('get_next_user', function()
  {
    get_next_user();
  });

  engine.on('query_mongo_list', function(data)
  {
    logger.trace('engine.query_mongo_list received %j', data);
    // twitter.controller.queryUser(user, callback);
    query_mongo_user(data);
  });


  engine.on('check_remove_list', function(data){
    logger.trace('check for purge %s, %j', data.full_name, docs[data.id_str].state);

    if (  data.purge )
    {
      logger.trace("purging %s", data.full_name);
      delete docs[data.id_str];
    }
    else if( ! ( docs[data.id_str].state.expand_followers ||
      docs[data.id_str].state.expand_friends ||
      docs[data.id_str].state.query_follwers ||
      docs[data.id_str].state.query_friends ))
      {
        logger.info("purging %s", docs[data.id_str].full_name);
        delete docs[data.id_str];
        engine.emit('get_next_user');
      }
      else
      {
        logger.debug("not purging %s", docs[data.id_str].full_name);
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
          logger.error("error updating user in db %s, %s", docs[id_str].full_name, err);
          //
        }
        else {
          logger.trace('successfully saved user %s', docs[id_str].full_name);
        }
        logger.trace('%j', docs[id_str]);

        engine.emit('check_remove_list', data);

      });

    });
};


// #refactor:10 can this get scoped into the engine?
var workerCursor = null;



  var queryDBList = [];


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



  var seedTemplate = {
    internal: {
      query_followers: 1,
      query_friends: 1,
      expand_followers: 2,
      expand_friends: 2,
    }
  };

//adapt for list
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



  // TODO exchange limit on being able to query for a limit on the number queried
// TODO query : list / membership
  //fans out into 4 functions
  //twitter.api.queryFriends
  //twitter.api.queryFollowers
  //twitter.api.expandFriends
  //twitter.api.expandFollowers

  function runQueries(user){

/*  //// In lieu of this, I'm going to force the database to query everyone.
    //todo add some fuzzyness to the friends count
    if (user.internal.query_friends > 1 && user.friends_count > user.friends.length)
    {
      user.state.query_friends = 1;
    }
    if (user.internal.query_followers > 1 && user.followers_count > user.followers.length)
    {
      user.state.query_followers = 1;
    }
*/

    if (user.state.query_friends && user.friends_count > 0 )
    {
      logger.trace('will query_friends: %s', user.full_name);
      queryFriendsSem.take(function()
      {
        twitter.api.queryFriends(user, callback_query_twitter_friends_ids);
      });
    }
    else
    {
      logger.trace('will not query_friends: %s', user.full_name);
      user.state.query_friends = 0;
    }

    if ( user.state.expand_friends  && user.friends_count < 20000 && user.friends_count > 0)
    {
      logger.trace('will expand_friends: %s', user.full_name);

      twitter.controller.countExistingUsers(user.friends, function(count, query){
        logger.info("%d\tof\t%d\tfriends loaded",count, user.friends_count);

        if (count <= (user.friends_count * 0.90)){
          queryFriendsSem.take(function(){
            twitter.api.queryFriendsUsers(user, callback_query_twitter_friends);
          });
        }
        else {
          docs[user.id_str].state.expand_friends = 0;
          engine.emit('accumulate_user_changes', {id_str: user.id_str});
        }
      });
    }
    else
    {
      logger.trace('will not expand_friends: %s', user.full_name);
      user.state.expand_friends = 0;
    }


    if ( user.state.query_followers  && user.followers_count > 0)
    {
      logger.trace('will query_followers: %s', user.full_name);
      queryFollowersSem.take( function()
      {
        twitter.api.queryFollowers(user, callback_query_twitter_followers_ids);
      });
    }
    else
    {
      logger.trace('will not query_followers: %s', user.full_name);
      user.state.query_followers = 0;
    }

    if (user.state.expand_followers && user.followers_count < 20000  && user.followers_count > 0)
    {
      logger.trace('will expand_followers: %s', user.full_name);
      twitter.controller.countExistingUsers(user.followers, function(count, query){
        logger.info("%d\tof\t%d\tfollowers loaded",count, user.followers_count);

        if (count <= (user.followers_count * 0.90)){
          queryFollowersSem.take(function(){
            twitter.api.queryFollowersUsers(user, callback_query_twitter_followers);
          });
        }
        else {
          docs[user.id_str].state.expand_followers = 0;
          engine.emit('accumulate_user_changes', {id_str: user.id_str});
        }
      });
    }
    else
    {
      logger.trace('will not expand_followers: %s', user.full_name);
      user.state.expand_followers = 0;
    }

  }


// TODO add list indicator to logger
  //keenClient = new Keen(config.env.keen);
setInterval(logSemStatus, 30*1000);

function logSemStatus(){
    logger.info("semaphore status: Friends: %d\tFollowers: %d",
    queryFriendsSem.current, queryFollowersSem.current);
}

  // scrapeUser(user, partent);
  //
  // If a user exists, update info and query parameters
  // If one does not exist, create it
  // purge may be null=0=false, or (true)
  // needs the full object so that it can update the object.

// TODO change to list scraper
  function scrapeUser(user, parent, purge)
  {
    if (! purge) {
      purge = 0;
    }
    // TODO overwrite internal if values greater than current
    logger.debug('scrape: %s', user.full_name);
    logger.trace('scrape: %j', user);

    twitter.controller.queryUser(user, function(err, result){
      // TODO add test for valid data
      if (err)
      {
        logger.error("error querying user from db %s, %s", user.full_name, err);
        //
      }
      //strip extra fields
      delete user.status;
    //  logger.info('%j', result);
      if (result)
      {
        user.friends = result.friends;
        user.followers = result.followers;

        user.internal = {
          user_queried: new Date(),
          query_user: 0,
          query_followers:
            (parent.internal.expand_followers ||
              result.internal.expand_folowers) ?
            1 : 0,
          query_friends:
            (parent.internal.expand_friends ||
              result.internal.expand_friends) ?
            1 : 0,
          expand_followers:
            ( parent.internal.expand_followers - 1 > result.internal.expand_followers)?
            parent.internal.expand_followers - 1 : result.internal.expand_followers,
          expand_friends:
            ( parent.internal.expand_friends - 1 > result.internal.expand_friends)?
            parent.internal.expand_friends - 1 : result.internal.expand_friends,
        };

        user.state = {
          query_followers: result.internal.query_followers,
          query_friends: result.internal.query_friends,
          expand_followers: result.internal.expand_followers,
          expand_friends: result.internal.expand_friends,
        };
      }
      else
      {
        user.friends = [];
        user.followers = [];
        user.internal = {
          user_queried: new Date(),
          query_user: 0,
          query_followers: (parent.internal.expand_followers > 0)? 1 : 0,
          query_friends: (parent.internal.expand_friends > 0)? 1 : 0,
          expand_followers: parent.internal.expand_followers - 1,
          expand_friends: parent.internal.expand_friends - 1,
        };

        user.state = {
          query_followers: (parent.internal.expand_followers > 0)? 1 : 0,
          query_friends: (parent.internal.expand_friends > 0)? 1 : 0,
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

// TODO duplicate into list/ownership and list/members
// TODO duplicate lsit/ownership to list/subscriptions
// TODO duplcate list/members to list/subscribers
  function callback_query_twitter_friends_ids(err, results, finished, data, next_cursor_str)
  {
    logger.trace('last: %s', finished);

    //if there's an error, get out now
    // TODO add test for valid data
    if (err)
    {
      logger.error("twitter api error querying %s,\t%s", data.full_name, err);
      docs[data.id_str].state.query_friends = 0;
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
      queryFriendsSem.leave();
      return;
    }

    //append set of friends to data's friends array
    docs[data.id_str].friends = docs[data.id_str].friends.concat(results );
    logger.debug(' %s\tnew: %d\taccumulated: %d\t/ %d',
    data.full_name,
    results.length,
    docs[data.id_str].friends.length,
    docs[data.id_str].friends_count);

    engine.emit('accumulate_user_changes', {id_str: data.id_str});

    //when finished
    if (finished)
    {
      docs[data.id_str].friends = util.uniqArray(docs[data.id_str].friends);

      twitter.controller.countExistingUsers(docs[data.id_str].friends, function(count, query){
        logger.info("%d\tof\t%d\tfriends loaded",count, docs[data.id_str].friends_count);
        engine.emit('accumulate_user_changes', {id_str: data.id_str});
      });

      docs[data.id_str].state.query_friends = 0;
      queryFriendsSem.leave();
    }
    else
    {
      twitter.api.queryFriends(data,next_cursor_str, callback_query_twitter_friends_ids);
      engine.emit('accumulate_user_changes', {id_str: data.id_str});
    }

  }
