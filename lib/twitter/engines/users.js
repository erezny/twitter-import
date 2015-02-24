
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
var usersWorker = null;
var workerCursor = null;
// TODO styleguide:
// Friends before followers, query before expand.

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

module.exports.init = function (config_, parent_)
{
  usersWorker = this;
  config = config_;
  parent = parent_;
  logger = config.logger;
  twitter = parent;
  engine = parent.engine;
  workerCursor = null;
  queryFriendsCursor = null;
  queryFollowersCursor = null;
  expandFriendsCursor = null;
  expandFollowersCursor = null;
  queryFriends= {
    currentObject: null,
  };
  queryFollowersCursor= {
    currentObject: null,
  };
  expandFriendsCursor= {
    currentObject: null,
  };
  expandFollowersCursor= {
    currentObject: null,
  };
  logger.debug('users engine init');

  engine.once('dbready', function(){
      //query_mongo_user(seed);
      engine.emit('get_next_queryFriends');
      setTimeout(function (){
      engine.emit('get_next_queryFollowers');
      }, 15*1000);
      setTimeout(function (){
      engine.emit('get_next_expandFriends');
    }, 30*1000);
      setTimeout(function (){
      engine.emit('get_next_expandFollowers');
    }, 45*1000);

  });

  engine.on('get_next_queryFriends', function()
  {
    usersWorker.get_next_queryFriends();
  });

  engine.on('get_next_queryFollowers', function()
  {
    usersWorker.get_next_queryFollowers();
  });

  engine.on('get_next_expandFriends', function()
  {
    usersWorker.get_next_expandFriends();
  });

  engine.on('get_next_expandFollowers', function()
  {
    usersWorker.get_next_expandFollowers();
  });


  engine.on('queryFriends', function(user, cursor_str)
  {
    twitter.api.queryFriends(user, cursor_str, usersWorker.callback_queryFriends);
  });

  engine.on('queryFollowers', function(user, cursor_str)
  {
    twitter.api.queryFollowers(user, cursor_str, usersWorker.callback_queryFollowers);
  });

  engine.on('expandFriends', function(user, cursor_str)
  {
    twitter.api.expandFriends(user, cursor_str, usersWorker.callback_expandFriends);
  });

  engine.on('expandFollowers', function(user, cursor_str)
  {
    twitter.api.expandFollowers(user, cursor_str, usersWorker.callback_expandFollowers);
  });

};

  module.exports.get_next_queryFriends = function()
  {
    logger.debug("Fetching next user");

    twitter.controller.collection.findOneAndUpdate(
      {
        'state.query_friends': 1,
        'locks.query_friends': 0,
        'locks.query_user': 0,
        friends_count: {$gt : 0}
      },
      {$set: {'locks.query_user': 1,
            'locks.query_friends': 1}},
      {sort: { 'state.expand_friends': -1, friends_count: 1},
      limit: 1},
      function(err, result)
      {
        logger.trace("%j", result);
        if (err)
        {
          logger.error('error querying next queryFriends: %j', err);
        }
        else if (! result.value)
        {
          logger.error('no result');
          setTimeout(function (){
            engine.emit('get_next_queryFriends');
          }, 60*1000);
        }
        else {
          logger.trace('queryFriends: %s', result.value.screen_name);
          engine.emit('queryFriends', result.value);
        }
      }
    );
  };
  module.exports.get_next_queryFollowers = function()
  {
    logger.debug("Fetching next user");

    queryFriendsCursor = twitter.controller.collection.findOneAndUpdate(
      {
        'state.query_followers': 1,
        'locks.query_followers': 0,
        'locks.query_user': 0,
        followers_count: {$gt : 0}
      },
      {$set: {'locks.query_user': 1,
            'locks.query_followers': 1}},
      {sort: { 'state.expand_followers': -1, followers_count: 1},
      limit: 1},
      function(err, result)
      {
        logger.trace("%j", result);
        if (err)
        {
          logger.error('error querying next queryFollowers: %j', err);
        }
        else if (! result.value)
        {
          logger.error('no result');
          setTimeout(function (){
          engine.emit('get_next_queryFollowers');
        }, 60*1000);
        }
        else
        {
          logger.trace('queryFollowers: %s', result.value.screen_name);
          engine.emit('queryFollowers', result.value);
        }
      }
    );
  };

  module.exports.get_next_expandFriends = function()
  {
    logger.debug("Fetching next user");

    expandFollowersCursor = twitter.controller.collection.findOneAndUpdate(
      {
        'state.expand_friends': {$gt: 0},
        'locks.expand_friends': 0,
        'locks.query_user': 0,
        friends_count: {$gt : 0}
      },
        {$set: {'locks.query_user': 1,
              'locks.expand_friends': 1}},
        {sort: {'state.query_friends': 1, friends_count: 1},
        limit: 1},
        function(err, result)
        {
          logger.trace("%j", result);
          if (err)
          {
            logger.error('error querying next expandFriends: %j', err);
          }
          else if (! result.value)
          {
            logger.error('no result');
            setTimeout(function (){
            engine.emit('get_next_expandFriends');
          }, 60*1000);
          }
          else {
            logger.trace('expandFriends: %s', result.value.screen_name);
            engine.emit('expandFriends', result.value);
          }
        }
    );
  };

  module.exports.get_next_expandFollowers = function()
  {
    logger.debug("Fetching next user");

    expandFriendsCursor = twitter.controller.collection.findOneAndUpdate(
      {
        'state.expand_followers': {$gt: 0},
        'locks.expand_followers': 0,
        'locks.query_user': 0,
        followers_count: {$gt : 0}
      },
      {$set: {'locks.query_user': 1,
            'locks.expand_followers': 1}},
      {sort: {'state.query_followers': 1, followers_count: 1},
      limit: 1},
      function(err, result)
      {
        logger.trace("%j", result);
        if (err)
        {
          logger.error('error querying next expandFollowers: %j', err);
        }
        else if (! result.value)
        {
          logger.error('no result');

          setTimeout(function (){
          engine.emit('get_next_expandFollowers');
        }, 60*1000);
        }
        else {
          logger.trace('expandFollowers: %s', result.value.screen_name);
          engine.emit('expandFollowers', result.value);
        }
      }
    );

  };

  // query_twitter_user
  // parameters:
  // data: {
  //  id_str: id to query
  //  internal.expandFollowers: how deep to continue searching for followers.
  //                    0 don't even do this one
  //                    1 look up basic info on followers
  //                    2 look up follower's followers
  //  internal.expandFollowing: see above
  //  }

  module.exports.query_twitter_user = function(data){
    logger.info('query_twitter_user received %s', data.id_str);

    var id_str = data.id_str;
    var expandFollowers = data.internal.expandFollowers;
    var expandFriends = data.internal.expandFriends;

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

      usersWorker.scrapeUser(results);

    });

  };

  // scrapeUser(user, partent);
  //
  // If a user exists, update info and query parameters
  // If one does not exist, create it
  // purge may be null=0=false, or (true)
  // needs the full object so that it can update the object.

module.exports.scrapeUser = function(user, parent)
  {
    // TODO overwrite internal if values greater than current
    logger.trace('scrape: %j', user);

    twitter.controller.lockUser(user, function(err, result){
      // TODO add test for valid data
      if (err)
      {
        logger.error("error querying user from db %s, %s", user.screen_name, err);
        //
      }
      result = result.value;
      //strip extra fields
      delete user.status;
    //  logger.info('%j', result);
    // TODO Result && Parent, Result && ! parent
      if (result)
      {
        if (!result.locks.query_user){
          //put back, wait til next time
          // TODO timout until user is not locked.
          twitter.controller.unlockUser(result, function(err, result){
            //nothing
            return;
          });
          return;
        }
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
      else if (parent)
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
      else
      {
        user.friends = [];
        user.followers = [];
        user.internal = {
          user_queried: new Date(),
          query_user: 0,
          query_followers: 1,
          query_friends: 1,
          expand_followers: 0,
          expand_friends: 0,
        };

        user.state = {
          query_followers: 0,
          query_friends: 0,
          expand_followers: 0,
          expand_friends: 0,
        };
      }

      user.locks = {
                query_user: 0,
                query_followers: 0,
                query_friends: 0,
                expand_followers: 0,
                expand_friends: 0,
              };

      twitter.controller.saveUnlockUser(user, function(err, result){
        if (err){
          logger.error('error saving user: %j', err);
          return;
        }
      });
    });

  };



  module.exports.callback_queryFriends = function(err, results, finished, parent, next_cursor_str)
  {
    logger.trace('last: %s', finished);

    //if there's an error, get out now
    // TODO add test for valid parent
    if (err)
    {
      logger.error("twitter api error querying %s,\t%s", parent.screen_name, err);
      parent.locks.query_friends = 0;
      twitter.controller.saveUnlockUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });
      return;
    }

    //append set of friends to parent's friends array
    parent.friends = parent.friends.concat(results );
    parent.friends = util.uniqArray(parent.friends);

    logger.info('%s\tnew: %d\taccumulated: %d\t/ %d',
      parent.screen_name,
      results.length,
      parent.friends.length,
      parent.friends_count
    );

    //when finished
    if (finished)
    {
      twitter.controller.countExistingUsers(parent.friends, function(count, query){
        logger.info("%s\t%d\tof\t%d\tfriends loaded",parent.screen_name,count, parent.friends.length);
      });

      parent.state.query_friends = 0;
            parent.locks.query_friends = 0;
      twitter.controller.saveUnlockUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });

      engine.emit('get_next_queryFriends');
  }
    else
    {
      twitter.controller.saveUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });
      engine.emit('queryFriends',parent,next_cursor_str);
    }

  };

module.exports.callback_queryFollowers = function(err, results, finished, parent, next_cursor_str)
{
  logger.trace('last: %s', finished);
  //if there's an error, get out now
  // TODO add test for valid parent
  if (err)
  {
    logger.error("twitter api error querying %s, %s", parent.screen_name, err);
    parent.locks.query_followers = 0;
    twitter.controller.saveUnlock(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
  }

  //append set of followers to parent's followers array
  parent.followers = parent.followers.concat(results);
  parent.followers = util.uniqArray(parent.followers);

  logger.info('%s\tnew: %d,\taccumulated: %d\t/ %d',
    parent.screen_name,
    results.length,
    parent.followers.length,
    parent.followers_count
  );


  if (finished)
  {
    twitter.controller.countExistingUsers(parent.followers, function(count, query){
      logger.info("%s\t%d\tof\t%d\tfollowers loaded",parent.screen_name, count, parent.followers.length);
      // TODO move this outside to avoid latence. Kept inside for race condition.
      engine.emit('accumulate_user_changes', {id_str: parent.id_str});
    });

    parent.state.query_followers = 0;
        parent.locks.query_followers = 0;
    twitter.controller.saveUnlockUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
    engine.emit('get_next_queryFollowers');
  }
  else
  {
    twitter.controller.saveUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
    engine.emit('queryFollowers', parent, next_cursor_str);
  }

};

module.exports.callback_expandFriends = function(err, results, finished, parent, next_cursor_str)
{
  logger.trace('last: %s', finished);
  //if there's an error, get out now
  // TODO add test for valid parent
  if (err)
  {
    logger.error("twitter api error querying %s, %s", parent.screen_name, err);

    // TODO retry api call on twitter error
    parent.locks.expand_friends = 0;
    twitter.controller.saveUnlock(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
    return;
  }

  results.forEach(function(user) {
    usersWorker.scrapeUser(user, parent);
  });

  logger.info('%s\tnew: %d',
    parent.screen_name,
    results.length
  );
  setTimeout(function(){
  twitter.controller.countExistingUsers(parent.friends, function(count, query){
    logger.info("%s\t%d\tof\t%d\tfriends loaded",parent.screen_name, count, parent.friends.length);
  });}, 5*1000);

  if (finished)
  {
    parent.state.expand_friends = 0;
      parent.locks.expand_friends = 0;
    twitter.controller.saveUnlockUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
    engine.emit('get_next_expandFriends');
  }
  else
  {
    engine.emit('expandFriends', parent, next_cursor_str);
  }


};

  module.exports.callback_expandFollowers = function(err, results, finished, parent, next_cursor_str)
  {
    logger.trace('last: %s', finished);

    //if there's an error, get out now
    // TODO add test for valid parent
    if (err)
    {
      logger.error("twitter api error querying %s, %s", parent.screen_name, err);

      // TODO retry api call on twitter error
      parent.locks.expand_followers = 0;
      twitter.controller.saveUnlock(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });
      return;
    }

    results.forEach(function(user){
      usersWorker.scrapeUser(user, parent, 1);
    });

    setTimeout(function(){
      twitter.controller.countExistingUsers(parent.followers, function(count, query){
        logger.info("%s\t%d\tof\t%d\tfollowers loaded",parent.screen_name, count, parent.followers.length);
      });
    }, 5*1000);

    if (finished)
    {
      parent.state.expand_followers = 0;
        parent.locks.expand_followers = 0;
      twitter.controller.saveUnlockUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });

      engine.emit('get_next_expandFollowers');
    }
    else
    {
      engine.emit('expandFollowers', parent, next_cursor_str);
    }

};
