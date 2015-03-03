
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
  followers: [],
  friends: [],
  internal: {
    query_user: 0,
    query_followers: 1,
    query_friends: 1,
    expand_followers: 3,
    expand_friends: 3,
  }
};

module.exports.init = function (config_, parent_)
{
  usersWorker = this;
  config = config_;
  parent = parent_;
  logger = config.logger;
  twitter = parent;
  engine = parent.engine;

  this.queryFriends= {
    currentObject: null,
    cursor: null,
    stop: 0,
    lastRan: null,
  };
  this.queryFollowers= {
    currentObject: null,
    cursor: null,
    stop: 0,
    lastRan: null,
  };
  this.expandFriends= {
    currentObject: null,
    cursor: null,
    stop: 0,
    lastRan: null,
  };
  this.expandFollowers= {
    currentObject: null,
    cursor: null,
    stop: 0,
    lastRan: null,
  };

  logger.debug('users engine init');

  engine.once('dbready', function(){
    // TODO remove once
      twitter.controller.collection.update(
        {$or: [{'locks.query_followers': 1},{'locks.query_friends': 1},{'locks.expand_followers': 1},{'locks.expand_friends': 1}, {'locks.query_user': 1}] },
        {$set: {'locks.query_followers': 0,'locks.query_friends': 0,'locks.expand_followers': 0,'locks.expand_friends': 0, 'locks.query_user': 0}},
        {multi: 1}
      );

      usersWorker.queryFriends.cursor =
        twitter.controller.collection.find(
          {
            'state.query_friends': 1,
            'locks.query_friends': 0,
            'locks.query_user': 0,
            friends_count: {$gt : 0}
          }
        ).limit(10);

      usersWorker.queryFollowers.cursor =
        twitter.controller.collection.find(
          {
            'state.query_followers': 1,
            'locks.query_followers': 0,
            'locks.query_user': 0,
            followers_count: {$gt : 0}
          }
        ).limit(10);

      usersWorker.expandFriends.cursor =
        twitter.controller.collection.find(
          {
            'state.expand_friends': {$gt: 0},
            'locks.expand_friends': 0,
            'locks.query_user': 0,
            friends_count: {$gt : 0}
          }
        ).limit(4)

      usersWorker.expandFollowers.cursor =
        twitter.controller.collection.find(
        {
          'state.expand_followers': {$gt: 0},
          'locks.expand_followers': 0,
          'locks.query_user': 0,
          followers_count: {$gt : 0}
        }
      ).limit(4);

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

  engine.on('seedUser', function(user, parent)
  {
    if (!parent)
    {
      parent = seed;
    }
    usersWorker.query_twitter_user(user, parent);
  });

};



  module.exports.get_next_queryFriends = function()
  {
    logger.debug("Fetching next user");

    usersWorker.queryFriends.cursor.next(
      function(err, result)
      {
        logger.trace("%j", result);
        if (err)
        {
          logger.error('error querying next queryFriends: %j', err);
        }

        if (! result || err)
        {
          logger.debug('resetting queryFriends cursor');
          usersWorker.queryFriends.cursor.rewind();
          setTimeout(function (){
            engine.emit('get_next_queryFriends');
          }, 2*1000);
        }
        else
        {
          result.locks.query_friends = 1;
          twitter.controller.lockUser(result, function(err, result) {
            if (result.value.locks.query_user > 1)
            {
              //user is already taken, get a new one.
              engine.emit('get_next_queryFriends');
            }
            else {
            logger.trace('queryFriends: %s', result.value.screen_name);
            engine.emit('queryFriends', result.value);
            }
          });
        }
    });
  };
  module.exports.get_next_queryFollowers = function()
  {
    logger.debug("Fetching next user");

    usersWorker.queryFollowers.cursor.next(
      function(err, result)
      {
        logger.trace("%j", result);
        if (err)
        {
          logger.error('error querying next queryFollowers: %j', err);
        }

        if (! result || err)
        {
          logger.debug('resetting queryFollowers cursor');
          usersWorker.queryFollowers.cursor.rewind();
          setTimeout(function (){
            engine.emit('get_next_queryFollowers');
          }, 2*1000);
        }
        else
        {
          result.locks.query_followers = 1;
          twitter.controller.lockUser(result, function(err, result) {
            if (result.value.state.query_user > 1)
            {
              //user is already taken, get a new one.
              engine.emit('get_next_queryFollowers');
            }
            else
            {
              logger.trace('queryFollowers: %s', result.value.screen_name);
              engine.emit('queryFollowers', result.value);
            }
          });
        }
      }
    );
  };

  module.exports.get_next_expandFriends = function()
  {
    logger.debug("Fetching next user");

    usersWorker.expandFriends.cursor.next(
        function(err, result)
        {
          logger.trace("%j", result);
          if (err)
          {
            logger.error('error querying next expandFriends: %j', err);
          }

          if (! result || err)
          {
            logger.debug('resetting expandFriends cursor');
            usersWorker.expandFriends.cursor.rewind();
            setTimeout(function (){
              engine.emit('get_next_expandFriends');
            }, 2*1000);
          }
          else
          {
            result.locks.expand_friends = 1;
            twitter.controller.lockUser(result, function(err, result) {
              if (result.value.state.query_user > 1)
              {
                //user is already taken, get a new one.
                engine.emit('get_next_expandFriends');
              }
              else
              {
                logger.trace('expandFriends: %s', result.value.screen_name);
                engine.emit('expandFriends', result.value);
              }
            });
          }
        }
    );
  };

  module.exports.get_next_expandFollowers = function()
  {
    logger.debug("Fetching next user");

    usersWorker.expandFollowers.cursor.next(
      function(err, result)
      {
        logger.trace("%j", result);
        if (err)
        {
          logger.error('error querying next expandFollowers: %j', err);
        }

        if (! result || err)
        {
          logger.debug('resetting expandFollowers cursor');
          usersWorker.expandFollowers.cursor.rewind();
          setTimeout(function (){
            engine.emit('get_next_expandFollowers');
          }, 2*1000);
        }
        else
        {
          result.locks.expand_followers = 1;
          twitter.controller.lockUser(result, function(err, result) {
            if (result.value.state.query_user > 1)
            {
              //user is already taken, get a new one.
              engine.emit('get_next_expandFriends');
            }
            else
            {
              logger.trace('expandFollowers: %s', result.value.screen_name);
              engine.emit('expandFollowers', result.value);
            }
          });
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

  module.exports.query_twitter_user = function(data, parent){
    logger.info('query_twitter_user received %s', data.screen_name);

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

      usersWorker.scrapeUser(results, parent);
    });

  };

function arrayInit(array){
  if (typeof(user.followers) !== "object"){
    return [];
  }
  else
    return array;
}

module.exports.cleanUser = function(user){
  user.followers = arrayInit(user.followers);
  user.friends = arrayInit(user.friends);
  user.lists = arrayInit(user.lists);
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
          query_followers:
            ( parent.internal.expand_followers - 1 > result.internal.expand_followers)?
            1 : 0,
          query_friends:
            ( parent.internal.expand_friends - 1 > result.internal.expand_friends)?
            1 : 0,
          expand_followers:
            ( parent.internal.expand_followers - 1 > result.internal.expand_followers)?
            1 : 0,
          expand_friends:
            ( parent.internal.expand_friends - 1 > result.internal.expand_friends)?
            1 : 0,
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
      parent.state.query_friends = 0;
      twitter.controller.saveUnlockUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });
      engine.emit('get_next_queryFriends');
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
    if (finished || usersWorker.queryFriends.stop )
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
        engine.emit('get_next_queryFriends');
      });

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
    parent.state.query_followers = 0;
    twitter.controller.saveUnlockUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
    engine.emit('get_next_queryFollowers');
    return;
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


  if (finished || usersWorker.queryFollowers.stop )
  {
    twitter.controller.countExistingUsers(parent.followers, function(count, query){
      logger.info("%s\t%d\tof\t%d\tfollowers loaded",parent.screen_name, count, parent.followers.length);
    });

    parent.state.query_followers = 0;
    parent.locks.query_followers = 0;

    twitter.controller.saveUnlockUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
      engine.emit('get_next_queryFollowers');
    });

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
      parent.state.expand_friends = 0;
    twitter.controller.saveUnlockUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
    });
    engine.emit('get_next_expandFriends');
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
      if (! finished && parent.friends_count * 0.95 < count)
      {
        usersWorker.expandFollowers.stop = 1;
        logger.info("%s\tstopping expandFriends after next query", parent.screen_name)
      }
  });}, 5*1000);

  if (finished || usersWorker.expandFriends.stop )
  {
    parent.state.expand_friends = 0;
      parent.locks.expand_friends = 0;
    twitter.controller.saveUnlockUser(parent, function(err, results){
      if (err){
        logger.error('error saving user: %j', err);
      }
      engine.emit('get_next_expandFriends');
    });
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
      parent.state.expand_followers = 0;
      twitter.controller.saveUnlockUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
      });
      engine.emit('get_next_expandFollowers');
      return;
    }

    results.forEach(function(user){
      usersWorker.scrapeUser(user, parent, 1);
    });

    setTimeout(function(){
      twitter.controller.countExistingUsers(parent.followers, function(count, query){
        logger.info("%s\t%d\tof\t%d\tfollowers loaded",parent.screen_name, count, parent.followers.length);
        if (! finished && parent.followers_count * 0.95 < count)
        {
          usersWorker.expandFollowers.stop = 1;
          logger.info("%s\tstopping expandFollowers after next query", parent.screen_name)
        }

    });
    }, 5*1000);

    if (finished || usersWorker.expandFollowers.stop )
    {
      parent.state.expand_followers = 0;
        parent.locks.expand_followers = 0;
      twitter.controller.saveUnlockUser(parent, function(err, results){
        if (err){
          logger.error('error saving user: %j', err);
        }
        usersWorker.expandFollowers.stop = 0;
        engine.emit('get_next_expandFollowers');
      });

    }
    else
    {
      engine.emit('expandFollowers', parent, next_cursor_str);
    }

};
