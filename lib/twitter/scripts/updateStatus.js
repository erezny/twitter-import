
var assert = require('assert');

var MongoClient = require('mongodb').MongoClient;
var logger = require('tracer').colorConsole({
  level: 'info',
  root: './',
});

var RSVP = require('rsvp');

var db;

//runtime loop
MongoClient.connect('mongodb://twitterImport:1EUyVa367T6wnJTzgykwi0s2@zapp:19024/socialGraph?authMechanism=SCRAM-SHA-1&authSource=socialGraph',
  {
    logger: logger,
    numberOfRetries: 10,
    retryMiliSeconds: 10000
  },
  function(err, db_) {
    if (err){
      process.exit;
    }

    db = db_;
    logger.trace('About to query mongo');

    db.collection("twitterUsers").update(
      { },
      {
        'state.query_friends': 0,
        'state.expand_friends': 0,
        'state.query_followers': 0,
        'state.expand_followers': 0,
      }, {
        multi: 1,
      }).then(function(err,results) {
        logger.debug("finished resetting states");
      }); // then

    var stream =  db.collection("twitterUsers").find(
      {
        $or: [
          { $and: [
            { 'friends_count': { $gt: 0 } },
            { $or: [
              { 'internal.query_friends': { $gt: 0 } },
              { 'internal.expand_friends': { $gt: 0 } },
            ] },
          ] },
          { $and: [
            { 'followers_count': { $gt: 0 } },
            { $or: [
              { 'internal.query_followers': { $gt: 0 } },
              { 'internal.expand_followers': { $gt: 0 } },
            ] },
          ] }
        ]
      })
      .stream();

    // Execute find on all the documents
    stream.on('end', function() {
      logger.info("end");
      //when openQueries == 0
    });

    stream.on('data', function(data) {

      logger.trace('Reviewing mongo for %s', data.screen_name);

      if ( !data.protected ) {
        processTwitterUser(stream, data)
      }

    });
});

var openQueries = 0;

function processTwitterUser(stream, user){
  var parallelChecks = 5;
  var countJobs = [];

  openQueries++;
  if ( openQueries >= parallelChecks ) {
    stream.pause();
  }
  countJobs.push(testQueryFriends(user));
  countJobs.push(testQueryFollowers(user));
  countJobs.push(testExpandFriends(user));
  countJobs.push(testExpandFollowers(user));

  RSVP.allSettled(countJobs).then(function(jobs) {
    openQueries--;
    logger.trace('next user. Open Queries: %d', openQueries);
    if (openQueries < parallelChecks ){
      stream.resume();
    }
  });
}

function testQueryFriends(data){
  if ( Array.isArray(data.friends) &&
        ( data.friends.length || 0 )< data.friends_count * .95 &&
        data.internal.query_friends > 0 && data.state.query_friends == 0 &&
        data.friends_count < 500000
      ) {
    var listed = data.friends.length || 0 ;
    logger.info('set query friends %d/%d \t%s', listed, data.friends_count, data.screen_name);
    return setQueryFriends(data);
  } else {
    logger.debug("not querying friends on %s", data.screen_name);
    return new RSVP.Promise( function (resolve, reject) { resolve() } );
  }
};

function testQueryFollowers(data){
  if ( Array.isArray(data.followers) &&
        ( data.followers.length || 0 )< data.followers_count * .95 &&
        data.internal.query_followers > 0 && data.state.query_followers == 0 &&
        data.followers_count < 500000
      ) {
    var listed = data.followers.length || 0 ;
    logger.info('set query followers %d/%d \t%s', listed, data.followers_count, data.screen_name);
    return setQueryFollowers(data);
  } else {
    logger.debug("not querying followers on %s", data.screen_name);
    return new RSVP.Promise( function (resolve, reject) { resolve() } );
  }
};

function testExpandFriends(data){
  return new RSVP.Promise( function (resolve, reject) {
    if ((data.friends.length || 0) > 0 && data.internal.expand_friends > 0 && data.friends.length < 10000){
      logger.trace("listing %d friends on %s", data.friends.length, data.screen_name);
      countExistingUsers(data.friends)
      .then(function(results) {
        if ( results < data.friends_count * 0.9 ) {
          logger.info("expanding %d/%d friends on %s",results, data.friends_count, data.screen_name);
          setExpandFriends(data, results).then(function() {
            resolve();
          });
        } else {
          logger.debug("not expanding %d friends of %s",results, data.screen_name);
          resolve();
        }
      }).catch(function(err) {
        logger.error("count existing friends of %s error %j", data.screen_name, err);
        reject();
      });
    } else {
      logger.debug("not listing %d friends on %s", data.friends.length, data.screen_name);
      resolve();
    }
  });
};

function testExpandFollowers(data){
  return new RSVP.Promise( function (resolve, reject) {
    if ((data.followers.length || 0) > 0 && data.internal.expand_followers > 0 && data.followers.length < 10000){
      logger.trace("listing %d followers on %s",data.follower_count,  data.screen_name);
      countExistingUsers(data.followers).then(function(results) {
        if (results < data.followers_count * 0.9) {
          logger.info("expanding %d/%d followers on %s",results, data.followers_count , data.screen_name);
          setExpandFollowers(data, results).then(function() {
            resolve();
          });
        } else {
          logger.debug("not expanding %d followers of %s", results , data.screen_name);
          resolve();
        }
      }).catch(function(err) {
        logger.error("count existing followers of %s error %j", data.screen_name, err);
        reject();
      });
    } else {
      logger.debug("not listing %d followers on %s",data.followers.length,  data.screen_name);
      resolve();
    }
  });
};

function setExpandFriends(user, queried) {
  return new RSVP.Promise( function (resolve, reject) {
    var query = { "id_str": user.id_str };
    db.collection("twitterUsers").findOneAndUpdate( query ,
      { $set: { 'state.expand_friends': 1, 'state.numFriendsFound': queried }
    }, function(err, result) {
      if (err){
        logger.error('queried %j', err);
        reject(err);
      }
      resolve(result);
    } );
  });
};

function setExpandFollowers(user, queried) {
  return new RSVP.Promise( function (resolve, reject) {
    var query = { "id_str": user.id_str };
    db.collection("twitterUsers").findOneAndUpdate( query,
      { $set: { 'state.expand_followers': 1, 'state.numFollowersFound': queried }
    }, function(err, result) {
      if (err){
        logger.error('queried %j', err);
        reject(err);
      }
      resolve(result);
    } );
  });
};

function setQueryFriends(user) {
  return new RSVP.Promise( function (resolve, reject) {
    var query = { "id_str": user.id_str };
    db.collection("twitterUsers").findOneAndUpdate( query ,
      { $set:
        { 'state.query_friends': 1,
        'state.numFriendsListed': listed,
        'state.numFriendsRemaining': user.friends_count - listed }
      }, function(err, result) {
        if (err){
          logger.error('queried %j', err);
          reject(err);
        }
        resolve(result);
      } );
    });
  };

  function setQueryFollowers(user) {
    return new RSVP.Promise( function (resolve, reject) {
      var query = { "id_str": user.id_str };
      db.collection("twitterUsers").findOneAndUpdate( query ,
        { $set:
          { 'state.query_followers': 1 ,
          'state.numFollowersListed': listed ,
          'state.numFollowersRemaining': user.followers_count - listed }
        }, function(err, result) {
          if (err){
            logger.error('queried %j', err);
            reject(err);
          }
          resolve(result);
        } );
      });
    };

    // twitter.controller.countDetailedFriends(user, callback);
    // count number of
    function countExistingUsers(id_str_list) {
      return new RSVP.Promise( function (resolve, reject) {
        db.collection("twitterUsers").count( { id_str: { $in: id_str_list } }, function(err, count) {
          if (err) {
            logger.error('db error counting users %j', err);
            reject("db error");
          } else {
            resolve( count );
          }
        });
      });
    };
