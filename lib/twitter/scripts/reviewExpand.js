
var assert = require('assert');

var MongoClient = require('mongodb').MongoClient;
var logger = require('tracer').colorConsole({
  level: 'debug',
  root: './',
});

var RSVP = require('rsvp');

var db;

//runtime loop
MongoClient.connect('mongodb://twitterImport:1EUyVa367T6wnJTzgykwi0s2@zapp:19024/socialGraph?authMechanism=SCRAM-SHA-1&authSource=socialGraph', function(err, db_) {
  db = db_;

    logger.trace('About to query mongo');
    var stream =  db.collection("twitterUsers").find(
       {
          $or: [
          { $and: [
              { 'internal.expand_friends': { $gt: 0 } },
              { 'state.expand_friends': 0 },
              { 'friends_count': { $gt: 0 } }
            ] },
          { $and: [
              { 'internal.expand_followers': { $gt: 0 } },
              { 'state.expand_followers': 0 },
              { 'followers_count': { $gt: 0 } }
            ] }
          ]
     })
     .sort( { 'friends_count': 1 } )
     .stream();
      // Execute find on all the documents
    stream.on('end', function() {
      logger.trace("end");
      db.close();
    });

    function testQueryFriends(data){
      if ((data.friends.length || 0 )< data.friends_count * .95&&  data.internal.query_friends > 0 && data.state.query_friends == 0){
        logger.info("querying friends on %s", data.screen_name);
        return setQueryFriends(data);
      } else {
        logger.debug("not querying friends on %s", data.screen_name);
        return new RSVP.Promise( function (resolve, reject) { resolve() } );
      }
    };

    function testQueryFollowers(data){
      if ((data.followers.length || 0)< data.followers_count * .95&&  data.internal.query_followers > 0 && data.state.query_followers == 0){
        logger.info("querying followers on %s", data.screen_name);
        return setQueryFollowers(data);
      } else {
        logger.debug("not querying followers on %s", data.screen_name);
        return new RSVP.Promise( function (resolve, reject) { resolve() } );
      }
    };

      function testExpandFriends(data){
      return new RSVP.Promise( function (resolve, reject) {
        if ((data.friends.length || 0) > 0 && data.internal.expand_friends > 0 && data.friends.length < 10000){
          logger.info("listing %d friends on %s", data.friends.length, data.screen_name);
            countExistingUsers(data.friends)
            .then(function(results) {
              if (results.value.exists < results.value.queried * .9) {
                logger.info("expanding %d friends on %s",results.value.exists , data.screen_name);
                setExpandFriends(data, results.value.queried).then(function() {
                  resolve();
                });
              } else {
                logger.debug("not expanding %d friends on %s",results.value.exists , data.screen_name);
                resolve();
              }
            }).catch(function(err) {
              logger.error("count existing friends error %j", err);
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
          logger.info("listing %d followers on %s",data.followers.length,  data.screen_name);
          countExistingUsers(data.followers).then(function(results) {
            if (results.value.exists < results.value.queried * .9) {
              logger.info("expanding %d followers on %s",results.value.exists , data.screen_name);
              setExpandFollowers(data, results.value.queried).then(function() {
                resolve();
              });
            } else {
              logger.debug("not expanding %d followers on %s",results.value.exists , data.screen_name);
              resolve();
            }
          }).catch(function(err) {
            logger.error("count existing followers error %j", err);
            reject();
          });
        } else {
          logger.debug("not listing %d followers on %s",data.followers.length,  data.screen_name);
          resolve();
        }
      });
      };

      var openQueries = 0;

    stream.on('data', function(data) {
      logger.info('Querying mongo for %s', data.screen_name);

      if (openQueries++ > 5){
        stream.pause();
      }
      var countJobs = [];

      countJobs.push(testQueryFriends(data));
      countJobs.push(testQueryFollowers(data));
      countJobs.push(testExpandFriends(data));
      countJobs.push(testExpandFollowers(data));

      RSVP.allSettled(countJobs).then(function(jobs) {
          openQueries--;
          logger.debug('next user. Open Queries: %d', openQueries);
          stream.resume();
      });

    });
  });

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
    var listed = user.friends.length || 0 ;
    logger.info('set query friends %d found %d', user.friends_count, listed);
    var query = { "id_str": user.id_str };
    db.collection("twitterUsers").findOneAndUpdate( query ,
    { $set:
      {'state.query_friends': 1 ,
      'state.numFriendsListed': listed,
      'state.numFriendsRemaining': user.friends_count - listed}
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
    var listed = user.followers.length || 0 ;
    logger.info('set query followers %d found %d', user.followers_count, listed);
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
        logger.error('queried %j', err);
         reject("db error");
         return;
       }

      resolve({ queried: id_str_list.length, exists: count } );
    });
  });
};
