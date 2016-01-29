
var assert = require('assert');

var MongoClient = require('mongodb').MongoClient;
var logger = require('tracer').colorConsole({
  level: 'trace',
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
              { 'state.expand_friends': 0 }
            ] },
          { $and: [
              { 'internal.expand_followers': { $gt: 0 } },
              { 'state.expand_followers': 0 }
            ] }
          ]
     }
   ).stream();
      // Execute find on all the documents
    stream.on('end', function() {
      logger.trace("end");
      db.close();
    });

    stream.on('data', function(data) {
      logger.trace('Querying mongo for %s', data.screen_name);
      stream.pause();
      var countJobs = [];
      if (data.friends.length > 0 && data.state.query_friends == 0){
        countJobs.push(countExistingUsers(data.friends));
      }
      if (data.followers.length > 0 && data.state.query_followers == 0){
        countJobs.push(countExistingUsers(data.followers));
      }

      if (countJobs.length > 1){
        RSVP.allSettled(countJobs).then(function(jobs) {
          logger.debug("friends %d / %d", jobs[0].value.exists, jobs[0].value.queried);
          logger.debug("followers %d / %d", jobs[1].value.exists, jobs[1].value.queried);

          var updateJobs = [];
          if (jobs[0].value.exists < jobs[0].value.queried * .9 && data.internal.expand_friends > 0) {
            logger.info("expanding friends on %s", data.screen_name);
            updateJobs.push(setExpandFriends(data));
          }
          if (jobs[1].value.exists < jobs[1].value.queried * .9 && data.internal.expand_followers > 0) {
            logger.info("expanding followers on %s", data.screen_name);
            updateJobs.push(setExpandFollowers(data));
          }
          if (updateJobs.length > 0){
            RSVP.allSettled(updateJobs).then(function(jobs) {
              stream.resume();
            } );
          } else {
            stream.resume();
          }
        });
      } else {
        stream.resume();
      }

    });
  });

function setExpandFriends(user, callback) {
  return new RSVP.Promise( function (resolve, reject) {
    var query = { "id_str": user.id_str };
    db.collection("twitterUsers").findOneAndUpdate( { 'id_str': user.id_str },
    { $set: { 'state.expand_friends': 1
    } }, function(err, result) {
      if (err){
        reject(err);
      }
      resolve(result);
    } );
   });
};

function setExpandFollowers(user) {
  return new RSVP.Promise( function (resolve, reject) {
    var query = { "id_str": user.id_str };
    db.collection("twitterUsers").findOneAndUpdate( { 'id_str': user.id_str },
    { $set: { 'state.expand_followers': 1
      } }, function(err, result) {
        if (err){
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
      logger.trace('queried %j %d', err, count);
      if (err) { reject("db error"); return;}
      resolve({ queried: id_str_list.length, exists: count } );
    });
  });
};
