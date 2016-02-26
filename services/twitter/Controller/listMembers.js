
// #refactor:10 write queries
var util = require('util');
var Twit = require('twit');
var T = new Twit({
  consumer_key:         process.env.TWITTER_CONSUMER_KEY,
  consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
  //access_token:         process.env.TWITTER_ACCESS_TOKEN,
  //access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
  app_only_auth:        true
});

var MongoClient = require('mongodb').MongoClient,
assert = require('assert');

const metrics = require('../../../lib/crow.js').withPrefix("twitter.lists.controller.members");
var queue = require('../../../lib/kue.js');

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
var limiterMembers = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

setInterval( function() {
queue.inactiveCount( 'receiveListMembers', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
  metrics.setGauge("queue.inactive", total);
});
}, 15 * 1000 );

queue.process('receiveListMembers', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  metrics.counter("start").increment();

  redis.hgetall(util.format("twitter:%s", job.data.user.id_str), function(err, member) {
    if (member && member.neo4jID && member.neo4jID != "undefined"){

      redis.hgetall(util.format("twitterList:%s",job.data.list.id_str), function(err, list) {
        if (list && list.neo4jID && list.neo4jID != "undefined"){
          setListMember({ id: parseInt(member.neo4jID) }, { id: parseInt(list.neo4jID) }).then(done);
        } else {
          metrics.counter("members.rel_list_not_exist").increment();
          logger.error("neo4j list not in redis %j",err);
          done();
        }
      });
    } else {
      queue.create('queryUser', { user: job.data.user } ).attempts(5).removeOnComplete( true ).save();
      metrics.counter("members.rel_user_not_exist").increment();
      logger.debug("neo4j user not in redis %j",err);
      done();
    }
  });
});

function setListMember(user, list) {
  assert( typeof(user.id) == "number");
  assert( typeof(list.id) == "number");
  return new RSVP.Promise( function (resolve, reject) {
    neo4j.queryRaw("start x=node({idx}), n=node({idn}) MATCH (x)-[r:includes]->(n) RETURN x,r,n",
    { idx: list.id, idn: user.id }, function(err, results) {
      if (err){
        logger.error("neo4j find error %j",err);
        metrics.counter("members.rel_find_error").increment();
        reject("error");
        return;
      }
      logger.trace("neo4j found %j", results);
      if (results.data.length > 0) {
        //TODO search for duplicates and remove duplicates
        logger.debug("relationship found %j", results.data[0][1]);
        metrics.counter("members.rel_already_exists").increment();
        resolve(results.data[0][1]);
        if (results.data.length > 1){
          for (var i = 1; i < results.data.length; i++){
            neo4j.rel.delete(results.data[i][1].metadata.id, function(err) {
              if (!err) logger.debug("deleted duplicate relationship");
            });
          }
        }
        return;
      }
      neo4j.relate(list.id, 'includes', user.id, function(err, rel) {
        if (err){
          logger.error("neo4j save error %j %j", { user: user, list: list }, err);
          metrics.counter("members.rel_save_error").increment();
          reject("error");
          return;
        }
        logger.debug("saved relationship %j", rel);
        metrics.counter("members.rel_saved").increment();
        resolve(rel);
      });
    })
  });
}
