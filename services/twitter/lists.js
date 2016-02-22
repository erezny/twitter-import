
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

const crow = require("crow-metrics");
const request = require("request");
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.lists");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var RateLimiter = require('limiter').RateLimiter;
//set rate limiter slightly lower than twitter api limit
var limiter = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);
var limiterMembers = new RateLimiter(1, (1 / 14) * 15 * 60 * 1000);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'info'
} );
var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

process.once( 'SIGTERM', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
});

var neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });

  var db = null;
  MongoClient.connect(util.format('mongodb://%s:%s@%s:%d/%s?authMechanism=SCRAM-SHA-1&authSource=%s',
  process.env.MONGO_USER,
  process.env.MONGO_PASSWD,
  process.env.MONGO_HOST,
  process.env.MONGO_PORT,
  process.env.MONGO_DATABASE,
  process.env.MONGO_DATABASE
),
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

  queue.process('receiveUserListOwnership', function(job, done) {
    //  logger.info("received job");
    logger.trace("received job %j", job);
    saveListToMongo(job.data.list);
    upsertListToNeo4j(job.data.list)
    .then(function(savedList) {
      //this is receiving mongo's output
      logger.trace("savedList: %j", savedList);
      return new RSVP.Promise( function (resolve, reject) {
        redis.hgetall(util.format("twitter:%s",job.data.list.owner), function(err, obj) {
          if (obj && obj.neo4jID && obj.neo4jID != "undefined"){
            setListOwnership({ id: parseInt(obj.neo4jID) }, savedList).then(resolve,reject);
          } else {
            queue.create('queryUser', { user: { id_str: job.data.list.owner } } ).attempts(5).removeOnComplete( true ).save();
            metrics.counter("rel_user_not_exist").increment();
            reject({ reason: "user.neo4jID not in redis", list: savedList });
          }
        });
      });
    })
    .then(function() {
      return new RSVP.Promise(function(resolve) {
        queue.create('queryListMembers', { list: { id_str: job.data.list.id_str } } ).attempts(5).removeOnComplete( true ).save();
        resolve();
      });
    })
    .then(done)
    .catch(function(err) {
      logger.error("receiveUserListOwnership error on %j\n%j\n--", job.data.list, err);
      done(err);
    });
  });

});

queue.process('queryUserListOwnership', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);
  queryUserListOwnership(job.data.user, job.data.cursor)
  .then(done)
  .catch(function(err) {
    logger.error("queryUserListOwnership error %j: %j", job.data, err);
  });
});

function queryUserListOwnership(user, cursor) {
  return new Promise(function(resolve, reject) {
    logger.info("queryUserListOwnership %s", user.id_str);
    limiter.removeTokens(1, function(err, remainingRequests) {
      T.get('lists/ownerships', { user_id: user.id_str, cursor: cursor, count: 1000 }, function(err, data)
      {
        if (err){
          logger.error("twitter api error %j %j", user, err);
          reject(err);
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryUserListOwnership twitter api callback");
        for (list of data.lists){
          var filteredList = {
            id_str: list.id_str,
            name: list.name,
            uri: list.uri,
            subscriber_count: list.subscriber_count,
            member_count: list.member_count,
            mode: list.mode,
            description: list.description,
            slug: list.slug,
            full_name: list.full_name,
            created_at: list.created_at,
            owner: list.user.id_str
          }
          queue.create('receiveUserListOwnership', { list: filteredList } ).attempts(5).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          queue.create('queryUserListOwnership', { user: user, cursor: data.next_cursor_str }).attempts(5).removeOnComplete( true ).save();
        }
        resolve(data.lists);
      });
    });
  });
};

function saveListToMongo(list) {
  return db.collection("twitterLists").update(
    { 'id_str': list.id_str },
    { $set: {
      'id_str': list.id_str,
      twitterInfo: list,
    },
    $currentDate: { 'timestamps.updated': { $type: "timestamp" } }
  },
  { upsert: true });
};

function upsertListToNeo4j(node) {
  delete node.id;

  return new RSVP.Promise( function (resolve, reject) {
    logger.trace('upserting %s %s', node.name, node.id_str);
    neo4j.find( { id_str: node.id_str }, false, "twitterList" ,
    function(err, results) {
      if (err){
        logger.error("neo4j find %s %j",node.name, err);
        metrics.counter("neo4j_find_error").increment();
        reject(err);
        return;
      }

      if (results.length > 0){
        logger.debug("found %j", results);
        //if results.length > 1, flag as error
        redis.hset(util.format("twitterList:%s",node.id_str), "neo4jID", results[0].id, function(err, res) { });
        metrics.counter("neo4j_exists").increment();
        resolve(results[0]);
        return;
      }

      neo4j.save(node, "twitterList", function(err, savedNode) {
        if (err){
          logger.error("neo4j save %s %j", node.name, err);
          metrics.counter("neo4j_save_error").increment();
          reject(err);
          return;
        }
        redis.hset(util.format("twitterList:%s",node.id_str), "neo4jID", savedNode.id, function(err, res) { });
        logger.debug('inserted list %s', savedNode.name);
        metrics.counter("neo4j_inserted").increment();
        resolve(savedNode);
      });
    });
  });
}

function setListOwnership(user, list) {
  assert( typeof(user.id) == "number");
  assert( typeof(list.id) == "number");
  return new RSVP.Promise( function (resolve, reject) {
    logger.trace("setListOwnership %j %j", user, list);
    neo4j.queryRaw("start x=node({idx}), n=node({idn}) MATCH (x)-[r:owns]->(n) RETURN x,r,n",
    { idx: user.id, idn: list.id }, function(err, results) {
      if (err){
        logger.error("neo4j find error %j",err);
        metrics.counter("rel_find_error").increment();
        reject("error");
        return;
      }
      logger.trace("neo4j found %j", results);
      if (results.data.length > 0) {
        //TODO search for duplicates and remove duplicates
        logger.debug("relationship found %j", results.data[0][1]);
        metrics.counter("rel_already_exists").increment();
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
      //set this to cypher create unique
      neo4j.relate(user.id, 'owns', list.id, function(err, rel) {
        if (err){
          logger.error("neo4j save error %j %j", { user: user, list: list }, err);
          metrics.counter("rel_save_error").increment();
          reject("error");
          return;
        }
        logger.debug("saved relationship %j", rel);
        metrics.counter("rel_saved").increment();
        resolve(rel);
      });
    })
  });
}

queue.process('queryListMembers', function(job, done) {
  //  logger.info("received job");
  logger.trace("queryListMembers received job %j", job);
  queryListMembers(job.data.list)
  .then(done)
  .catch(function(err) {
    logger.error("queryListMembers error: %j", err);
  });
});

function queryListMembers(list, cursor) {
  return new Promise(function(resolve, reject) {
    logger.info("queryListMembers");
    limiterMembers.removeTokens(1, function(err, remainingRequests) {
      T.get('lists/members', { list_id: list.id_str, cursor: cursor, count: 5000 }, function (err, data)
      {
        if (err){
          logger.error("twitter api error %j", err);
          reject(err);
          return;
        }
        logger.trace("Data %j", data);
        logger.debug("queryListMembers twitter api callback");
        for (user of data.users){
          queue.create('receiveListMembers', { list: list, user: { id_str: user.id_str } } ).attempts(5).removeOnComplete( true ).save();
        }
        if (data.next_cursor_str !== '0'){
          queue.create('queryListMembers', { list: list, cursor: data.next_cursor_str }).attempts(5).removeOnComplete( true ).save();
        }
        resolve(data.lists);
      });
    });
  });
};

queue.process('receiveListMembers', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);

  redis.hgetall(util.format("twitter:%s", job.data.user.id_str), function(err, member) {
    if (member && member.neo4jID && member.neo4jID != "undefined"){

      redis.hgetall(util.format("twitterList:%s",job.data.list.id_str), function(err, list) {
        if (list && list.neo4jID && list.neo4jID != "undefined"){
          setListMember({ id: parseInt(member.neo4jID) }, { id: parseInt(list.neo4jID) }).then(done);
        } else {
          metrics.counter("rel_list_not_exist").increment();
          logger.error("neo4j list not in redis %j",err);
          done();
        }
      });
    } else {
      queue.create('queryUser', { user: job.data.user } ).attempts(5).removeOnComplete( true ).save();
      metrics.counter("rel_user_not_exist").increment();
      logger.error("neo4j user not in redis %j",err);
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
        metrics.counter("rel_find_error").increment();
        reject("error");
        return;
      }
      logger.trace("neo4j found %j", results);
      if (results.data.length > 0) {
        //TODO search for duplicates and remove duplicates
        logger.debug("relationship found %j", results.data[0][1]);
        metrics.counter("rel_already_exists").increment();
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
          metrics.counter("rel_save_error").increment();
          reject("error");
          return;
        }
        logger.debug("saved relationship %j", rel);
        metrics.counter("rel_saved").increment();
        resolve(rel);
      });
    })
  });
}
