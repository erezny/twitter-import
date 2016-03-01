
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');
var cacheCtrl = require('../../lib/twitter/cacheCtrl');
const metrics = require('../../lib/crow.js').withPrefix("twitter.users.maintenance");
var neo4j = require('../../lib/neo4j.js');

var BloomFilter = require("bloomfilter").BloomFilter;
// 10M entries, 1 false positive
var bloom = new BloomFilter(
  8 * 1024 * 1024 * 40, // MB
  23        // number of hash functions.
);

var RSVP = require('rsvp');
var logger = require('tracer').colorConsole( {
  level: 'debug'
} );

// append scanrx methods for any clients
var redis = require("redis").createClient( {
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT)
});

var cursor = '0';
var lastCount = 0;
var count = 0;

function scan() {
    redis.scan(
        cursor,
        'MATCH', 'twitter:*',
        'COUNT', '1000',
        function(err, res) {
            if (err) throw err;

            // Update the cursor position for the next scan
            cursor = res[0];
            // get the SCAN result for this iteration
            var keys = res[1];

            // Remember: more or less than COUNT or no keys may be returned
            // See http://redis.io/commands/scan#the-count-option
            // Also, SCAN may return the same key multiple times
            // See http://redis.io/commands/scan#scan-guarantees
            // Additionally, you should always have the code that uses the keys
            // before the code checking the cursor.
            var jobs = [];
            if (keys.length > 0) {
                for (key of keys){
                  if (key.search("twitter:[0-9]+" != -1)){
                    jobs.push(scanUser(key));
                  }
                }
                count = count + keys.length;
            }

            // It's important to note that the cursor and returned keys
            // vary independently. The scan is never complete until redis
            // returns a non-zero cursor. However, with MATCH and large
            // collections, most iterations will return an empty keys array.

            // Still, a cursor of zero DOES NOT mean that there are no keys.
            // A zero cursor just means that the SCAN is complete, but there
            // might be one last batch of results to process.

            // From <http://redis.io/commands/scan>:
            // 'An iteration starts when the cursor is set to 0,
            // and terminates when the cursor returned by the server is 0.'
            if (cursor === '0') {
                return console.log('Iteration complete');
            }
            if (count > lastCount + 1000){
            //  console.log("count: %d \tcache size: %d \tremoved: %d", count, found, removed);
              lastCount = count;
            }

            RSVP.allSettled(jobs).then(scan);
        }
    );
}

  scan();

var found = 0;
function scanUser(key){
  return new RSVP.Promise( function (resolve, reject) {
    redis.hgetall(key, function (err, obj) {
      var id_str = key.replace("twitter:", "");
      logger.trace(obj);
      removeDuplicates(id_str, parseInt(obj.neo4jID)).then(resolve, reject);
    });
  });
}

var sem = require('semaphore')(2);
function removeDuplicates(id_str, neo4jID) {
    return new RSVP.Promise( function (resolve, reject) {
      sem.take(function() {
        logger.trace("querying %s, %s", id_str, neo4jID);
        neo4j.queryRaw("match (n:twitterUser{id_str:{id_str}}) where not( id(n) = {neo4jID}) return n ",
          { id_str: id_str, neo4jID: neo4jID }, function(err, results) {
            sem.leave();
          if (err){
              logger.error("neo4j error %j", err);
              reject("error");
          } else {
            if (results.data.length > 0) {
              logger.trace("returned %j", results);
              var jobs = [];
              for (node of results.data){
                logger.trace("node %j", node);
                jobs.push(removeNode(node[0].metadata.id));
              }
              RSVP.allSettled(jobs).then(resolve);
            } else {
              resolve();
            }
          }
        });
      });
    });
}

var removeSem = require('semaphore')(2);
function removeNode(id) {
    return new RSVP.Promise( function (resolve, reject) {
      removeSem.take(function() {
        logger.debug("removing %s", id);
        neo4j.delete(id, true, function(err) {
          removeSem.leave();
          if (err){
            logger.error("neo4j error %j", err);
            reject("error");
          } else {
            resolve();
          }
        });
      });
    });
}
