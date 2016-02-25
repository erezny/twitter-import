
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');
var cacheCtrl = require('../../../lib/twitter/cacheCtrl');
const metrics = require('../../../lib/crow.js').withPrefix("twitter.users.maintenance");

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var BloomFilter = require("bloomfilter").BloomFilter;
// 10M entries, 1 false positive
var bloom = new BloomFilter(
  8 * 1024 * 1024 * 40, // MB
  23        // number of hash functions.
);

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

// append scanrx methods for any clients
var kueRedis = require("redis").createClient( {
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT)
});

process.once( 'SIGTERM', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
});

process.once( 'SIGINT', function ( sig ) {
  queue.shutdown( 5000, function(err) {
    console.log( 'Kue shutdown: ', err||'' );
    process.exit( 0 );
  });
});

var cursor = '0';
var lastCount = 0;
var count = 0;

function scan() {
    kueRedis.scan(
        cursor,
        'MATCH', 'twitter:job:*',
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
              //  console.log('Array of matching keys', keys);
                for (key of keys){
                  if (!key.match("log")){
                    jobs.push(scanJob(key));
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
              console.log("count: %d \tcache size: %d \tremoved: %d", count, found, removed);
              lastCount = count;
            }

            RSVP.allSettled(jobs).then(scan);
        }
    );
}

kueRedis.select(1, function() {
  scan();
});

var found = 0;
function scanJob(id){
  return new RSVP.Promise( function (resolve, reject) {
    kueRedis.hgetall(id, function (err, obj) {
      var jobID = id.replace("twitter:job:", "");
      var id_str;
      try {
        obj.data = JSON.parse(obj.data);
        id_str = obj.data.user.id_str;
      } catch (err) {
        resolve();
        return;
      }

      if (obj.type == 'queryUser' && obj.state == 'inactive'){

        if (bloom.test(id_str) ) {
          removeJob(jobID).finally(resolve);
        } else {
          bloom.add(id_str);
          found++;
          cacheCtrl.checkUserQueryTime(obj.data.user)
          .then(resolve, function() {
              removeJob(jobID).finally(resolve);
          })
        }
      } else {
        resolve();
      }
    });
  });
}

var removed = 0;
function removeJob(id){
  return new RSVP.Promise( function (resolve, reject) {
    kue.Job.get( id, function( err, job ) {
      //console.log("remove job", job.id);
       if (job) {
         job.remove( function() {
           removed++;
           resolve();
           //console.log( 'removed ', job.id );
        });
      } else {
        resolve();
      }
    });
  });
}

//untested
function increaseJobPriority(id){
  kue.Job.get( id, function( err, job ) {
    priority = job.priority();
    console.log("priority %j", priority);
    if (priority > -15) {
      job.priority(priority - 1).save();
    }
  });
};

/*
{ max_attempts: '1',
  type: 'queryUser',
  created_at: '1456239917758',
  promote_at: '1456239917758',
  removeOnComplete: 'true',
  updated_at: '1456239917812',
  priority: '0',
  data: '{"user":{"id_str":"1356046153"}}',
  state: 'inactive' }
*/
