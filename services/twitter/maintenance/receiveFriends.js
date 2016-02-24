
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');

const crow = require("crow-metrics");
const request = require("request");
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.friends.receive.maintenance");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

var BloomFilter = require("bloomfilter").BloomFilter;

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
        'COUNT', '20000',
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
  setInterval(function() {
    scan();
  }, 30 * 60 * 60 * 1000);
});

// 10M entries, 1 false positive
var relationshipBloom = new BloomFilter(
  8 * 1024 * 1024 * 40, // MB
  23        // number of hash functions.
);

// 10M entries, 1 false positive
var jobBloom = new BloomFilter(
  8 * 1024 * 1024 * 40, // MB
  23        // number of hash functions.
);

var found = 0;
function scanJob(id){
  return new RSVP.Promise( function (resolve, reject) {
    kueRedis.hgetall(id, function (err, obj) {
      var jobID = id.replace("twitter:job:", "");
      var rel_id, job_id;
      try {
        obj.data = JSON.parse(obj.data);
        if (obj.data.user.id_str < obj.data.friend.id_str){
          rel_id = util.format("%s:%s", obj.data.user.id_str, obj.data.friend.id_str );
          job_id = util.format("%s:%s:%s", jobID, obj.data.user.id_str, obj.data.friend.id_str );
        } else {
          rel_id = util.format("%s:%s", obj.data.friend.id_str, obj.data.user.id_str );
          job_id = util.format("%s:%s:%s", jobID, obj.data.friend.id_str, obj.data.user.id_str );
        }
      } catch (err) {
        resolve();
        return;
      }

      if (obj.type == 'receiveFriend' && obj.state == 'inactive'){

        if (relationshipBloom.test(rel_id) && !jobBloom.test(job_id)) {
          removeJob(jobID).finally(resolve);
        } else {
          relationshipBloom.add(rel_id);
          jobBloom.test(job_id);
          found++;
          resolve();
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
