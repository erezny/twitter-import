
// #refactor:10 write queries
var util = require('util');
var assert = require('assert');

const crow = require("crow-metrics");
const request = require("request");
const metrics = new crow.MetricsRegistry({ period: 15000, separator: "." }).withPrefix("twitter.users.maintenance");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
metrics.counter("app_started").increment();

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

  setInterval( function() {
  queue.inactiveCount( 'cleanUserJobs', function( err, total ) { // others are activeCount, completeCount, failedCount, delayedCount
    metrics.setGauge("queueSize", { object: "user", category: "Maintenance", status: "inactive", queue: "CleanUserJobs" }, total);
  });
  }, 15 * 1000 );

queue.process('cleanUserJobs', function(job, done) {
  //  logger.info("received job");
  logger.trace("received job %j", job);

  // queue.inactive( function( err, ids ) {
  //   ids.forEach( function( id ) {
  //     kue.Job.get( id, function( err, job ) {
  //       // Your application should check if job is a stuck one
  //       job.inactive();
  //     });
  //   });
  // });
  metrics.counter("finished").increment();
});

var cursor = '0';
var lastCount = 0;
var count = 0;

function scan() {
    kueRedis.scan(
        cursor,
        'MATCH', 'twitter:job:*',
        'COUNT', '100',
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
            if (keys.length > 0) {
              //  console.log('Array of matching keys', keys);
                for (key of keys){
                  if (!key.match("log")){
                    scanJob(key);
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
              console.log("count: %d \tcache size: %d \tremoved: %d", count, cache.length, removed);
              lastCount = count;
            }

            return scan();
        }
    );
}

kueRedis.select(1, function() {
  scan();
});

var cache = [];
var cacheMax = 1000000;
var hashCache = {};

function scanJob(id){
  kueRedis.hgetall(id, function (err, obj) {
    var jobID = id.replace("twitter:job:", "");
    obj.data = JSON.parse(obj.data);
    if (obj.type == 'queryUser' && obj.state == 'inactive'){

      if (cache.indexOf(obj.data.user.id_str) >= 0){
        removeJob(jobID);
        return;
      }
      if (cache.length < cacheMax){
        //console.log("user pushed to cache %s", obj.data.user.id_str);
        cache.push(obj.data.user.id_str);
        hashCache[obj.data.user.id_str] = { jobID: jobID };
        return;
      }
    }
  });
}

var removed = 0;
function removeJob(id){
  kue.Job.get( id, function( err, job ) {
    //console.log("remove job", job.id);
     job.remove( function() {
       removed++;
       //console.log( 'removed ', job.id );
     });
  });
}

//untested
function increaseJobPriority(id){
  kue.Job.get( id, function( err, job ) {
    job.priority.set(job.priority--).save();
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
