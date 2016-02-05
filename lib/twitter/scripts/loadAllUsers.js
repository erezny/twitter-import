
var util = require('util');
var RSVP = require('rsvp');
var test = require('assert');
var RateLimiter = require('limiter').RateLimiter;
var Queue = require('bull');

var MongoClient = require('mongodb').MongoClient,
  test = require('assert');

var twitterCollection  = null;

var logger = require('tracer').colorConsole( {
  level: 'trace'
} );

var userByIDQueue = Queue('loadTwitterUserMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);
var userNetworkQueue = Queue('loadTwitterUserNetworkMongoToNeo4j', process.env.REDIS_PORT, process.env.REDIS_HOST);

var db = null;

//runtime loop
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

  logger.trace("connected to mongo");

  var stream = db.collection("twitterUsers")
  .find( { } , { fields: { screen_name: 1, id_str: 1 } } )
  .sort( )
  .stream();
    // Execute find on all the documents
  stream.on('end', function() {
    logger.info("End of mongo stream");
    //db.close();
  });

  stream.on('data', function(data) {
    logger.trace("adding %s", data.screen_name);
    var tmp = userByIDQueue.add( { id_str: data.id_str } );
  });

});
