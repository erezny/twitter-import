
var assert = require('assert');

// #refactor:10 use promises

var MongoClient = require('mongodb').MongoClient;
var logger;

var url;
//var events = require('events').EventEmitter;
//var engine = new events();

var Keen = require('keen-js');

// Configure instance. Only projectId and writeKey are required to send data.
var keenClient = null;

var messageBuffer = {};
var timeLastSent = [];

var sendMessages = function(collection, messages){
  // send multiple events to Keen IO
  var events = {};
  events[collection] = messages;
  if (keenClient)
  {
    keenClient.addEvents(events, function(err, res) {
      if (err) {
        logger.error("Oh no, an error!");
      } else {
        logger.debug("Hooray, it worked!");
      }
    });
  }
};

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,

  init: function (config, db_){
    // simplify:20 reuse controller collections
    logger = config.logger;
    url = config.env.twitter.controller.url;
    if (config.env.keen.projectId.length > 10)
    {
      keenClient = new Keen(config.env.keen);
    }
    this.db = db_;
    this.collection = this.db.collection('socialGraph');
    this.tweetCollection = this.db.collection('tweets');
  // }

    messageBuffer.Total = [];

    // engine.emit('dbReady');
    this.countAll();
    this.countRemaining();
    this.countFinished();
    setInterval(this.countAll, 60*1000);
    setInterval(this.countRemaining, 60*1000);
    setInterval(this.countFinished, 60*1000);
  },

  countAll: function(){
    module.exports.collection.count({}, function(err, results){
      messageBuffer.Total.push(results);
      if (messageBuffer.Total.length > 29){
        sendMessages("Total", messageBuffer.Total.splice(0,29));
      }

      logger.info("Total objects in socialGrqph: ", results);
    });
  },

  countRemaining: function(){
    module.exports.collection.count({$or: [{'state.query_followers': 1},{'state.query_friends': 1},{'state.expand_followers': {$gt: 0}},{'state.expand_friends': {$gt: 0}}] }, function(err, results){
      messageBuffer.Total.push(results);
      if (messageBuffer.Total.length > 29){
        sendMessages("Unprocessed", messageBuffer.Total.splice(0,29));
      }

      logger.info("Objects left to query in socialGrqph: ", results);
    });
  },
  countFinished: function(){
    module.exports.collection.count({'state.query_followers': 0,'state.query_friends': 0,'state.expand_followers': 0,'state.expand_friends': 0}, function(err, results){
      messageBuffer.Total.push(results);
      if (messageBuffer.Total.length > 29){
        sendMessages("Finished", messageBuffer.Total.splice(0,29));
      }

      logger.info("Finished objects in socialGrqph: ", results);
    });
  },

};
