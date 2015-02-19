
var assert = require('assert');

// #refactor:10 use promises

var MongoClient = require('mongodb').MongoClient;
var logger;

var url = 'mongodb://localhost:27017/viewer-dev';
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
  keenClient.addEvents(events, function(err, res) {
    if (err) {
      console.log("Oh no, an error!");
    } else {
      console.log("Hooray, it worked!");
    }
  });
};

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,

  init: function (config, db_){
  /*if (!callback) {
    callback = config;
    config = {};
  }
  else{ */
    logger = config.logger;
    keenClient = new Keen(config.env.keen);
    this.db = db_;
    this.collection = this.db.collection('socialGraph');
    this.tweetCollection = this.db.collection('tweets');
  // }

    messageBuffer.Total = [];

    // engine.emit('dbReady');
    this.countAll();
    setInterval(this.countAll, 30*1000);

  },

  countAll: function(){
    module.exports.collection.count({}, function(err, results){
      messageBuffer.Total.push(results);
      if (messageBuffer.Total.length > 29){
        sendMessages("Total", messageBuffer.Total.splice(0,29));
      }

      logger.info("Total objects in socialGrqph: ", results);
    });
  },/*
  countAll: function(){
    module.exports.collection.count({'internal.query_followers': true}, function(err, results){
      messageBuffer.Total.push(results);
      if (messageBuffer.Total.length > 29){
        sendMessages("", messageBuffer.Total.splice(0,29));
      }

      logger.info("Followers Query Queue: ", results);
    });
  },*/
};
