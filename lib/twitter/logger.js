
var assert = require('assert');

// #refactor:10 use promises

var MongoClient = require('mongodb').MongoClient;
var logger;

var url;
//var events = require('events').EventEmitter;
//var engine = new events();

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,

  init: function (config, db_) {
    // simplify:20 reuse controller collections
    logger = config.logger;
    url = config.env.twitter.controller.url;

    this.db = db_;
    this.collection = this.db.collection('socialGraph');
    this.tweetCollection = this.db.collection('tweets');

    //this.countSummarize();
    //setInterval(this.countSummarize, 60 * 1000);
  },
  countSummarize: function() {
    module.exports.collection.count({}, function(err, total) {
      module.exports.collection.count({
        $or: [
          { 'state.query_followers': 1 },
          { 'state.query_friends': 1 },
          { 'state.expand_followers': { $gt: 0 } },
          { 'state.expand_friends': { $gt: 0 } }
        ] }, function(err, remaining) {
        logger.info("To query in socialGraph: %d / %d", remaining, total);
    });
    });
  }

};
