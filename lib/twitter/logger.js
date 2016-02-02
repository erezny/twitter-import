
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

  init: function (config, controller_, influx_) {
    // simplify:20 reuse controller collections
    logger = config.logger;
    url = config.env.twitter.controller.url;

    this.controller = controller_;
    this.influx = influx_;

    this.countSummarize();
    setInterval(this.countSummarize, 5 * 60 * 1000);
  },
  countSummarize: function() {
      module.exports.controller.countRemaining(
         function(err, remaining) {
          var point = { value: remaining, time: new Date() };
          module.exports.influx.writePoint("enQueue", remaining, { group: "total" },
              function(err, response) {
            if (err){
              logger.error("influx error: %j %j", err, response);
            }
            })
        logger.info("To query in socialGraph: %d", remaining);
    });
  }

};
