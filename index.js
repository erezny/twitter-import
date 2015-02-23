
// TODO can the init be removed by using a constructor?
var config = require('./config/');
config.init();
var twitter = require('./lib/twitter/');

var util = require('./lib/util.js');

// TODO change logger type based on config file
var logger = require('tracer').colorConsole(config.env.logger);
config.logger = logger;

twitter.init(config, function(){
  //callback
});
