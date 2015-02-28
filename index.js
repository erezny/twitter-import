
// clarity:20 remove init by using a constructor
var config = require('./config/');
config.init();
var twitter = require('./lib/twitter/');

var util = require('./lib/util.js');

// devOps:20 change logger type based on config file
var logger = require('tracer').colorConsole(config.env.logger);
config.logger = logger;

twitter.init(config, function()
{
  // strengthen:20 if we have a valid username
  if (process.argv[2].length > 2)
  {
    twitter.engine.emit('seedUser',
      {screen_name: process.argv[2]});
  }
  //callback
});

// devOps:10 use forever-monitor
//  var forever = require('forever-monitor');
// var child = new (forever.Monitor)('your-filename.js', {
//   max: 3,
//   silent: true,
//   args: []
// });
//
// child.on('exit', function () {
//   console.log('your-filename.js has exited after 3 restarts');
// });
//
// child.start();
