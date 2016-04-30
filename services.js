var forever = require('forever-monitor');

var childOpts =  {
    'silent': false,            // Silences the output from stdout and stderr in the parent process
    'pidFile': './', // Path to put pid information for the process(es) started
    'max': 10,                  // Sets the maximum number of times a given script should run
    'killTree': true,           // Kills the entire child process tree on `exit`
    'minUptime': 2000,          // Minimum time a child process has to be up. Forever will 'exit' otherwise.
    'spinSleepTime': 15000,      // Interval between restarts if a child is spinning (i.e. alive < minUptime).
    'watch': false,               // Value indicating if we should watch files.
    'env': { },
    'logFile': './log/forever.log', // Path to log output from forever process (when daemonized)
  };

var scripts = [
    './services/twitter/import.js',
    './services/twitter/ui/kue.js'
];
var childs = scripts.map(function (filename) {
  return new (forever.Monitor)(filename, childOpts).start();
});

function addEvents(child) {
  child.on('exit', function () {
    console.log('%s has exited after %d restarts', child.command, child.restarts);
  });
  child.on('restart', function () {
    console.log('%s restarted, count: %d', child.command, child.restarts);
  });
}
process.once( 'SIGTERM', shutdown );
process.once( 'SIGINT', shutdown );

function shutdown(sig) {
  for ( var child of childs)  {
    child.kill(sig);
  }
  setTimeout( function() {
    process.exit( 0 );
  }, 5 * 60 * 1000);
}
