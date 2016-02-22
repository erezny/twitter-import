var forever = require('forever-monitor');

var childOpts =  {
    'silent': false,            // Silences the output from stdout and stderr in the parent process
    'pidFile': './', // Path to put pid information for the process(es) started
    'max': 10,                  // Sets the maximum number of times a given script should run
    'killTree': true,           // Kills the entire child process tree on `exit`
    'minUptime': 2000,          // Minimum time a child process has to be up. Forever will 'exit' otherwise.
    'spinSleepTime': 1000,      // Interval between restarts if a child is spinning (i.e. alive < minUptime).
    'watch': false,               // Value indicating if we should watch files.
    'env': { },
    'logFile': './log/forever.log', // Path to log output from forever process (when daemonized)
  };

var childs = {
  twitter: {
  //  apiToMongo: (new (forever.Monitor)('./services/twitter/apiToMongo.js', childOpts)).start(),
    checkNeo4jFollowers: new (forever.Monitor)('./services/twitter/checkNeo4jFollowers.js', childOpts).start(),
    checkNeo4jFriends: new (forever.Monitor)('./services/twitter/checkNeo4jFriends.js', childOpts).start(),
    checkNeo4jUsers: new (forever.Monitor)('./services/twitter/checkNeo4jUsers.js', childOpts).start(),
    lists: new (forever.Monitor)('./services/twitter/lists.js', childOpts).start(),
  //  users: new (forever.Monitor)('./services/twitter/users.js', childOpts).start(),
  }
};
function addEvents(child) {
  child.on('exit', function () {
    console.log('%s has exited after %d restarts', child.command, child.restarts);
  });
  child.on('restart', function () {
    console.log('%s restarted, count: %d', child.command, child.restarts);
  });
}
