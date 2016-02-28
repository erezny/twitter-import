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

var childs = {
  twitter: {
  //  apiToMongo: (new (forever.Monitor)('./services/twitter/apiToMongo.js', childOpts)).start(),
//    checkNeo4jFollowers: new (forever.Monitor)('./services/twitter/checkNeo4jFollowers.js', childOpts).start(),
//    checkNeo4jFriends: new (forever.Monitor)('./services/twitter/checkNeo4jFriends.js', childOpts).start(),
//    checkNeo4jUsers: new (forever.Monitor)('./services/twitter/checkNeo4jUsers.js', childOpts).start(),
    queryUsers: new (forever.Monitor)('./services/twitter/API/queryUsers.js', childOpts).start(),
    receiveUsers: new (forever.Monitor)('./services/twitter/Controller/receiveUsers.js', childOpts).start(),
    queryListMembers: new (forever.Monitor)('./services/twitter/API/queryListMembers.js', childOpts).start(),
    queryListOwnership: new (forever.Monitor)('./services/twitter/API/queryListOwnership.js', childOpts).start(),
    listMembers: new (forever.Monitor)('./services/twitter/Controller/listMembers.js', childOpts).start(),
    listOwnership: new (forever.Monitor)('./services/twitter/Controller/listOwnership.js', childOpts).start(),
    queryFriendsList: new (forever.Monitor)('./services/twitter/API/queryFriendsList.js', childOpts).start(),
    receiveFriends: new (forever.Monitor)('./services/twitter/Controller/receiveFriends.js', childOpts).start(),
    queryFriendsIDs: new (forever.Monitor)('./services/twitter/API/queryFriendsIDs.js', childOpts).start(),
    //queryFollowerIDs: new (forever.Monitor)('./services/twitter/API/queryFollowersIDs.js', childOpts).start(),
    //queryFollowersList: new (forever.Monitor)('./services/twitter/API/queryFollowersList.js', childOpts).start(),
    fillFriendsQueue: new (forever.Monitor)('./services/twitter/maintenance/fillFriendsIDsQueue.js', childOpts).start(),
    statsVIP: new (forever.Monitor)('./services/twitter/maintenance/statsVIP.js', childOpts).start(),
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
