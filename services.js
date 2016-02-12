var forever = require('forever-monitor');

var childOpts = {
  max: 3,
  silent: true,
  args: []
};

var childs = {
  twitter: {
    apiToMongo: new (forever.Monitor)('services/twitter/apiToMongo.js', childOpts),
    checkNeo4jFollowers: new (forever.Monitor)('services/twitter/checkNeo4jFollowers.js', childOpts),
    checkNeo4jFriends: new (forever.Monitor)('services/twitter/checkNeo4jFriends.js', childOpts),
    checkNeo4jUsers: new (forever.Monitor)('services/twitter/checkNeo4jUsers.js', childOpts),
  }
}
child.on('exit', function () {
  console.log('your-filename.js has exited after 3 restarts');
});

child.start();
