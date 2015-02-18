
// #refactor:10 global config with env specific include

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,

// #refactor:10 for each service, set up config
// api key
// callback url
// rate limit
// scheduler
  init: function (env){
    this.env = require('./env/'+env+'.js');
  }

};
