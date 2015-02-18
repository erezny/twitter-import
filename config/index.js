
// #refactor:10 global config with env specific include

module.exports = {
  db: null,
  collection: null,
  tweetCollection: null,
  env_str: 'dev',

// #refactor:10 for each service, set up config
// api key
// callback url
// rate limit
// scheduler
  init: function (){
    this.env = require('./env/'+this.env_str+'.js');
  }

};
