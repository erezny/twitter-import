var util = require('util');

module.exports = {
  twitter: {
    api: {
      consumer_key:         process.env.TWITTER_CONSUMER_KEY,
      consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
      //access_token:         process.env.TWITTER_ACCESS_TOKEN,
      //access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
      app_only_auth:        true
    },
    controller: {
      url: util.format('mongodb://%s:%s@%s:%d/%s?authMechanism=SCRAM-SHA-1&authSource=%s',
        process.env.MONGO_USER,
        process.env.MONGO_PASSWD,
        process.env.MONGO_HOST,
        process.env.MONGO_PORT,
        process.env.MONGO_DATABASE,
        process.env.MONGO_DATABASE
      ),
    },
  },

  logger:{
    level: 'info',
    root: './',
  },
};
