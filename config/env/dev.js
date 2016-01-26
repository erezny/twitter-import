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
      url: util.format('mongodb://%s:%s@%s:%d/%s?authMechanism=SCRAM-SHA-1&authSource=admin',
        process.env.MONGO_USER,
        process.env.MONGO_PASSWD,
        process.env.MONGO_PORT_27017_TCP_ADDR,
        process.env.MONGO_PORT_27017_TCP_PORT,
        process.env.MONGO_COLLECTION
      ),
    },
  },

  logger:{
    level: 'info',
    root: './',
  },
};
