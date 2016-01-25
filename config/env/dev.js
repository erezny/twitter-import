var util = require('util');

module.exports = {
  twitter: {
    api: {
      consumer_key:         process.env.TWITTER_CONSUMER_KEY,
      consumer_secret:      process.env.TWITTER_CONSUMER_SECRET,
      access_token:         process.env.TWITTER_ACCESS_TOKEN,
      access_token_secret:  process.env.TWITTER_ACCESS_TOKEN_SECRET
    },
    controller: {
      url: util.format('mongodb://%s:%d/%s',
        process.env.MONGO_TUTUM_SERVICE_HOSTNAME,
        process.env.MONGO_PORT_27017_TCP_PORT,
        process.env.MONGO_COLLECTION
      ),
    },
  },

  logger:{
    level: 'debug',
    root: './',
  },
};
