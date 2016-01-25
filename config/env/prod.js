var util = require('util');

module.exports = {
  twitter: {
    api: {
      consumer_key:         '',
      consumer_secret:      '',
      access_token:         '',
      access_token_secret:  ''
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
  keen: {
    projectId: '',
    writeKey: '',
    readKey: '',
    masterKey: '',
    protocol: 'https',              // String (optional: https | http | auto)
    host: 'api.keen.io/3.0',        // String (optional)
    requestType: 'jsonp'            // String (optional: jsonp, xhr, beacon)
  },
};
