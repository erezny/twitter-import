module.exports = {
  twitter: {
    api: {
      consumer_key:         '',
      consumer_secret:      '',
      access_token:         '',
      access_token_secret:  ''
    },
    controller: {
      url: 'mongodb://localhost:27017/twitter-users',
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
