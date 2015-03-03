module.exports = {
  twitter: {
    api: {
      consumer_key:         'nqcvYQFZRHRqgerM9F5I7ycft',
      consumer_secret:      'jOCoMqvwdqLm035ObYXgwLAG6icSPgMe4m6o63vxn9E0kTKHjR',
      access_token:         '16876313-GnG6mx61biAP35pkSZPeUffPB9h1rBhNm0wMzMFOh',
      access_token_secret:  'TVYwy47RkyrA2aa4hpVBX39cTyfeHGgXFRCeytnhIaC1Y'
    },
    controller: {
      url: 'mongodb://localhost:27017/twitter',
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
