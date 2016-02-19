
var logger = require('tracer').colorConsole( {
  level: 'trace'
} );
var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});

queue.create('queryUserListOwnership', {
  user: { id_str: "16876313" }, cursor: "-1"
}).save();
