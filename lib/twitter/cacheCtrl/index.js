var RSVP = require('rsvp');
var util = require('util');

var redis = require("redis").createClient({
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT),
});

function checkUserQueryTime(user){
  return new Promise(function(resolve, reject) {
    var key = util.format("twitter:%s", user.id_str);
    var currentTimestamp = new Date().getTime();
    redis.hgetall(key, function(err, obj) {
      if ( !obj || !obj.queryTimestamp || obj.queryTimestamp > parseInt((+new Date) / 1000) - (60 * 60 * 24) ) {
        resolve(user);
      } else {
        reject( { message: "user recently queried" } );
      }
    });
  });
}

module.exports = {
  checkUserQueryTime: checkUserQueryTime
}
