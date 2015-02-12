
var twitter_query = require('../../lib/twitter/query.js');

var assert = require('assert');

describe('twitter.query', function(){

  it('should have getFollowers', function(){

    console.log(typeof(twitter_query.getFollowers));
    assert.equal(typeof(twitter_query.getFollowers), 'function');
    //done();

  });

  it('should have getFollowing', function(){

    //console.log(typeof(twitter_query.getFollowing));
    assert.equal(typeof(twitter_query.getFollowing), 'function');
    //done();

  });

  it('should have getUser', function(){

    //console.log(typeof(twitter_query.getUser));
    assert.equal(typeof(twitter_query.getUser), 'function');
    //done();

  });

  describe('#getFollowers()', function(){
    var user = {};

    before(function(){
      user = {
        "id": 16876313,
        "id_str": "16876313",
        "name": "Elliott Rezny",
        "screen_name": "erezny",
        "followers_count": 74,
        "friends_count": 250,
      };
    });

    it('should return close to the correct number of followers for a user', function(done){

      var followers = [];

      twitter_query.getFollowers(user, function(results, finished){
        followers.push.apply(results);
        console.log(followers);
        if (finished){
          assert(Math.abs(followers.length - user.followers_count) < 5);
          done();
        }

      });


    });

  });

  describe('#getFollowing()', function(){

    it('should return close to the correct number of followers for a user', function(done){

      assert(false, 'test not implemented');

      done();

    });

  });

  describe('#getUser()', function(){

    it('should return a user', function(done){

      assert(false, 'test not implemented');

      done();

    });

  });

});
