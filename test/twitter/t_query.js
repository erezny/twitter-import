
var twitter_query = require('../../lib/twitter/query.js');

var assert = require('assert');

describe('twitter.query', function(){
  var user = {};

  before(function(){
    user = {
      "id": 16876313,
      "id_str": "16876313",
      "name": "Elliott Rezny",
      "screen_name": "erezny",
      "followers_count": 74,
      "friends_count": 258,
    };
  });

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


    it('should return close to the correct number of followers for a user', function(done){

      var followers = [];

      twitter_query.getFollowers(user, function(results, finished){

        followers.push.apply(followers, results);

        if (finished){
          assert(Math.abs(followers.length - user.followers_count) < 5);
          done();
        }

      });


    });

  });

  describe('#getFollowing()', function(){

    it('should return close to the correct number of followers for a user', function(done){

      var following = [];

      twitter_query.getFollowing(user, function(results, finished){

        following.push.apply(following, results);

        if (finished){
          assert(Math.abs(following.length - user.friends_count) < 5);
          done();
        }

      });

    });

  });

  describe('#getUser()', function(){

    it('should return a user', function(done){

      twitter_query.getUser(user, function(returnedUser){

        assert(returnedUser.description);
        done();


      });

    });

  });

});
