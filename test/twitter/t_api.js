
var twitter_query = require('../../lib/twitter/api.js');

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

  it('should have queryFollowers', function(){

    console.log(typeof(twitter_query.queryFollowers));
    assert.equal(typeof(twitter_query.queryFollowers), 'function');
    //done();

  });

  it('should have queryFriends', function(){

    //console.log(typeof(twitter_query.queryFriends));
    assert.equal(typeof(twitter_query.queryFriends), 'function');
    //done();

  });

  it('should have queryUser', function(){

    //console.log(typeof(twitter_query.queryUser));
    assert.equal(typeof(twitter_query.queryUser), 'function');
    //done();

  });

  describe('#queryFollowers()', function(){


    it('should return close to the correct number of followers for a user', function(done){

      var followers = [];

      twitter_query.queryFollowers(user, function(results, finished){

        followers.push.apply(followers, results);

        if (finished){
          assert(Math.abs(followers.length - user.followers_count) < 5);
          done();
        }

      });


    });


  });

  describe('#queryFriends()', function(){

    it('should return close to the correct number of followers for a user', function(done){

      var friends = [];

      twitter_query.queryFriends(user, function(results, finished){

        friends.push.apply(friends, results);

        if (finished){
          assert(Math.abs(friends.length - user.friends_count) < 5);
          done();
        }

      });

    });

  });

  describe('#queryUser()', function(){

    it('should return a user', function(done){

      twitter_query.queryUser(user, function(err, returnedUser){
        assert(err === null, 'query returned an error');

        assert(returnedUser.description);
        done();


      });

    });

  });

});
