
var twitter_controller = require('../../lib/twitter/controller.js');
twitter_controller.init(function(){});

var assert = require('assert');

describe('twitter.controller', function(){
    before( function(){
    });

    after( function(){
      twitter_controller.db.close();
    });

    it('should have queryUser', function(){

      //console.log(typeof(twitter_controller.queryUser));
      assert.equal(typeof(twitter_controller.queryUser), 'function');
      //done();

    });

  it('should have updateUser', function(){

    //console.log(typeof(twitter_controller.updateUser));
    assert.equal(typeof(twitter_controller.updateUser), 'function');

  });

  it('should have updateTweet', function(){

    //console.log(typeof(twitter_controller.saveTweet));
    assert.equal(typeof(twitter_controller.updateTweet), 'function');

  });

  it('should have saveFollowers', function(){

    assert.equal(typeof(twitter_controller.saveFollowers), 'function');

  });

  it('should have saveFollowing', function(){

    assert.equal(typeof(twitter_controller.saveFollowing), 'function');

  });

  it('should have a database connection', function(){
    //console.log(twitter_controller.db);
    assert(twitter_controller.db !== null, 'collection not ready');

  });

  var user = {
        "id": 16876313,
        "id_str": "16876313",
        "name": "Elliott Rezny",
        "screen_name": "erezny",
        "followers_count": 74,
        "friends_count": 258,
      };

  var insertUser = {
        "id": 76,
        "id_str": "76",
        "name": "Trombones",
        "screen_name": "trombin",
        "followers_count": 80000,
        "friends_count": 76,
      };


  describe('#queryUser()', function(){

      it('should return user from database', function(done){

          twitter_controller.queryUser(user, function(err, result_){

            assert(err === null, 'query returned an error');
            result = result_;
            //console.log(result);
            assert(result.name == 'Elliott Rezny', 'did not return 1 object');

            done();
          });

      });


    });

  describe('#updateUser()', function(){


    it('should add user not already in database', function(done){

      twitter_controller.updateUser(insertUser, function(err, result_){

        assert(err === null, 'query returned an error');
        //console.log(result);
        done();
      });

    });

    before(function(){

      insertUser = {
            "id": 76,
            "id_str": "76",
            "name": "Brass Rails",
            "screen_name": "trombin",
            "followers_count": 80000,
            "friends_count": 76,
          };

    });

    it('should change user in database', function(done){

      twitter_controller.updateUser(insertUser, function(err, result_){

        assert(err === null, 'query returned an error');
        //console.log(result);
        done();
      });

    });


  });

  describe('#updateTweet()', function(){

    var tweet = {
      text: "RT @ChinaFile: Game Boys—In China's vast subculture of video game addicts, a few go pro and get rich—Gregory Isaacson via @aeonmag—http://t…",
      id: 563843714552844289,
      id_str: "563843714552844289",
      user: {
        id: 481943972,
        followers_count: 54254,
        id_str: "481943972",
        screen_name: "aeonmag",
      },
    }

    it('should add tweet not already in database', function(done){

      twitter_controller.updateTweet(tweet, function(err, result_){

        assert(err === null, 'query returned an error');
        //console.log(result);
        done();
      });

    });



    it('should change tweet in database', function(done){

      twitter_controller.updateTweet(tweet, function(err, result_){

        assert(err === null, 'query returned an error');
        //console.log(result);
        done();
      });

    });



  });

});
