
var twitter_controller = require('../../lib/twitter/controller.js');

var assert = require('assert');

describe('twitter.controller', function(){

  it('should have updateUser', function(){

    console.log(typeof(twitter_controller.updateUser));
    assert.equal(typeof(twitter_controller.updateUser), 'function');
    //done();

  });

  it('should have saveTweet', function(){

    //console.log(typeof(twitter_controller.saveTweet));
    assert.equal(typeof(twitter_controller.saveTweet), 'function');
    //done();

  });

  describe('#saveUser()', function(){

    it('should change user in database', function(done){

      assert(false, 'test not implemented');

      done();

    });

    it('should add user not already in database', function(done){

      assert(false, 'test not implemented');

      done();

    });

  });

  describe('#saveTweet()', function(){

    it('should change tweet in database', function(done){

      assert(false, 'test not implemented');

      done();

    });

    it('should add tweet not already in database', function(done){

      assert(false, 'test not implemented');

      done();

    });

  });

});
