
var util = require('../lib/util.js');
var assert = require('assert');

describe('util', function() {

  it('should have uniqArray', function() {
    assert.equal(typeof(util.uniqArray), 'function');
  });

  it('should have isEmpty', function() {
    assert.equal(typeof(util.isEmpty), 'function');
  });

  it('should have CatchSigterm', function() {
    assert.equal(typeof(util.CatchSigterm), 'function');
  });

  describe('#uniqArray()', function() {

    it('should return all values of a unique array', function(done) {

      var unique_array = [ 1,2,3 ];
      var result = util.uniqArray(unique_array);

      //console.log(result);

      assert.equal(result.length, unique_array.length);
      assert.equal(result[0], unique_array[0]);
      assert.equal(result[1], unique_array[1]);
      assert.equal(result[2], unique_array[2]);

      done();

    });

    it('should return fewer values of a non-unique array', function(done) {

      var unique_array = [ 1,2,3,3 ];
      var result = util.uniqArray(unique_array);

      //console.log(result);

      assert.equal(3, result.length);

      done();

    });
  });

  describe('#isEmpty()', function() {

    it('should return true for {}', function(done) {

      assert.equal(true, util.isEmpty({ }));
      done();

    });

    it('should return false for object with keys', function(done) {

      assert.equal(false, util.isEmpty({ a: 1 }));
      assert.equal(false, util.isEmpty({ a: null }));
      assert.equal(false, util.isEmpty([ 1,2,3 ]));
      done();
    });

    it('should return true for null', function(done) {

      assert.equal(true, util.isEmpty(null));
      done();

    });
  });

  describe('#CatchSigterm()', function() {

    it('should set to true upon construction', function(done) {
      var keep_running = new util.CatchSigterm();
      console.log(keep_running);
      assert.equal(true, keep_running.get());
      done();
    });

    // it('should catch SIGINT (press ctrl-c) and set to false after SIGINT', function(done) {
    //   var keep_running = new util.CatchSigterm();
    //   this.timeout(6000);
    //   console.log("Press ctrl-c");
    //   setInterval(() => {
    //     if ( !keep_running.get()){
    //       done();
    //     }
    //   }, 1000);
    // });

  });

});
