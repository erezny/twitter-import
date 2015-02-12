
var util = require('../lib/util.js');
var assert = require('assert');

describe('util', function(){

  it('should have uniqArray', function(){
    //console.log(typeof(util.uniqArray));
    assert.equal(typeof(util.uniqArray), 'function');
    //done();
  });

  describe('#uniqArray()', function(){
    it('should return all values of a unique array', function(done){

      var unique_array = [1,2,3];
      var result = util.uniqArray(unique_array);

      //console.log(result);

      assert.equal(result.length, unique_array.length);
      assert.equal(result[0], unique_array[0]);
      assert.equal(result[1], unique_array[1]);
      assert.equal(result[2], unique_array[2]);

      done();

    });
    it('should return fewer values of a non-unique array', function(done){

      var unique_array = [1,2,3,3];
      var result = util.uniqArray(unique_array);

      //console.log(result);

      assert.equal(3, result.length);

      done();

    });
  });

});
