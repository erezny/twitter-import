
var CatchSigterm = function() {
  var _this = this;
  this.v = true;
  function interrupt_running( sig ) {
    _this.stop();
  }
  process.once( 'SIGTERM', interrupt_running);
  process.once( 'SIGINT', interrupt_running);
};
CatchSigterm.prototype.get = function() { return this.v; };
CatchSigterm.prototype.stop = function() { this.v = false; };

module.exports = {
  uniqArray: function(array, key_function)
  {
    var list = [];
    var unique = {};
    if ( !key_function){
      key_function = function(obj) {
        return obj;
      };
    }
    for (var i in array){
      if ( typeof(unique[key_function(array[i])]) == "undefined"){
        list.push(array[i]);
      }
      unique[key_function(array[i])] = 0;
    }
    return list;
  },

  isEmpty: function(map) {
    for (var key in map) {
      if (map.hasOwnProperty(key)) {
         return false;
      }
    }
    return true;
  },
  CatchSigterm: CatchSigterm

};
