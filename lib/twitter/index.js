

module.exports = {

  init: function(config, callback){
    this.controller.init(config, callback);
    this.api.init(config);
  },
  controller: require('./controller.js'),
  api: require('./api.js'),

}
