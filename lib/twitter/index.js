


var events = require('events');

module.exports = {
  openEvents: 0,
  callback: null,
  init: function(config_, callback_){
    this.callback = callback_;
    this.config = config_;
    this.engine = new events.EventEmitter();
    this.controller.init(this.config,this.controller_callback, this);
    this.api.init(this.config);
    this.engines.users.init(this.config, this);
//    this.engine.lists.init(config, this);
  },
  controller_callback: function(that){
    that.config.logger.debug("contoroller set up.");
    that.engine.emit('dbready');
    that.logger.init(that.config, that.controller.db);
    that.engine.on('dbConnection', function()
    {
    that.openEvents++;
    });

    that.engine.on('dbConnectionClose', function()
    {
      that.openEvents--;
      if ( ! that.openEvents ){
        that.logger.info('closed db connection');
        that.controller.db.close();
      }
    });

    that.engine.emit('dbConnection');
    that.callback();
    //that.callback = null;
  },
  config: null,
  controller: require('./controller.js'),
  api: require('./api.js'),
  logger: require('./logger.js'),
  engine: null,
  engines: {
    users: require('./engines/users.js'),
  //  lists: require('./lib/twitter/engines/lists.js'),
  }
};
