
module.exports = {
  filterUser: function(user) {
    return {
      id_str: user.id_str,
      screen_name: user.screen_name,
      name: user.name,
      followers_count: user.followers_count,
      friends_count: user.friends_count,
      favourites_count: user.favourites_count,
      description: user.description,
      location: user.location,
      statuses_count: user.statuses_count,
      protected: user.protected
    };
  }
}
