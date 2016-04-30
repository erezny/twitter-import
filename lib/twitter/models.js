
module.exports = {
  filterList: function(list) {
    return {
      id_str: list.id_str,
      name: list.name,
      uri: list.uri,
      subscriber_count: list.subscriber_count,
      member_count: list.member_count,
      mode: list.mode,
      description: list.description,
      slug: list.slug,
      full_name: list.full_name,
      created_at: list.created_at
    };
  },
  filterStatus: function(user) {
    return {
      id_str: user.id_str,
      created_at: user.created_at,
      text: user.text,
      retweet_count: user.retweet_count,
      possibly_sensitive: user.possibly_sensitive,
      lang: user.lang
    };
  },
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
      protected: user.protected,
      listed_count: user.listed_count,
    };
  }
};
