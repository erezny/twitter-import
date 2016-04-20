
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
    }
  }
}
