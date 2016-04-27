profile match (n:twitterUser) where n.screen_name is null
return count (n) as to_import

match (n:twitterUser)
  where n.analytics_updated is null or n.analytics_updated < 10
  with n limit 100000
optional match followerships=(n)<-[:follows]-(m:twitterUser)
  where not m.screen_name is null
  with n, size(collect( followerships)) as followers
optional match friendships=(n)-[:follows]->(l:twitterUser)
  where not l.screen_name is null
  with n, followers, size(collect (friendships)) as friends
set n.followers_imported_count = followers,
    n.friends_imported_count = friends,
    n.analytics_updated = 10
    with n where n.followers_imported_count > n.followers_count or n.friends_imported_count > n.friends_count
    return n

return sum(n.friends_imported_count) as friends_imported, sum(n.friends_count) as friends_count,
  sum(n.followers_imported_count) as followers_imported, sum(n.followers_count) as followers_count

match (n:twitterUser)
  where exists(n.friends_imported_count) and n.friends_count > 0
  with n, floor(tofloat(n.friends_imported_count) / tofloat(n.friends_count) * 100) as friends_imported_percent
  return distinct(friends_imported_percent) as imported_percent, count(*) as count, head(collect (n)) as node
  where imported_percent < 100 order by imported_percent desc

match (n:twitterUser)
  where exists(n.friends_imported_count) and n.friends_count > 0
  with n, floor(tofloat(n.friends_imported_count) / tofloat(n.friends_count) * 100) as friends_imported_percent
  where friends_imported_percent <= 100
  return distinct(friends_imported_percent) as imported_percent, count(*) as count
  order by imported_percent desc

match (n:twitterUser)
  where n.followers_imported_count <= and n.followers_count > 0
  with floor(tofloat(n.followers_imported_count) / tofloat(n.followers_count) * 100) as followers_imported_percent
  return distinct(followers_imported_percent) as imported_percent, count(*) as count

match (n:twitterUser)
optional match (n)-[r:follows]->(:twitterUser)
with n, count(r) as followers_found_count
return n

match (n:twitterUser)
  where n.analytics_updated = 10
  with n limit 100000
optional match followerships=(n)<-[:follows]-(m:twitterUser)
  where m.screen_name is null
  with n, size(collect( followerships)) as followers
optional match friendships=(n)-[:follows]->(l:twitterUser)
  where l.screen_name is null
  with n, followers, size(collect (friendships)) as friends
set n.followers_to_import_count = followers,
    n.friends_to_import_count = friends,
    n.analytics_updated = 20
    with n where n.followers_to_import_count > n.followers_count or n.friends_imported_count > n.friends_count
  return n
