match (n:twitterUser)
where (not exists(n.screen_name)) and (not n.protected )
return n
order by n.weighted_vip_distance desc limit 1800

match (auth:twitterOAuth)
match (auth)-[:oauth]-(user:twitterUser) with auth, user
optional match (user)-[:follows*..2]->(p:twitterUser)
where not (user)-[]-(p) and p.friends_imported_count < (p.friends_count * .5)
with auth, head(p)

profile match (auth:twitterOAuth)
match (auth)-[:oauth]-(user:twitterUser)
with auth, user where user.analytics_updated >=10
optional match (user)-[:follows*1..2]->(p:twitterUser)
where p.friends_imported_count < (p.friends_count * .5)
with p, p.friends_imported_count / p.friends_count as percent_finished,  auth
order by percent_finished desc
with auth, head(collect(p)) as import
return auth, import

profile match (auth:twitterOAuth)
match (auth)-[:oauth]-(user:twitterUser)
with auth, user where user.analytics_updated >=10
optional match (user)-[:follows*1..3]->(p:twitterUser)
where not exists(p.screen_name)
with p, auth limit 100
return auth, collect (p) as import

profile match (:service{type:"VIP"})-[*1..3]->(user:twitterUser)
where user.screen_name is null
optional match (user)<-[:follows]-(p:twitterUser)
where p.screen_name is not null
with user, count(p) as weight order by weight desc limit 100
return user, weight
