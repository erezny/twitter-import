match p=(n)-[:follows]->(m:twitterUser) where m.screen_name is null
with n, count(p) as import_friends
set n.import_friends = import_friends
with n
match (n) with n order by n.import_friends desc limit 10


profile match (n:twitterUser) where n.screen_name is null
match p=(n)-[:follows]->(m:twitterUser) where m.screen_name is not null
with n, count(p) as import_friends
set n.user_import_weight = import_friends
with n
match (n) with n order by n.user_import_weight desc limit 10
return n


profile match (n:twitterUser) where n.screen_name is null
match (v:service{type:"VIP"})--(t:twitterUser) with , t
match (n)<-[:follows]-(m:twitterUser) with n, count(distinct m), distinct m
set n.vip_links_p = vip_links
match p=(m:twitterUser)<-[:follows]-(t)
with n, count(p) as vip_links_p
set n.user_import_weight = vip_links_p
with n
match (n) with n order by n.vip_links_p desc limit 10
return n


profile match (n:twitterUser) where n.screen_name is null with n limit 10000
match (v:service{type:"VIP"})--(t:twitterUser) with n, distinct t as t
optional match p=shortestPath((n)<-[*..15]-(t:twitterUser))
with n, count(nodes(p)) as distance
return n, distance order by distance asc limit 10

with distance, count(distinct distance) as distribution
return distance, distribution


distance	distribution
6	1
0	1


set n.vip_links_p = vip_links
match p=(m:twitterUser)<-[:follows]-(t)
with n, count(p) as vip_links_p
set n.user_import_weight = vip_links_p
with n
match (n) with n order by n.vip_links_p desc limit 10
return n


profile
match (v:service{type:"VIP"})--(t:twitterUser)
match (n:twitterUser) where n.screen_name is null with n, rand() as r order by r limit 100


profile match (n:twitterUser) where n.screen_name is null
with n, rand() as r order by r limit 100
match p=(n)-[:follows]->(m:twitterUser) where m.screen_name is not null
with n, count(p) as count_friends
set n.details_friends = count_friends
with n
match p=(n)-[:follows]->(m:twitterUser) where m.screen_name is null
with n, count(p) as count_friends
set n.id_str_friends = count_friends
with n
match p=(n)-[:follows]->(m:twitterUser)
with n, count(p) as imported_friend_rels
set n.imported_friend_rels = imported_friend_rels
with n
match (n) with n order by n.imported_friends desc limit 10
return n

profile match (n:twitterUser) where n.screen_name is null
with n, rand() as r order by r limit 100
match p=(n)-[:follows]->(m:twitterUser) where m.screen_name is not null
with n, count(p) as count_friends
set n.details_friends = count_friends
with n
match (n) with n order by n.details_friends desc limit 10
return n

profile match (n:twitterUser) where n.screen_name is null
with n
match p=(n)-[:follows]->(m:twitterUser)
with n, count(p) as count_friends
set n.friend_rels = count_friends
with n
match (n) with n order by n.imported_friend_rels desc limit 10
return n

profile match (n:twitterUser)
match (v:service{type:"VIP"}) with n, v
optional match p=shortestPath((n)<-[*..15]-(v))
with n, count(nodes(p)) as distance
set n.vip_distance = distance
with n
match (n)
 with distinct n.vip_distance as vip_distance_key, count(*) as vip_distance_value
return vip_distance_key, vip_distance_value order by vip_distance_value


profile match (n:twitterUser) where n.friends_count is not null
match (v:service{type:"VIP"}) with n, v
optional match p=(n)-[:follows]->(m:twitterUser)
with n, n.friends_count - count(nodes(p)) as remaining
set n.import_friends_remaining = remaining
with n
match (n)
 with distinct n.import_friends_remaining as import_friends_remaining_key, count(*) as import_friends_remaining_count
return import_friends_remaining_key, import_friends_remaining_count order by import_friends_remaining_key
