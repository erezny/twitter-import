match (v:service{type:"VIP"})
match (n:twitterUser{screen_name:"SamHarrisOrg"})
merge (n)<-[r:includes]-(v) return v, n

then update stats

match (v:service{type:"VIP"})--(n:twitterUser) with n
create unique (d:analytics{type:"vipDistance"})-[:vip]-(n) with d, n
match (n)-[:follows]->(f:twitterUser) with d,n,f
create unique (f)-[r:distance ]->(d)
set r.value = 1
with n, f
return n, count(r)

profile match (v:service{type:"VIP"})--(n:twitterUser) with n
create unique (d:analytics{type:"vipDistance"})-[:vip]-(n) with d, n
match (n)-[:follows*2]->(f:twitterUser) with d,n,f where not exists((f)-[:distance]->(d)) limit 1000
merge(f)-[r:distance]->(d)
on create set r.value = 2
with n, f
return n, count(f)

profile match (v:service{type:"VIP"})--(n:twitterUser) with n
merge (d:analytics{type:"vipDistance"})-[:vip]-(n) with d, n
match (n)-[:follows*2]->(f:twitterUser)
with d,n,f limit 10000
merge (f)-[r:distance]->(d)
on create set r.value = 2
with n, f
return n, count(f)
