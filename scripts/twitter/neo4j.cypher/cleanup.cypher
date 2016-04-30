
match (n:twitterUser)
where n.friends_count > 0 and n.friends_imported_count > n.friends_count
with n, n.friends_imported_count - n.friends_count as overage
return n
order by overage desc limit 10

start r=relationship(*)
match s-[r]->e
with s,e,type(r) as typ, tail(collect(r)) as coll
foreach(x in coll | delete x)

match (n:twitterUser)
where n.screen_name="erezny"
match (n)-[r]-(m)
with distinct type(r) as typ
return typ

match (n:twitterUser)
where n.screen_name="erezny"
match (n)-[r]-(m)
with distinct type(r) as typ
return typ
