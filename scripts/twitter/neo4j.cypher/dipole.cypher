match (n:twitterUser)
where n.scren_name in ["erezny", "Tuggernuts23"]
match (n)-[:follows]->(n1)-[:follows]->(n2)
with n, count (n1) as friends, count (n2) as friends2

match (n:twitterUser{screen_name:"erezny"}), (m:twitterUser{screen_name:"Tuggernuts23"})
match (n)-[:follows]->(n11)<-[:follows]-(m)
return n,m, collect(n11) as c11

match (n:twitterUser{screen_name:"erezny"}), (m:twitterUser{screen_name:"Tuggernuts23"})
match (n)-[:follows]->(n11)<-[:follows]-(m), (n)-[:follows]->(n12)<-[:follows*2]-(m), (n)-[:follows*2]->(n21)<-[:follows]-(m)
return n,m, count(n11) as c11, count(n12) as c12, count(n21) as c21

match (n:twitterUser{screen_name:"erezny"}), (m:twitterUser{screen_name:"Tuggernuts23"})
match (n)-[:follows]->(n11)<-[:follows]-(m),
(n)-[:follows]->(n12)<-[:follows*2]-(m),
(n)-[:follows*2]->(n21)<-[:follows]-(m)
(n)-[:follows*2]->(n22)<-[:follows*2]-(m),
(n)-[:follows]->(n13)<-[:follows*3]-(m),
(n)-[:follows*3]->(n31)<-[:follows]-(m),
return n,m,
count(n11) as c11,
count(n12) as c12,
count(n21) as c21,
count(n22) as c22,
count(n13) as c13,
count(n31) as c31

match (n:twitterUser{screen_name:"erezny"}), (m:twitterUser{screen_name:"Tuggernuts23"})
match (n)-[:follows*1..2]->(l)<-[:follows*1..2]-(m)
return n,m, count(l)

match (n:twitterUser{screen_name:"erezny"}), (m:twitterUser{screen_name:"Tuggernuts23"})
match (n)-[rn:follows*1..2]->(l)<-[rm:follows*1..2]-(m)
with l, [length(rn), length(rm)] as distance
return distinct distance, count(l)
