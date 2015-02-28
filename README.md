Twitter-Import
==============

**[Twitter-Import](https://www.github.com/erezny/twitter-import) is an open source engine to sync users, lists, and tweets from Twitter to MongoDb and Neo4j.

Setup
-----

`git clone https://github.com/erezny/twitter-import.git`
`npm install`
`nano config/env/dev.js`
`node index.js <screen_name>`

Remarks
-------

This program traverses the twitter social graph centered around 1 or more users. 

Integrates with:
----------------

  - user
  - user/followers{list|ids}
  - user/friends/{list|ids}
  - (planned) user/lists/ownership
  - (planned) user/lists/subscribed
