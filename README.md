Twitter-Import
==============

[Twitter-Import](https://www.github.com/erezny/twitter-import) is an open source engine to sync users, lists, and tweets from Twitter to MongoDb and Neo4j.

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

Dockerfile
----------

Required variables:

  - TWITTER_CONSUMER_KEY
  - TWITTER_CONSUMER_SECRET
  - TWITTER_ACCESS_TOKEN
  - TWITTER_ACCESS_TOKEN_SECRET
  - MONGO_ENV_TUTUM_SERVICE_HOSTNAME
  - MONGO_ENV_PORT_27017_TCP_PORT
  - MONGO_COLLECTION=twitter
  - MONGO_USER
  - MONGO_PASSWD
