'use strict';
var util = require('util');
var SocksProxyAgent = require('socks-proxy-agent');
var http = require('http');
const TwitterNeo4j = require('./twitter/controller/neo4j.js');

function Neo4j(logger, metrics) {
  const proxy = process.env.SOCKS_PROXY || "";

  if (proxy.match("socks:")){
    this.agent = new SocksProxyAgent(proxy);
    console.log("socks %s", proxy);
  } else {
    const agent_opts = { keepAlive: true };
    this.agent = new http.Agent(agent_opts);
  }

  this.neo4j = require('seraph')( {
    server: util.format("%s://%s:%s",
    process.env.NEO4J_PROTOCOL,
    process.env.NEO4J_HOST,
    process.env.NEO4J_PORT),
    endpoint: 'db/data',
    agent: this.agent,
    user: process.env.NEO4J_USERNAME,
    pass: process.env.NEO4J_PASSWORD });

  this.twitter = new TwitterNeo4j(this.neo4j, logger, metrics);
}

module.exports = Neo4j;
