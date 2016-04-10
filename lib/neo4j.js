var util = require('util');
var SocksProxyAgent = require('socks-proxy-agent');
var http = require('http');

// SOCKS proxy to connect to

const proxy = process.env.SOCKS_PROXY || "";
var agent;

if (proxy.match("socks:")){
  agent = new SocksProxyAgent(proxy);
  console.log("socks %s", proxy);
} else {
  const agent_opts = { keepAlive: true };
  agent = new http.Agent(agent_opts);
}

module.exports = neo4j = require('seraph')( {
  server: util.format("%s://%s:%s",
  process.env.NEO4J_PROTOCOL,
  process.env.NEO4J_HOST,
  process.env.NEO4J_PORT),
  endpoint: 'db/data',
  agent: agent,
  user: process.env.NEO4J_USERNAME,
  pass: process.env.NEO4J_PASSWORD });
