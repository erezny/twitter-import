
var util = require('util');
const crow = require("crow-metrics");
const Request = require("request");
var SocksProxyAgent = require('socks-proxy-agent');
var http = require('http');

const influxServer = {
  host: process.env.INFLUX_HOST || '127.0.0.1',
  protocol: process.env.INFLUX_PROTOCOL || 'http',
  port: process.env.INFLUX_PORT || '8086',
  database: process.env.INFLUX_DATABASE || 'socialGraph',
  username: process.env.INFLUX_USERNAME || 'socialGraph',
  password: process.env.INFLUX_PASSWORD || '',
};
const influxConnectString = util.format("%s://%s:%s@%s:%d/write?db=%s",
  process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
  process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE);

module.exports = {
      init: function(prefix, tags, logger) {//Decide: change period for dev, test, prod
        var request, socksProxy;
        const proxy = process.env.SOCKS_PROXY || "";
        if (proxy.match("socks:")){
          socksProxy =  new SocksProxyAgent(proxy);
          request = function(options, callback) {
            options.agent = socksProxy;
            return Request(options, callback);
          };
        } else {
          request = Request;
        }

        var metrics = new crow.MetricsRegistry({ period: 5000, separator: ".", tags: tags || {} }).withPrefix(prefix);
        crow.exportInflux(metrics, request, { url: influxConnectString,
        log: logger
        });

        metrics.setGauge("heap_used", function () { return process.memoryUsage().heapUsed; });
        metrics.setGauge("heap_total", function () { return process.memoryUsage().heapTotal; });
        metrics.counter("app_started").increment();

        metrics.RelSaved = metrics.counter("rel_saved");
        metrics.RelError = metrics.counter("rel_error");
        metrics.Start = metrics.counter("start");
        metrics.FreshQuery = metrics.counter("freshQuery");
        metrics.ContinuedQuery = metrics.counter("continuedQuery");
        metrics.Finish = metrics.counter("finish");
        metrics.QueryError = metrics.counter("queryError");
        metrics.RepeatQuery = metrics.counter("repeatQuery");
        metrics.UpdatedTimestamp = metrics.counter("updatedTimestamp");
        metrics.ApiError = metrics.counter("apiError");
        metrics.ApiFinished = metrics.counter("apiFinished");
        metrics.TxnFinished = metrics.counter("txnFinished");
        metrics.TxnError = metrics.counter("txnError");

        return metrics;
      }
    }
