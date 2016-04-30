
var util = require('util');
const crow = require("crow-metrics");
const request = require("request");

module.exports = {
      init: function(prefix, tags) {//Decide: change period for dev, test, prod
        var metrics = new crow.MetricsRegistry({ period: 5000, separator: ".", tags: tags || {} }).withPrefix(prefix);
        crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
        process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
        process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
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
