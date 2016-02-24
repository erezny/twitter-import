
var util = require('util');
const crow = require("crow-metrics");
const request = require("request");

crow.exportInflux(metrics, request, { url: util.format("%s://%s:%s@%s:%d/write?db=%s",
process.env.INFLUX_PROTOCOL, process.env.INFLUX_USERNAME, process.env.INFLUX_PASSWORD,
process.env.INFLUX_HOST, parseInt(process.env.INFLUX_PORT), process.env.INFLUX_DATABASE)
});

module.export = metrics = new crow.MetricsRegistry({ period: 5000, separator: "." });
