var jscomplexity = require('jscomplexity');
var RSVP = require('rsvp');
var Table = require('cli-table');

function reportAggregator() {
  var combinedReport = [];

  function pushReport(report) {
    Array.prototype.push.apply(combinedReport, report.report);
    return;
  };

  function get() {
    return { report: combinedReport };
  };

  function tableArray() {
    return combinedReport.sort(function(a,b) {
      return a.path > b.path;
    }).map(function(item) {
      return [
        item.path,
        item.complexity,
        item.lineNumber,
        item.maintainability,
      ];
    })
  }

  return  {
    pushReport: pushReport,
      get: get,
      tableArray: tableArray
    };
}

var report = reportAggregator();

var complexityTasks = [
  jscomplexity( 'lib/**/**.js' )
    .then( report.pushReport ),
  jscomplexity( 'services/**/**.js' )
    .then( report.pushReport ),
  jscomplexity( 'scripts/**/**.js' )
    .then( report.pushReport ),
];

// instantiate
var table = new Table({
  head: [ 'Path', 'Complexity', 'Lines', 'Maintainability' ],
  colWidths: [ 40, 12, 10, 17 ]
});

RSVP.all(complexityTasks).then(function() {
  Array.prototype.push.apply(table, report.tableArray());
  console.log(table.toString());
  consol
  // posts contains an array of results for the given promises
}).catch( function( reason ) {
  // if any of the promises fails.
});
