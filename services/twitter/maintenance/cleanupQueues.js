
var logger = require('tracer').colorConsole( {
  level: 'trace'
} );
var kue = require('kue');
var queue = kue.createQueue({
  prefix: 'twitter',
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    db: 1, // if provided select a non-default redis db
  }
});

setInterval(cleanup, 1 * 60 * 1000 );
function cleanup() {
  kue.Job.rangeByState( 'completed', 0, 10000, 'asc', function( err, jobs ) {
    jobs.forEach( function( job ) {
      job.remove( function() {
        console.log( 'removed ', job.id );
      });
    });
  });

  kue.Job.rangeByState( 'failed', 0, 10000, 'asc', function( err, jobs ) {
    jobs.forEach( function( job ) {
      job.remove( function() {
        console.log( 'removed ', job.id );
      });
    });
  });

}
cleanup();

// kue.Job.rangeByState( 'inactive', 0, 100000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
//
// kue.Job.rangeByState( 'active', 0, 100, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });

// kue.Job.rangeByType( 'queryUser', 'inactive', 0, 100000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
//
// kue.Job.rangeByType( 'queryUser', 'active', 0, 1000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
// kue.Job.rangeByType( 'queryUser', 'completed', 0, 1000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
//
// kue.Job.rangeByType( 'queryUser', 'failed', 0, 1000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
//
// kue.Job.rangeByType( 'queryUserListOqueryUserwnership', 'delayed', 0, 1000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
