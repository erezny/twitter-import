
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

// kue.Job.rangeByState( 'inactive', 0, 100, 'asc', function( err, jobs ) {
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
// kue.Job.rangeByState( 'completed', 0, 100000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });
//
// kue.Job.rangeByState( 'failed', 0, 100000, 'asc', function( err, jobs ) {
//   jobs.forEach( function( job ) {
//     job.remove( function() {
//       console.log( 'removed ', job.id );
//     });
//   });
// });

kue.Job.rangeByType( 'queryFriendsList', 'inactive', 0, 100, 'asc', function( err, jobs ) {
  jobs.forEach( function( job ) {
    job.remove( function() {
      console.log( 'removed ', job.id );
    });
  });
});

kue.Job.rangeByType( 'queryFriendsList', 'active', 0, 100, 'asc', function( err, jobs ) {
  jobs.forEach( function( job ) {
    job.remove( function() {
      console.log( 'removed ', job.id );
    });
  });
});
kue.Job.rangeByType( 'queryFriendsList', 'completed', 0, 100, 'asc', function( err, jobs ) {
  jobs.forEach( function( job ) {
    job.remove( function() {
      console.log( 'removed ', job.id );
    });
  });
});

kue.Job.rangeByType( 'queryFriendsList', 'failed', 0, 100, 'asc', function( err, jobs ) {
  jobs.forEach( function( job ) {
    job.remove( function() {
      console.log( 'removed ', job.id );
    });
  });
});

kue.Job.rangeByType( 'queryFriendsList', 'delayed', 0, 100, 'asc', function( err, jobs ) {
  jobs.forEach( function( job ) {
    job.remove( function() {
      console.log( 'removed ', job.id );
    });
  });
});
