var gulp = require('gulp');
var jscs = require('gulp-jscs');
var istanbul = require('gulp-istanbul');
// We'll use mocha here, but any test framework will work
var mocha = require('gulp-mocha');

var source = ['lib/**/*.js', 'lib/*.js', 'index.js'];

gulp.task('test', function (cb) {
  gulp.src(source)
    .pipe(istanbul()) // Covering files
    .pipe(istanbul.hookRequire()) // Force `require` to return covered files
    .on('finish', function () {
      gulp.src(['test/*.js'])
        .pipe(mocha())
        .pipe(istanbul.writeReports()) // Creating the reports after tests runned
        .on('end', cb);
    });
});

gulp.task('style', function () {
    return gulp.src(['lib/**/*.js', 'lib/*.js', 'index.js'])
        .pipe(jscs());
});
