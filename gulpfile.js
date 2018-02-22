/**
 * @license
 * Copyright 2016 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const chalk = require('chalk');
const diff = require('diff');
const fs = require('fs-extra');
const gulp = require('gulp');
const mocha = require('gulp-mocha');
const format = require('gulp-clang-format');
const sourcemaps = require('gulp-sourcemaps');
const tslint = require('gulp-tslint');
const tsc = require('gulp-typescript');
const karma = require('karma');
const nopt = require('nopt');
const path = require('path');
const thru = require('through2');

let tsProject;

function getProject() {
  if (!tsProject) {
    tsProject = tsc.createProject('tsconfig.json');
  }
  return tsProject;
}

function prettyPrint(patch) {
  if (patch.hunks.length) {
    console.log(chalk.yellow('===== ' + patch.oldFileName));
    patch.hunks.forEach((hunk) => {
      let numberOld = hunk.oldStart;
      let numberNew = hunk.newStart;
      hunk.lines.forEach((line) => {
        if (line[0] == '-') {
          console.log(chalk.bgRed(numberOld + ' ' + line));
          numberOld++;
        } else if (line[0] == '+') {
          console.log(chalk.bgGreen(numberNew + ' ' + line));
          numberNew++;
        } else {
          console.log(numberOld + ' ' + line);
          numberOld++;
          numberNew++;
        }
      });
    });
    console.log();
  }
}

function checkFormat() {
  let stream = thru.obj(function(file, enc, done) {
    if (file.isBuffer()) {
      let original = fs.readFileSync(file.path, 'utf8');
      let formatted = file.contents.toString();
      let patch = diff.structuredPatch(file.path, null, original, formatted);
      prettyPrint(patch);
    } else {
      console.error('Not supported');
      process.exit(1);
    }

    // Make sure the file goes through the next gulp plugin.
    this.push(file);
    done();
  });
  return stream;
}

function getGrepPattern() {
  let knownOpts = { 'grep': String };
  let opts = nopt(knownOpts, null, process.argv, 2);
  return opts.grep ? opts.grep : undefined;
}

gulp.task('default', () => {
  let log = console.log;
  log('Usage:');
  log('gulp build: build all libraries and tests');
  log('gulp clean: remove all intermediate files');
  log('gulp test: run mocha tests');
  log('gulp format: format files using clang-format');
  log('gulp check: lint and format check files');
});

gulp.task('build', () => {
  return getProject()
      .src()
      .pipe(sourcemaps.init())
      .pipe(tsProject())
      .pipe(sourcemaps.write('.'))
      .pipe(gulp.dest(tsProject.options.outDir));
});

gulp.task('lint', () => {
  return getProject()
      .src()
      .pipe(tslint({formatter: 'stylish'}))
      .pipe(tslint.report({
        summarizeFailureOutput: true
      }));
});

gulp.task('test', () => {
  let server = new karma.Server({
    configFile: path.join(__dirname, 'karma_config.js'),
    singleRun: true
  });
  
  server.on('run_complete', () => {
    karma.stopper.stop();
  });
  server.start();
});

gulp.task('qtest', ['build'], () => {
  let mochaOptions = {
    reporter: 'spec',
    require: ['source-map-support/register'],
    grep: getGrepPattern()
  };

  gulp.src('out/tests/**/*.js', {read: false})
      .pipe(mocha(mochaOptions));
});

gulp.task('format', () => {
  return getProject()
      .src()
      .pipe(format.format())
      .pipe(gulp.desc('.'));
});

gulp.task('check', ['lint'], () => {
  return getProject()
      .src()
      .pipe(format.format())
      .pipe(checkFormat());
});

gulp.task('clean', () => {
  fs.removeSync(getProject().options.outDir);
  fs.removeSync('coverage');
});