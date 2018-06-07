/**
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
const debug = require('gulp-debug');
const guppy = require('git-guppy')(gulp);
const mocha = require('gulp-mocha');
const format = require('gulp-clang-format');
const sourcemaps = require('gulp-sourcemaps');
const tslint = require('gulp-tslint');
const tsc = require('gulp-typescript');
const yaml = require('js-yaml');
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

function isQuickMode() {
  let knownOpts = { 'quick': Boolean };
  let opts = nopt(knownOpts, null, process.argv, 2);
  return opts.quick || false;
}

function getGrepPattern() {
  let knownOpts = { 'grep': String };
  let opts = nopt(knownOpts, null, process.argv, 2);
  return opts.grep;
}

function genFlags() {
  let flags = yaml.safeLoad(fs.readFileSync('flags.yaml', 'utf8'));

  let knownOpts = { 'flag': Array };
  let opts = nopt(knownOpts, null, process.argv, 2);
  if (opts.flag) {
    opts.flag.forEach(line => {
      let index = line.indexOf(':');
      if (index != -1) {
        let key = line.substring(0, index);
        let value = line.substring(index + 1);
        if (value == 'true') {
          value = true;
        } else if (value == 'false') {
          value = false;
        }
        flags.Flags[key] = value;
      }
    });
  }

  let contents = 'export class Flags {\n';
  for (let key in flags.Flags) {
    let value = flags.Flags[key];
    let quote = '\'';
    if (value == 'true' || value == 'false') {
      quote = '';
    }
    // We do not use readonly so that tests can modify them, esp. DEBUG.
    contents += `  public static ${key} = ${quote}${value}${quote};\n`;
  }
  contents += '}  // class Flags\n';
  fs.ensureDirSync('lib/gen');
  fs.writeFileSync('lib/gen/flags.ts', contents, {encoding: 'utf-8'});
  gulp.src('lib/gen/flags.ts')
      .pipe(format.format())
      .pipe(gulp.dest('lib/gen'));
}

gulp.task('default', () => {
  let log = console.log;
  log('gulp tasks:');
  log('  build: build all libraries and tests');
  log('  clean: remove all intermediate files');
  log('  test: run mocha tests (quick mode only)');
  log('  format: format files using clang-format');
  log('  check: lint and format check files');
  log('options:');
  log('  --quick, -q: Quick test only');
  log('  --grep, -g: Mocha grep pattern');
  log('  --flag <KEY:VALUE>: Override flags');
});

gulp.task('build', () => {
  genFlags();
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

gulp.task('test', ['build'], () => {
  if (isQuickMode()) {
    let mochaOptions = {
      reporter: 'spec',
      require: ['source-map-support/register'],
      grep: getGrepPattern()
    };

    gulp.src('out/tests/**/*.js', {read: false})
        .pipe(mocha(mochaOptions));
  } else {
    let server = new karma.Server({
      configFile: path.join(__dirname, 'karma_config.js'),
      singleRun: true,
      mocha: { grep: getGrepPattern() }
    });

    server.on('run_complete', () => {
      karma.stopper.stop();
    });
    server.start();
  }
});

gulp.task('ci', ['build'], () => {
  const server = new karma.Server({
    configFile: path.join(__dirname, 'karma_config_ci.js')
  });
  server.on('run_complete', (ret) => {
    console.log(`karma run complete, exitCode: ${JSON.stringify(ret)}`);
    karma.stopper.stop();
  });
  server.start();
});

gulp.task('debug', ['build'], () => {
  new karma.Server({
      configFile: path.join(__dirname, 'karma_config.js'),
      singleRun: false,
      mocha: { grep: getGrepPattern() }
  }).start();
});

gulp.task('pre-commit', ['build', 'lint', 'fastcheck'], () => {
});

gulp.task('fastcheck', () => {
  return getProject()
      .src()
      .pipe(format.checkFormat('file'))
      .on('warning', e => {
        debug(e.message);
        process.exit(1);
      });
});

gulp.task('format', () => {
  return getProject()
      .src()
      .pipe(format.format('file'))
      .pipe(gulp.dest('.'));
});

gulp.task('check', ['lint'], () => {
  return getProject()
      .src()
      .pipe(format.format('file'))
      .pipe(checkFormat());
});

gulp.task('clean', () => {
  fs.removeSync(getProject().options.outDir);
  fs.removeSync('coverage');
  fs.removeSync('lib/gen');
});
