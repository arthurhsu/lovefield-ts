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
const { exec } = require('child_process');
const fs = require('fs-extra');
const glob = require('glob');
const gulp = require('gulp');
const debug = require('gulp-debug');
const guppy = require('git-guppy')(gulp);
const mocha = require('gulp-mocha');
const sourcemaps = require('gulp-sourcemaps');
const tsc = require('gulp-typescript');
const yaml = require('js-yaml');
const karma = require('karma');
const nopt = require('nopt');
const path = require('path');
const Toposort = require('toposort-class');

const DIST_DIR = 'dist';
const DIST_FILE = path.join(DIST_DIR, 'lf.ts');
const SRC_PATTERN = ['**/*.ts', `${DIST_DIR}/**`];

let tsProject;
let deps;

function getProject() {
  if (!tsProject) {
    tsProject = tsc.createProject('tsconfig.json');
  }
  return tsProject;
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
        let value = line.substring(index + 1).trim();
        flags.Flags[key] = value;
      }
    });
  }

  let contents = 'export class Flags {\n';
  for (let key in flags.Flags) {
    let value = flags.Flags[key];
    let quote = '\'';
    if (typeof(value) == 'boolean') {
      quote = '';
    }
    // We do not use readonly so that tests can modify them, esp. DEBUG.
    contents += `  static ${key} = ${quote}${value}${quote};\n`;
  }
  contents += '}\n';
  fs.ensureDirSync('lib/gen');
  fs.writeFileSync('lib/gen/flags.ts', contents, {encoding: 'utf-8'});
  gulp.src('lib/gen/flags.ts')
      .pipe(gulp.dest('lib/gen'));
}

gulp.task('default', (cb) => {
  let log = console.log;
  log('gulp tasks:');
  log('  build: build all libraries and tests');
  log('  clean: remove all intermediate files');
  log('  check: run `npx gts check` for lints and errors');
  log('  fix: run `npx gts fix` to automatically fix errors');
  log('  test: run mocha tests (quick mode only)');
  log('options:');
  log('  --quick, -q: Quick test only');
  log('  --grep, -g: Mocha grep pattern');
  log('  --flag <KEY:VALUE>: Override flags');
  cb();
});

gulp.task('clean', (cb) => {
  fs.removeSync(getProject().options.outDir);
  fs.removeSync('coverage');
  fs.removeSync('lib/gen');
  cb();
});

gulp.task('buildLib', gulp.series('clean', function actualBuildLib() {
  genFlags();
  getProject();
  return gulp.src(['lib/**/*.ts'])
      .pipe(sourcemaps.init())
      .pipe(tsProject())
      .pipe(sourcemaps.write('.'))
      .pipe(gulp.dest(path.join(tsProject.options.outDir, 'lib')));
}));

gulp.task('buildTesting', () => {
  getProject();
  return gulp.src(['testing/**/*.ts'])
      .pipe(sourcemaps.init())
      .pipe(tsProject())
      .pipe(sourcemaps.write('.'))
      .pipe(gulp.dest(path.join(tsProject.options.outDir, 'testing')));
});

gulp.task('buildTests', () => {
  getProject();
  return gulp.src(['tests/**/*.ts'])
      .pipe(sourcemaps.init())
      .pipe(tsProject())
      .pipe(sourcemaps.write('.'))
      .pipe(gulp.dest(path.join(tsProject.options.outDir, 'tests')));
});

gulp.task('buildTest', gulp.series(['buildTesting', 'buildTests']));
gulp.task('build', gulp.series(['buildLib', 'buildTest']));

gulp.task('deps', (cb) => {
  glob('lib/**/*.ts', (err, matches) => {
    let files = ['lib/gen/flags.ts'].concat(matches);
    let fileSet = new Set(files);
    const relativePath = (p) => {
      return path.relative(__dirname, p).replace(/\\/g, '/');
    };

    let topoGraph = [];
    fileSet.forEach(f => {
      const absDir = path.dirname(path.resolve(f));
      let parsing = false;
      const imports = fs.readFileSync(f, 'utf-8').split('\n')
          .map(l => {
            if (l.startsWith('import')) {
              parsing = true;
            }
            if (parsing) {
              const index = l.indexOf('} from \'');
              if (index != -1) {
                let child = l.substring(index + 8);
                child = child.substring(0, child.length - 2) + '.ts';
                child = relativePath(path.resolve(absDir, child));
                topoGraph.push([f, child]);
                parsing = false;
              }
            }
         });
    });

    let resolved = false;
    while (!resolved) {
      try {
        const t = new Toposort();
        topoGraph.forEach(pair => {
          t.add(pair[0], pair[1]);
        });
        const res = t.sort();
        resolved = true;
        deps = res.reverse();
      } catch(e) {
        const chain = e.message.split('\n')[1];
        // For debug use to check circular dependency.
        console.log(`WARNING: ${chain}`);
        const tokens = chain.split(' ');
        const key = tokens[tokens.length - 3];
        const value = tokens[tokens.length - 1];
        let index = -1;
        topoGraph.forEach((pair, i) => {
          if (pair[0] == key && pair[1] == value) {
            index = i;
          }
        });
        topoGraph.splice(index, 1);
      }
    }

    cb();
  });
});

function compile() {
  let compilerOptions = Object.assign(tsProject.config.compilerOptions, {});

  const tsp = tsc.createProject(compilerOptions);
  gulp.src('dist/lf.ts')
      .pipe(tsp())
      .pipe(gulp.dest('out/dist'));
}

gulp.task('genDist', gulp.series(['buildLib', 'deps'], function actualDist(cb) {
  let copyRight = false;  // only need to include copy right header once.
  // Erase file first.
  fs.ensureDirSync(DIST_DIR);
  let finalResult = [];
  deps.forEach(file => {
    contents = fs.readFileSync(file, 'utf-8').split('\n');
    if (!copyRight) {
      copyRight = true;
    } else if (file != 'lib/gen/flags.ts') {
      contents.splice(0, 15);
    }

    let exp = false;
    let multiLineImports = false;
    contents.forEach(line => {
      if (line.startsWith('// eslint:')) {
        // no-op
      } else if (line.startsWith('import ')) {
        if (line.indexOf(';') == -1) {
          multiLineImports = true
        }
      } else if (multiLineImports) {
        if (line.indexOf(';') != -1) {
          multiLineImports = false;
        }
      } else if (line == '// @export') {
        exp = true;
      } else if (line.startsWith('export ')) {
        if (exp) {
          exp = false;
          finalResult.push(line);
        } else {
          finalResult.push(line.substring(7));
        }
      } else {
        finalResult.push(line);
      }
    });
  });
  fs.writeFileSync(DIST_FILE, [
    '/* eslint-disable */',
  ].join('\n'), 'utf-8');
  fs.appendFileSync(DIST_FILE, finalResult.join('\n') + '\n\n', 'utf-8');
  compile();
  cb();
}));

gulp.task('dist', gulp.series(['genDist'], function actualBuildDist() {
  getProject();
  return gulp.src([DIST_FILE])
      .pipe(sourcemaps.init())
      .pipe(tsProject())
      .pipe(sourcemaps.write('.'))
      .pipe(gulp.dest(DIST_DIR));
}));

function quickTest() {
  let mochaOptions = {
    reporter: getGrepPattern() ? 'spec' : 'dot',
    require: ['source-map-support/register'],
    grep: getGrepPattern()
  };

  return gulp
      .src(['out/tests/**/*.js', '!out/tests/**/*_spec.js'], {read: false})
      .pipe(mocha(mochaOptions));
}

gulp.task('test', gulp.series(['dist', 'buildTest'], function actualTest(cb) {
  if (!fs.existsSync(getProject().options.outDir)) {
    cb('Compile Error!');
    return;
  }

  if (isQuickMode()) {
    return quickTest();
  } else {
    let server = new karma.Server({
      configFile: path.join(__dirname, 'karma_config.js'),
      singleRun: true,
      client: { mocha: { grep: getGrepPattern() } }
    });

    server.on('run_complete', () => {
      karma.stopper.stop();
      cb();
    });
    server.start();
  }
}));

function errorInSauceBrowserResults(ret) {
  let result = false;
  ret.browsers.forEach(browser => {
    let res = browser.lastResult;
    console.log(`${browser.name}: ${res.total} [${res.success}/${res.failed}]`);
    result = (res.failed > 0) ? true : result;
  });
  return result;
}

gulp.task('ci', gulp.series(['dist', 'buildTest'], function actualCI(cb) {
  if (!fs.existsSync(getProject().options.outDir)) {
    cb('Compile Error!');
    return;
  }

  quickTest();
  const server = new karma.Server({
    configFile: path.join(__dirname, 'karma_config_ci.js')
  });
  server.on('run_complete', (ret) => {
    karma.stopper.stop();
    if (errorInSauceBrowserResults(ret)) {
      console.log('===== TEST FAILED =====');
      process.exit(1);
    }
    cb();
  });
  server.start();
}));

gulp.task('debug', gulp.series(['dist', 'buildTest'], function actualDebug(cb) {
  if (!fs.existsSync(getProject().options.outDir)) {
    cb('Compile Error!');
    return;
  }

  const server = new karma.Server({
      configFile: path.join(__dirname, 'karma_config.js'),
      singleRun: false,
      client: { mocha: { grep: getGrepPattern() } }
  });
  server.on('run_complete', (ret) => {
    karma.stopper.stop();
    cb();
  });
  server.start();
}));

gulp.task('check', function gtsCheck(cb) {
  const cmd = exec('npx gts check');
  cmd.stdout.on('data', data => { console.log(data); });
  return cmd;
});

gulp.task('fix', function gtsFix(cb) {
  const cmd = exec('npx gts fix');
  cmd.stdout.on('data', data => { console.log(data); });
  return cmd;
});

gulp.task('pre-commit', gulp.parallel(['build', 'check'],
    function preCommitCheck(cb) {
      cb();
    }));
