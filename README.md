# lovefield-ts
[![Build Status](https://travis-ci.org/arthurhsu/lovefield-ts.svg?branch=master)](
https://travis-ci.org/arthurhsu/lovefield-ts)

Lovefield Typescript port and modernization.

The port attempts to maintain API compatibility with original Lovefield. As a
result, some parts may conflict with TypeScript best practice (e.g. interface
name must start with capital I).

## Expectations

### Supported

* Most of original Lovefield features (except Firebase and static schema).
* NEW: NodeJS support: NodeJS 10+ will be supported (with memory store only).

### Unsupported

* Legacy browsers and technologies. Please assume ES6 throughout.
  * As of Apr 2020, Chrome 60+, Firefox 60+, Safari 10+, Edge are supported.
  * Currently only continuously tested on latest Chrome given resource shortage.
* Firebase is no longer supported.
  * This project is not sponsored by Google and the developers do not have
    unlimited access for this project.
  * Firebase API changed and legacy Lovefield code cannot be used.
* Static schema: it was designed for use with Closure compiler. Since the tool
  chain has moved to TypeScript, it makes no sense to support it.

### Dist changes

* Lovefield-ts no longer ships minified JavaScript file. Instead, it provides
  * A concatenated TypeScript file that you can directly include in your
    TypeScript project.
  * A compiled ES6 JavaScript file with source map and TypeScript declarations.
    Just import it and use your existing packing/minifying config.
* Lovefield-ts no longer uses flags to do compile-time control. Instead, a
  runtime options object will be used. The interface is defined in
  `lib/base/lovefield_options.ts`. Users are supposed to define an object
  following that interface and set options via the new API `lf.options.set()`.
  * By default, an options object not providing error message explanations is
    provided for better minify performance. If you wish to include detailed
    error message in your package, use or copy `testing/debug_options.ts`.

### Test changes

* API tester and performance benchmarks are not implemented yet. They will be
  implemented in JavaScript but remove dependencies on Closure Libraries
  completely.

## Building and development instructions

The project is set to use modern Typescript (3.8+) and Mocha/Chai/Sinon/Karma as
its test framework. Compilation/test speed has improved significantly.

### Development set up

* Install Chrome
* Install Node 12+
* `npm install`
* `node node_modules/guppy-cli/bin/index.js pre-commit`

Lovefield-ts uses gulp 4, which is incompatible with gulp 3 that original
Lovefield uses. If you had installed gulp globally as suggested in README of
Lovefield, please run:

```
npm uninstall -g gulp
```

### Development flow

Run `gulp` to see the commands.

Please note that certain tests are only runnable in Karma (e.g. IndexedDB
related tests), and these tests will be named *_spec.ts.

### Directory structures

* `lib`: Lovefield main library source code
* `testing`: Facility code used for testing
* `tests`: Tests for Lovefield main library
* `out`: Temporary directory used to store intermediate files from tool chain
* `dist`: Generated dist files
* `coverage`: Code coverage report
