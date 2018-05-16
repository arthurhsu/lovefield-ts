# lovefield-ts

Lovefield Typescript port and modernization.

## Building and development instructions

### Development set up

* Install Chrome
* Install Node 9+
* `npm install`
* `node node_modules/guppy-cli/bin/index.js pre-commit`

### Development flow

Run `gulp` to see the commands.

Please note that certain tests are only runnable in Karma (e.g. IndexedDB
related tests), and these tests will be named *_spec.ts.

We have no intention to support legacy browsers and technologies. Please
assume ES6 throughout.

### Directory structures

* `lib`: Lovefield main library source code
* `tests`: Tests for Lovefield main library
* `out`: Temporary directory used to store intermediate files from toolchain
* `coverage`: Code coverage report
