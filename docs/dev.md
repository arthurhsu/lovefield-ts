# Building and development instructions

The project is set to use modern Typescript (3.8+) and Mocha/Chai/Sinon/Karma as
its test framework. Compilation/test speed has improved significantly (we are
talking about less than 10 minutes full CI time compared to original hour+ long
process).

## Development set up

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

## Development flow

Run `gulp` to see the commands.

Please note that certain tests are only runnable in Karma (e.g. IndexedDB
related tests), and these tests will be named *_spec.ts.
