# Lovefield Performance Benchmark

TL;DR: Results comparison done using Windows 10 and Chrome 81 can be found
in the [benchmark PDF](benchmark_results.pdf). The results suggested that
TypeScript port has comparable or better performance than original Lovefield,
except select operations requiring full table scan.

## About the code

This is a rewrite of Lovefield benchmark, and the original code can be found at

https://github.com/google/lovefield/tree/master/testing/perf
https://github.com/google/lovefield/tree/master/perf

This rewrite does not need Closure compiler. It can be run without modifications
for lovefield-ts, but you'll need to jump a few hoops to run it using the
original Lovefield.

## Run with lovefield-ts

```
npm i
npm run server
```
Then browse http://localhost:8080 using the browser you wanted to test on.

## Run with lovefield

1. Change the package.json to point to lovefield instead of lovefield-ts
2. Original Lovefield does not support module, so you'll need to change
   the index.html's body as the following:

```html
<script type="text/javascript" src="./node_modules/lovefield/dist/lovefield.js"></script>
<script type="text/javascript">
  lf.DataStoreType = lf.schema.DataStoreType;
  window.top['#lfRowId'] = lf.Row.getNextId;
</script>
<script type="module" src="perf.js"></script>
```

3. Fix `lovefield.js`: you need to export `lf.Row.getNextId` and add the
   missing `lf.Row.prototype.payload()` around line 3919:

   Find this code block first:
   ```javascript
   lf.Row.getNextId = function() {
     return lf.Row.nextId_++;
   };
   ```

   Add this code block below it:
   ```javascript
   goog.exportSymbol("lf.Row.getNextId", lf.Row.getNextId);
   lf.Row.prototype.payload = function() {
     return this.payload_;
   };
   ```

Then do the same `npm run server` and browse to `http://localhost:8080` using
the browser you wanted to test.
