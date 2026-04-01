# Building and development instructions

The project is set to use modern Typescript (6.0+) and Mocha/Chai/Sinon as
its test framework. We use **Rollup** for library bundling and **Webpack** for
browser test bundling.

## Development set up

- Install Chrome
- Install Node 18+
- `npm install`

## Development flow

Use `npm run` to see available commands or refer to the list below:

### Build Tasks

- `npm run build`: Compiles the source code and tests using `tsc`, then runs `npm run format`. Output is in `out/`.
- `npm run build:dist`: Generates the library bundles in `dist/` (CJS, ESM, and UMD formats) using Rollup, then runs `npm run format`.
- `npm run clean`: Removes all intermediate files, build outputs, and coverage reports.

### Testing Tasks

- `npm run test`: Runs the full test suite in a headless Chrome browser via Selenium.
- `npm run test:node`: Runs the subset of tests that are compatible with Node.js via Mocha.
- `npm run test:prepare`: Generates the test bundle (`tests/harness/test_bundle.js`) required for browser testing.

### Code Quality

The project uses **ESLint** (Flat Config) and **Prettier** for code quality and formatting.

- `npm run lint`: Runs `eslint .` to verify code style and linting rules.
- `npm run fix`: Runs `eslint . --fix` to automatically resolve many linting issues.
- `npm run format`: Runs `prettier --write .` to format all files in the project.

## Testing Details

- **Node.js Tests**: These are unit tests that do not depend on browser-specific APIs (like IndexedDB or WebSQL).
- **Browser Tests**: Full end-to-end tests that run in a real browser environment. These are essential for verifying backstore implementations.
