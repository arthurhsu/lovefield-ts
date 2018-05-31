// Karma configuration
// Generated on Sat Oct 22 2016 17:35:31 GMT-0700 (Pacific Daylight Time)

module.exports = function(config) {
  config.set({
    frameworks: ['mocha', 'karma-typescript'],
    singleRun: true,

    files: [
      'node_modules/chai/chai.js',
      '**/*.ts'
    ],

    exclude: [
      'node_modules/**/*.ts'
    ],

    preprocessors: {
      '**/*.ts': ['karma-typescript']
    },

    reporters: ['dots', 'saucelabs', 'karma-typescript'],
    port: 9876,
    colors: true,

    browsers: Object.keys(customLaunchers),

    customLaunchers: {
      sl_chrome_linux: {
        base: 'SauceLabs',
        browserName: 'chrome',
        platform: 'linux'
      }
    },

    karmaTypescriptConfig: {
      tsconfig: 'tsconfig.json',
      exclude: [
        'node_modules'
      ]
    },

    sauceLabs: {
      testName: 'Lovefield TypeScript Port',
      recordScreenshots: false,
      connectOptions: {
        port: 5757,
        logfile: 'sauce_connect.log'
      },
      public: 'public'
    },

    // Increase timeout in case connection in CI is slow
    captureTimeout: 120000
  })
}  // end of module
