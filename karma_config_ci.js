module.exports = function(config) {
  if (!process.env.SAUCE_USERNAME || !process.env.SAUCE_ACCESS_KEY) {
    console.error('Make sure the SAUCE_USERNAME and SAUCE_ACCESS_KEY environment variables are set.');
    process.exit(1);
  }

  var customLaunchers = {
    sl_chrome_linux: {
      base: 'SauceLabs',
      browserName: 'chrome',
      platform: 'linux'
    }
  };

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

    plugins: ['karma-sauce-launcher'],

    reporters: ['dots', 'saucelabs', 'karma-typescript'],
    port: 9876,
    colors: true,

    browsers: Object.keys(customLaunchers),

    karmaTypescriptConfig: {
      tsconfig: 'tsconfig.json',
      exclude: [
        'node_modules'
      ]
    },

    concurrency: 2,

    sauceLabs: {
      testName: 'Lovefield TypeScript Port',
      tunnelIdentifier: process.env.TRAVIS_JOB_NUMBER,
      username: process.env.SAUCE_USERNAME,
      accessKey: process.env.SAUCE_ACCESS_KEY,
      startConnect: process.env.TRAVIS !== 'true',
      recordScreenshots: false,
      connectOptions: {
        port: 5757,
        logfile: 'sauce_connect.log'
      },
      public: 'public'
    },

    captureTimeout: 0,
    customLaunchers: customLaunchers
  })
}  // end of module
