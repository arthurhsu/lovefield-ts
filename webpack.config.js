const path = require('path');

module.exports = {
  mode: 'development',
  entry: './tests/all_tests.ts',
  devtool: 'source-map',
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.ts', '.js'],
  },
  externals: {
    chai: 'chai',
  },
  output: {
    filename: 'test_bundle.js',
    path: path.resolve(__dirname, 'tests/harness'),
  },
};
