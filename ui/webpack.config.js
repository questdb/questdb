/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

const path = require("path")
const { CleanWebpackPlugin } = require("clean-webpack-plugin")
const CopyWebpackPlugin = require("copy-webpack-plugin")
const ForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin")
const HtmlWebpackPlugin = require("html-webpack-plugin")
const MiniCssExtractPlugin = require("mini-css-extract-plugin")
const Webpack = require("webpack")
const AnalyzerPlugin = require("webpack-bundle-analyzer").BundleAnalyzerPlugin

require("dotenv").config()

const PORT = 9999
const BACKEND_PORT = 9000
const isProdBuild = process.env.NODE_ENV === "production"
const runBundleAnalyzer = process.env.ANALYZE
const ASSET_PATH = process.env.ASSET_PATH || "/"

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = "development"
}

const monacoPatterns = [
  {
    from: "node_modules/monaco-editor/min/vs/loader.js",
    to: "assets/vs/loader.js",
  },
  {
    from: "node_modules/monaco-editor/min/vs/editor/editor.main.js",
    to: "assets/vs/editor/editor.main.js",
  },
  {
    from: "node_modules/monaco-editor/min/vs/editor/editor.main.nls.js",
    to: "assets/vs/editor/editor.main.nls.js",
  },
  {
    from: "node_modules/monaco-editor/min/vs/editor/editor.main.css",
    to: "assets/vs/editor/editor.main.css",
  },
  { from: "node_modules/monaco-editor/min/vs/base", to: "assets/vs/base" },
]

const basePlugins = [
  new CleanWebpackPlugin(),
  new HtmlWebpackPlugin({
    template: "src/index.hbs",
    minify: {
      minifyCSS: false,
      minifyJS: false,
      minifyURLs: true,
      removeComments: true,
      removeRedundantAttributes: true,
      removeScriptTypeAttributes: true,
      removeStyleLinkTypeAttributes: true,
      useShortDoctype: true,
    },
  }),
  new MiniCssExtractPlugin({
    filename: "qdb.css",
  }),
  new Webpack.DefinePlugin({
    "process.env": {
      NODE_ENV: JSON.stringify(process.env.NODE_ENV),
    },
  }),
]

const devPlugins = [
  new ForkTsCheckerWebpackPlugin({
    eslint: {
      enabled: true,
      files: "./src/**/*.ts[x]",
    },
  }),
  new CopyWebpackPlugin({
    patterns: monacoPatterns,
  }),
]

const devLoaders = [
  {
    test: /\.(ts|js)x$/,
    exclude: /node_modules/,
    use: "stylelint-custom-processor-loader",
  },
]

const prodPlugins = [
  new CopyWebpackPlugin({
    patterns: [{ from: "./assets/", to: "assets/" }, ...monacoPatterns],
  }),
]

module.exports = {
  devServer: {
    compress: true,
    host: "localhost",
    hot: false,
    overlay: !isProdBuild && {
      errors: true,
      warnings: false,
    },
    port: PORT,
    proxy: {
      context: ["/imp", "/exp", "/exec", "/chk"],
      target: `http://localhost:${BACKEND_PORT}/`,
    },
  },
  devtool: isProdBuild ? false : "eval-source-map",
  mode: isProdBuild ? "production" : "development",
  entry: "./src/index",
  output: {
    filename: "qdb.js",
    publicPath: ASSET_PATH,
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js"],
    modules: [path.resolve("./src"), path.resolve("./node_modules")],
  },
  module: {
    rules: [
      {
        test: /\.(png|jpg|ttf|woff)$/,
        use: ["file-loader"],
      },
      {
        test: /\.hbs$/,
        loader: "handlebars-loader",
      },
      {
        test: /\.(ts|js)x?$/,
        exclude: /node_modules/,
        loader: "babel-loader",
      },
      {
        test: /\.css$/i,
        use: [MiniCssExtractPlugin.loader, "css-loader"],
      },
      {
        test: /\.s[ac]ss$/i,
        use: [MiniCssExtractPlugin.loader, "css-loader", "sass-loader"],
      },
      ...(isProdBuild ? [] : devLoaders),
    ],
  },
  plugins: [
    ...basePlugins,
    ...(isProdBuild ? prodPlugins : devPlugins),
    ...(runBundleAnalyzer ? [new AnalyzerPlugin({ analyzerPort: 9998 })] : []),
  ],
  stats: {
    all: false,
    chunks: true,
    env: true,
    errors: true,
    errorDetails: true,
  },
}
