const path = require("path")
const { CleanWebpackPlugin } = require("clean-webpack-plugin")
const CopyWebpackPlugin = require("copy-webpack-plugin")
const ForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin")
const HtmlWebpackPlugin = require("html-webpack-plugin")
const MiniCssExtractPlugin = require("mini-css-extract-plugin")
const AnalyzerPlugin = require("webpack-bundle-analyzer").BundleAnalyzerPlugin

const PORT = 9999
const BACKEND_PORT = 9000
const isProdBuild = process.env.NODE_ENV === "production"
const runBundleAnalyzer = process.env.ANALYZE

if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = "development"
}

const basePlugins = [
  new CleanWebpackPlugin(),
  new HtmlWebpackPlugin({
    favicon: "assets/favicon.ico",
    template: "src/index.hbs",
    minify: {
      minifyCSS: true,
      minifyJS: true,
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
]

const devPlugins = [
  new ForkTsCheckerWebpackPlugin({
    eslint: true,
    measureCompilationTime: false,
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
  new CopyWebpackPlugin({ patterns: [{ from: "./assets/", to: "assets/" }] }),
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
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js"],
    modules: [path.resolve("./src"), path.resolve("./node_modules")],
  },
  module: {
    rules: [
      {
        test: /\.(png|svg|jpg|gif)$/,
        use: ["file-loader"],
      },
      {
        test: /\.(woff|woff2|eot|ttf|otf)$/,
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
