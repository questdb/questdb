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

module.exports = {
  env: {
    browser: true,
    es6: true,
  },
  extends: [
    "standard-with-typescript",
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "plugin:react/recommended",
    "plugin:react-hooks/recommended",
    "plugin:prettier/recommended",
  ],
  globals: { ace: true },
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaFeatures: {},
    ecmaVersion: 2018,
    project: path.resolve(__dirname, "./tsconfig.json"),
    tsconfigRootDir: __dirname,
    sourceType: "module",
  },
  plugins: ["@typescript-eslint", "babel", "prettier", "react", "standard"],
  rules: {
    "react/display-name": "off",
    "react-hooks/exhaustive-deps": "off",
    "react/jsx-no-bind": "off",
    "react/no-deprecated": "error",
    "react/style-prop-object": "error",
    "react/self-closing-comp": "error",
    "react/no-unused-prop-types": "warn",
    "react/no-unused-state": "warn",
    "react/no-this-in-sfc": "error",
    "react/no-typos": "error",
    "react/no-redundant-should-component-update": "error",
    "react/no-array-index-key": "error",
    "react/jsx-closing-bracket-location": "error",
    "react/jsx-closing-tag-location": "error",
    "react/jsx-equals-spacing": "error",
    "react/jsx-first-prop-new-line": "error",
    "react/jsx-indent": ["error", 2],
    "react/jsx-indent-props": ["error", 2],
    "react/jsx-pascal-case": "error",
    "react/jsx-sort-props": "error",
    "react/jsx-sort-default-props": "error",
    "react/jsx-wrap-multilines": "error",
    "react/jsx-tag-spacing": [
      "error",
      {
        closingSlash: "never",
        beforeSelfClosing: "always",
        afterOpening: "never",
        beforeClosing: "never",
      },
    ],
    "react/jsx-boolean-value": "error",
    "react/jsx-curly-spacing": "error",
    "react/jsx-no-comment-textnodes": "warn",
    "react/jsx-curly-brace-presence": "error",
    "react/prop-types": "off",
    "jsx-quotes": ["error", "prefer-double"],
    "@typescript-eslint/consistent-type-definitions": ["error", "type"],
    "@typescript-eslint/explicit-function-return-type": "off",
    "@typescript-eslint/explicit-module-boundary-types": "off",
    "@typescript-eslint/no-unused-vars": "warn",
    "@typescript-eslint/strict-boolean-expressions": "off",
    "@typescript-eslint/restrict-template-expressions": "off",
    "quote-props": ["error", "as-needed"],
    "object-shorthand": ["error", "always"],
    "no-unused-vars": "off",
    "no-var": ["error"],
    "no-void": "off",
    "no-console": [
      "warn",
      {
        allow: ["warn", "error", "info"],
      },
    ],
    "prettier/prettier": ["warn"],
  },
  settings: {
    react: {
      version: "detect",
    },
  },
}
