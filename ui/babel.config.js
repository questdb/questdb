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

const plugins = [
  [
    "@babel/plugin-transform-runtime",
    {
      corejs: 3,
      regenerator: true,
      useESModules: true,
    },
  ],
]

module.exports = {
  env: {
    development: {
      plugins: [
        [
          "styled-components",
          {
            displayName: true,
            minify: false,
            pure: true,
            ssr: false,
          },
        ],
        "@babel/plugin-transform-react-jsx-source",
        ...plugins,
      ],
    },
    production: {
      plugins: [
        [
          "styled-components",
          {
            displayName: false,
            minify: false,
            pure: true,
            ssr: false,
          },
        ],
        ...plugins,
      ],
    },
    test: {
      presets: [
        [
          "@babel/env",
          {
            shippedProposals: true,
            targets: {
              node: "current",
            },
          },
        ],
      ],
    },
  },
  plugins: [],
  presets: [
    [
      "@babel/env",
      {
        corejs: 3,
        modules: false,
        shippedProposals: true,
        useBuiltIns: "entry",
      },
    ],
    "@babel/react",
    "@babel/typescript",
  ],
}
