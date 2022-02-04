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

module.exports = {
  assetCopyPatterns: [
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
  ],

  sourceMapCopyPatterns: [
    { from: "node_modules/monaco-editor/min-maps/vs/", to: "min-maps/vs" },
  ],
}
