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

const fs = require("fs")
const path = require("path")
const { monacoPatterns } = require("./monaco.config")

const distPath = path.join(__dirname, "dist")

const removeLine = (filePath) => {
  const content = fs.readFileSync(filePath, "utf8").split("\n")
  const contentWithoutSourceMap = content
    .filter((line) => !line.includes("sourceMappingURL"))
    .join("\n")
  fs.writeFileSync(filePath, contentWithoutSourceMap, "utf8")
}

monacoPatterns.forEach(({ to }) => {
  if (Boolean(path.extname(to))) {
    removeLine(path.join(distPath, to))
  } else {
    // `monaco.config` might include paths pointing to folders.
    // In those cases, inspect every file in the folder

    const queue = [
      ...fs
        .readdirSync(path.join(distPath, to))
        .map((p) => path.join(distPath, to, p)),
    ]
    while (queue.length) {
      const item = queue.shift()
      if (Boolean(path.extname(item))) {
        removeLine(item)
      } else {
        const files = fs.readdirSync(item).map((p) => path.join(item, p))
        queue.push(...files)
      }
    }
  }
})
