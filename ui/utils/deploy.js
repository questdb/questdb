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
const archiver = require("archiver")
const rimraf = require("rimraf")

const source = path.join(process.cwd(), "/dist/")
const destination = path.join(
  process.cwd(),
  "..",
  "core/src/main/resources/io/questdb/site/public.zip",
)

const start = async () => {
  const archive = archiver("zip", { zlib: { level: 9 } })

  rimraf.sync(destination)
  const stream = fs.createWriteStream(destination)

  archive
    .on("error", (err) => {
      throw err
    })
    .directory(source, false)
    .pipe(stream)

  await archive.finalize()
}

start()
