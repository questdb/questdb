/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.text;

import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class CopyExportResult {
    private final Utf8StringSink path = new Utf8StringSink();
    private int cleanUpFileLength;
    private volatile boolean needCleanUp;
    private volatile CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
    private volatile CopyExportRequestTask.Status status = CopyExportRequestTask.Status.NONE;

    public void addFilePath(Path path, boolean needCleanUp, int cleanUpFileLength) {
        this.path.put(path);
        this.needCleanUp = needCleanUp;
        this.cleanUpFileLength = cleanUpFileLength;
    }

    public void cleanUpTempPath(FilesFacade ff) {
        if (needCleanUp) {
            path.clear(cleanUpFileLength, path.isAscii());
            TableUtils.cleanupDirQuiet(ff, path);
        }
    }

    public void clear() {
        phase = CopyExportRequestTask.Phase.NONE;
        status = CopyExportRequestTask.Status.NONE;
        path.clear();
        needCleanUp = false;
        cleanUpFileLength = 0;
    }

    public Utf8Sequence getPath() {
        return path;
    }
}