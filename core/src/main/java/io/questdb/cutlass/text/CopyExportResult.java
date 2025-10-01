/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.SerialParquetExporter;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cutlass.text.CopyExportContext.INACTIVE_COPY_ID;

public class CopyExportResult implements Closeable {
    private final CairoEngine engine;
    private final Path path = new Path();
    private int cleanUpFileLength;
    private long copyID = INACTIVE_COPY_ID;
    private volatile CharSequence message;
    private volatile boolean needCleanUp;
    private volatile CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
    private volatile CopyExportRequestTask.Status status = CopyExportRequestTask.Status.NONE;

    public CopyExportResult(CairoEngine engine) {
        this.engine = engine;
    }

    public void addFilePath(Path path, boolean needCleanUp, int cleanUpFileLength) {
        this.path.of(path);
        this.needCleanUp = needCleanUp;
        this.cleanUpFileLength = cleanUpFileLength;
    }

    public void cleanUpTempPath(FilesFacade ff) {
        if (needCleanUp) {
            SerialParquetExporter.cleanupDir(ff, path, cleanUpFileLength);
        }
    }

    public void clear() {
        copyID = INACTIVE_COPY_ID;
        phase = CopyExportRequestTask.Phase.NONE;
        status = CopyExportRequestTask.Status.NONE;
        message = null;
        path.trimTo(0);
    }

    @Override
    public void close() throws IOException {
        if (copyID != INACTIVE_COPY_ID) {
            engine.getCopyExportContext().cancel(copyID, null);
        }
        Misc.free(path);
    }

    public long getCopyID() {
        return copyID;
    }

    public CharSequence getMessage() {
        return message;
    }

    public Path getPath() {
        return path;
    }

    public CopyExportRequestTask.Phase getPhase() {
        return phase;
    }

    public CopyExportRequestTask.Status getStatus() {
        return status;
    }

    public boolean isFinished() {
        return this.phase == CopyExportRequestTask.Phase.SUCCESS ||
                status == CopyExportRequestTask.Status.FAILED ||
                status == CopyExportRequestTask.Status.CANCELLED;
    }

    public void report(CopyExportRequestTask.Phase phase, CopyExportRequestTask.Status status, CharSequence message) {
        this.phase = phase;
        this.status = status;
        this.message = message;
    }

    public void setCopyID(long copyID) {
        this.copyID = copyID;
    }
}