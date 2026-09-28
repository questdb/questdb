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

package io.questdb.cairo;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;

/** The adapter of builds without Delta: it refuses Delta state, which it cannot capture or restore. */
public final class UnsupportedDeltaCheckpoint implements DeltaCheckpoint {
    private final FindVisitor catalogVisitor = this::checkCatalog;
    private final DirectUtf8StringZ nameSink = new DirectUtf8StringZ();
    private final FindVisitor partitionVisitor = this::checkPartition;
    private FilesFacade ff;
    private Path table;
    private int tableLen;

    @Override
    public void capture(TableReader reader, Path checkpoint, SqlExecutionCircuitBreaker circuitBreaker) {
        if (reader.hasAnyDelta()) {
            throw CairoException.nonCritical().put("Delta checkpoint capture is not supported");
        }
    }

    @Override
    public void restore(FilesFacade ff, Path checkpoint, Path table) {
        this.ff = ff;
        this.table = table;
        this.tableLen = table.size();
        try {
            ff.iterateDir(checkpoint.$(), catalogVisitor);
            // Delta written after the capture lives in partition directories, e.g. 2020-01-01.5/_delta.
            ff.iterateDir(table.$(), partitionVisitor);
        } finally {
            table.trimTo(tableLen);
            this.table = null;
            this.ff = null;
        }
    }

    private void checkCatalog(long name, int type) {
        if (Utf8s.endsWithAscii(nameSink.of(name), CATALOG_SUFFIX)) {
            throw CairoException.nonCritical().put("Delta checkpoint recovery is not supported");
        }
    }

    private void checkPartition(long name, int type) {
        if (Files.notDots(name) && ff.exists(table.trimTo(tableLen).concat(name).concat(DIRECTORY_NAME).$())) {
            throw CairoException.nonCritical().put("Delta checkpoint recovery is not supported");
        }
    }
}
