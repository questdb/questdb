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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;

public final class SerialCsvFileImporter implements Closeable {
    private final int sqlCopyBufferSize;
    private final CharSequence sqlCopyInputRoot;
    private final FilesFacade ff;
    private Path path;
    private final CairoSecurityContext securityContext;
    private TextLoader textLoader;
    private CharSequence tableName;
    private CharSequence fileName;
    private boolean isHeaderFlag;
    private ParallelCsvFileImporter.PhaseStatusReporter statusReporter;
    private ExecutionCircuitBreaker circuitBreaker;
    private int atomicity;

    public SerialCsvFileImporter(CairoEngine engine) {
        CairoConfiguration configuration = engine.getConfiguration();
        this.sqlCopyBufferSize = configuration.getSqlCopyBufferSize();
        this.sqlCopyInputRoot = configuration.getSqlCopyInputRoot();
        this.path = new Path();
        this.ff = configuration.getFilesFacade();
        this.textLoader = new TextLoader(engine);
        this.securityContext = AllowAllCairoSecurityContext.INSTANCE;
    }

    @Override
    public void close() {
        path = Misc.free(path);
        textLoader = Misc.free(textLoader);
    }

    public void of(String tableName, String fileName, boolean isHeaderFlag, int atomicity, ExecutionCircuitBreaker circuitBreaker) {
        this.tableName = tableName;
        this.fileName = fileName;
        this.isHeaderFlag = isHeaderFlag;
        this.atomicity = atomicity;
        this.circuitBreaker = circuitBreaker;
    }

    public void process() throws TextImportException {
        long buf = Unsafe.malloc(sqlCopyBufferSize, MemoryTag.NATIVE_PARALLEL_IMPORT);
        setupTextLoaderFromModel();
        path.of(sqlCopyInputRoot).concat(fileName).$();
        long fd = ff.openRO(path);
        try {
            statusReporter.report(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE, TextImportTask.STATUS_STARTED, null);
            if (fd == -1) {
                throw TextImportException.instance(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE, "could not open file [errno=").put(Os.errno()).put(", path=").put(path).put(']');
            }

            long fileLen = ff.length(fd);
            long n = ff.read(fd, buf, sqlCopyBufferSize, 0);
            if (n > 0) {
                textLoader.setForceHeaders(isHeaderFlag);
                textLoader.setSkipRowsWithExtraValues(false);
                textLoader.parse(buf, buf + n, securityContext);
                textLoader.setState(TextLoader.LOAD_DATA);
                int read;
                statusReporter.report(TextImportTask.PHASE_ANALYZE_FILE_STRUCTURE, TextImportTask.STATUS_FINISHED, null);
                statusReporter.report(TextImportTask.PHASE_PARTITION_IMPORT, TextImportTask.STATUS_STARTED, null);
                while (n < fileLen) {
                    if (circuitBreaker.checkIfTripped()) {
                        TextImportException ex = TextImportException.instance(TextImportTask.PHASE_PARTITION_IMPORT, "import was cancelled");
                        ex.setCancelled(true);
                        throw ex;
                    }
                    read = (int) ff.read(fd, buf, sqlCopyBufferSize, n);
                    if (read < 1) {
                        throw TextImportException.instance(TextImportTask.PHASE_PARTITION_IMPORT, "could not read file [errno=").put(ff.errno()).put(']');
                    }
                    textLoader.parse(buf, buf + read, securityContext);
                    n += read;
                }
                textLoader.wrapUp();
                statusReporter.report(TextImportTask.PHASE_PARTITION_IMPORT, TextImportTask.STATUS_FINISHED, null);
            }
        } finally {
            if (fd != -1) {
                ff.close(fd);
            }
            textLoader.clear();
            Unsafe.free(buf, sqlCopyBufferSize, MemoryTag.NATIVE_PARALLEL_IMPORT);
        }
    }

    public void setStatusReporter(ParallelCsvFileImporter.PhaseStatusReporter reporter) {
        this.statusReporter = reporter;
    }

    private void setupTextLoaderFromModel() {
        textLoader.clear();
        textLoader.setState(TextLoader.ANALYZE_STRUCTURE);
        // todo: configure the following
        //   - what happens when data row errors out, max errors may be?
        //   - we should be able to skip X rows from top, dodgy headers etc.

        textLoader.configureDestination(tableName, false, false,
                atomicity != -1 ? atomicity : Atomicity.SKIP_ROW, PartitionBy.NONE,null);
    }
}
