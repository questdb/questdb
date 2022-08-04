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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public final class SerialCsvFileImporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(SerialCsvFileImporter.class);
    private final CharSequence inputRoot;
    private final FilesFacade ff;
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private Path inputFilePath;
    private final CairoSecurityContext securityContext;
    private TextLoader textLoader;
    private CharSequence tableName;
    private CharSequence timestampColumn;
    private CharSequence timestampFormat;
    private byte columnDelimiter;
    private boolean forceHeader;
    private int atomicity;
    private ParallelCsvFileImporter.PhaseStatusReporter statusReporter;
    private ExecutionCircuitBreaker circuitBreaker;
    // Initialized only once per of() call.
    private final StringSink importIdSink = new StringSink();

    public SerialCsvFileImporter(CairoEngine cairoEngine) {
        this.configuration = cairoEngine.getConfiguration();
        this.inputRoot = configuration.getSqlCopyInputRoot();
        this.inputFilePath = new Path();
        this.ff = configuration.getFilesFacade();
        this.textLoader = new TextLoader(cairoEngine);
        this.securityContext = AllowAllCairoSecurityContext.INSTANCE;
        this.cairoEngine = cairoEngine;
    }

    @Override
    public void close() {
        inputFilePath = Misc.free(inputFilePath);
        textLoader = Misc.free(textLoader);
    }

    public void of(
            String tableName,
            String inputFileName,
            long importId,
            byte columnDelimiter,
            CharSequence timestampColumn,
            CharSequence timestampFormat,
            boolean forceHeader,
            ExecutionCircuitBreaker circuitBreaker,
            int atomicity
    ) {
        this.tableName = tableName;
        this.timestampColumn = timestampColumn;
        this.timestampFormat = timestampFormat;
        this.columnDelimiter = columnDelimiter;
        this.forceHeader = forceHeader;
        this.circuitBreaker = circuitBreaker;
        this.atomicity = atomicity;
        inputFilePath.of(inputRoot).concat(inputFileName).$();
        importIdSink.clear();
        Numbers.appendHex(importIdSink, importId, true);
    }

    public void process() throws TextImportException {
        LOG.info()
                .$("started [importId=").$(importIdSink)
                .$(", file=`").$(inputFilePath).$('`').I$();

        final long startMs = getCurrentTimeMs();

        updateImportStatus(TextImportTask.STATUS_STARTED, Numbers.LONG_NaN, Numbers.LONG_NaN, 0);
        setupTextLoaderFromModel();

        final int sqlCopyBufferSize = cairoEngine.getConfiguration().getSqlCopyBufferSize();
        final long buf = Unsafe.malloc(sqlCopyBufferSize, MemoryTag.NATIVE_IMPORT);
        long fd = -1;
        try {
            fd = TableUtils.openRO(ff, inputFilePath, LOG);
            long fileLen = ff.length(fd);
            long n = ff.read(fd, buf, sqlCopyBufferSize, 0);
            if (n > 0) {
                if (columnDelimiter > 0) {
                    textLoader.configureColumnDelimiter(columnDelimiter);
                }
                textLoader.setForceHeaders(forceHeader);
                textLoader.setSkipRowsWithExtraValues(false);
                textLoader.parse(buf, buf + n, securityContext);
                textLoader.setState(TextLoader.LOAD_DATA);
                int read;
                while (n < fileLen) {
                    if (circuitBreaker.checkIfTripped()) {
                        TextImportException ex = TextImportException.instance(TextImportTask.NO_PHASE, "import was cancelled");
                        ex.setCancelled(true);
                        throw ex;
                    }
                    read = (int) ff.read(fd, buf, sqlCopyBufferSize, n);
                    if (read < 1) {
                        throw TextImportException.instance(TextImportTask.NO_PHASE, "could not read file [errno=").put(ff.errno()).put(']');
                    }
                    textLoader.parse(buf, buf + read, securityContext);
                    n += read;
                }
                textLoader.wrapUp();

                long errorCount = textLoader.getErrorLineCount();
                LongList columnErrorCounts = textLoader.getColumnErrorCounts();
                for (int i = 0, size = columnErrorCounts.size(); i < size; i++) {
                    errorCount += columnErrorCounts.get(i);
                }
                updateImportStatus(TextImportTask.STATUS_FINISHED, textLoader.getParsedLineCount(), textLoader.getWrittenLineCount(), errorCount);

                long endMs = getCurrentTimeMs();
                LOG.info()
                        .$("import complete [importId=").$(importIdSink)
                        .$(", file=`").$(inputFilePath).$('`')
                        .$("', time=").$((endMs - startMs) / 1000).$('s')
                        .I$();
            }
        } catch (TextException e) {
            throw TextImportException.instance(TextImportTask.NO_PHASE, e.getFlyweightMessage());
        } catch (CairoException e) {
            throw TextImportException.instance(TextImportTask.NO_PHASE, e.getFlyweightMessage(), e.getErrno());
        } finally {
            if (fd != -1) {
                ff.close(fd);
            }
            textLoader.clear();
            Unsafe.free(buf, sqlCopyBufferSize, MemoryTag.NATIVE_IMPORT);
        }
    }

    public void updateImportStatus(byte status, long rowsHandled, long rowsImported, long errors) {
        if (this.statusReporter != null) {
            this.statusReporter.report(TextImportTask.NO_PHASE, status, null, rowsHandled, rowsImported, errors);
        }
    }

    public void setStatusReporter(ParallelCsvFileImporter.PhaseStatusReporter reporter) {
        this.statusReporter = reporter;
    }

    private void setupTextLoaderFromModel() {
        textLoader.clear();
        textLoader.setState(TextLoader.ANALYZE_STRUCTURE);
        textLoader.configureDestination(
                tableName,
                false,
                false,
                atomicity != -1 ? atomicity : Atomicity.SKIP_ROW,
                PartitionBy.NONE,
                timestampColumn,
                timestampFormat
        );
    }

    private long getCurrentTimeMs() {
        return configuration.getMillisecondClock().getTicks();
    }
}
