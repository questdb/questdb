/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

public final class SerialCsvFileImporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(SerialCsvFileImporter.class);
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final CharSequence inputRoot;
    private int atomicity;
    private ExecutionCircuitBreaker circuitBreaker;
    private byte columnDelimiter;
    private boolean forceHeader;
    private long importId;
    private Path inputFilePath;
    private ParallelCsvFileImporter.PhaseStatusReporter statusReporter;
    private CharSequence tableName;
    private TextLoader textLoader;
    private CharSequence timestampColumn;
    private CharSequence timestampFormat;

    public SerialCsvFileImporter(CairoEngine cairoEngine) {
        this.configuration = cairoEngine.getConfiguration();
        this.inputRoot = configuration.getSqlCopyInputRoot();
        this.inputFilePath = new Path();
        this.ff = configuration.getFilesFacade();
        this.textLoader = new TextLoader(cairoEngine);
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
        this.importId = importId;
        inputFilePath.of(inputRoot).concat(inputFileName).$();
    }

    public void process(SecurityContext securityContext) throws TextImportException {
        LOG.info()
                .$("started [importId=").$hexPadded(importId)
                .$(", file=`").$(inputFilePath).$('`').I$();

        final long startMs = getCurrentTimeMs();

        updateImportStatus(TextImportTask.STATUS_STARTED, Numbers.LONG_NaN, Numbers.LONG_NaN, 0);
        setupTextLoaderFromModel();

        final int sqlCopyBufferSize = cairoEngine.getConfiguration().getSqlCopyBufferSize();
        final long buf = Unsafe.malloc(sqlCopyBufferSize, MemoryTag.NATIVE_IMPORT);
        int fd = -1;
        try {
            fd = TableUtils.openRO(ff, inputFilePath, LOG);
            long fileLen = ff.length(fd);
            long n = ff.read(fd, buf, sqlCopyBufferSize, 0);
            if (n > 0) {
                if (columnDelimiter > 0) {
                    textLoader.configureColumnDelimiter(columnDelimiter);
                }
                textLoader.setForceHeaders(forceHeader);
                textLoader.setSkipLinesWithExtraValues(false);
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
                        .$("import complete [importId=").$hexPadded(importId)
                        .$(", file=`").$(inputFilePath).$('`')
                        .$("', time=").$((endMs - startMs) / 1000).$('s')
                        .I$();
            }
        } catch (TextException e) {
            throw TextImportException.instance(TextImportTask.NO_PHASE, e.getFlyweightMessage());
        } catch (CairoException e) {
            throw TextImportException.instance(TextImportTask.NO_PHASE, e.getFlyweightMessage(), e.getErrno());
        } finally {
            ff.close(fd);
            textLoader.clear();
            Unsafe.free(buf, sqlCopyBufferSize, MemoryTag.NATIVE_IMPORT);
        }
    }

    public void setStatusReporter(ParallelCsvFileImporter.PhaseStatusReporter reporter) {
        this.statusReporter = reporter;
    }

    public void updateImportStatus(byte status, long rowsHandled, long rowsImported, long errors) {
        if (this.statusReporter != null) {
            this.statusReporter.report(TextImportTask.NO_PHASE, status, null, rowsHandled, rowsImported, errors);
        }
    }

    private long getCurrentTimeMs() {
        return configuration.getMillisecondClock().getTicks();
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
}
