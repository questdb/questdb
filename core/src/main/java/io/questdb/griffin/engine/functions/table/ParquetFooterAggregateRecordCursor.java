/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;

/**
 * One-row cursor for {@link ParquetFooterAggregateRecordCursorFactory}.
 * <p>
 * On {@link #of}, opens the parquet file, reads per-row-group min/max
 * statistics for the designated timestamp from the parquet footer, and
 * accumulates a global min and global max. No row decode, no row group
 * decompression, no page reads. Then {@link #hasNext} returns true exactly
 * once, exposing a {@link Record} whose long-valued columns are filled
 * from the aggregate buffer.
 * <p>
 * Per-column kind:
 * <ul>
 *   <li>{@code aggregateKinds[i] == false}: column i holds the global min</li>
 *   <li>{@code aggregateKinds[i] == true}: column i holds the global max</li>
 * </ul>
 * The factory wires aggregateKinds to match the planner's projection
 * order, so {@code SELECT min(ts), max(ts)} produces {@code [false, true]}.
 */
class ParquetFooterAggregateRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(ParquetFooterAggregateRecordCursor.class);
    private final boolean[] aggregateKinds;
    private final ParquetFileDecoder decoder = new ParquetFileDecoder();
    // Single-row payload: same length as aggregateKinds. Indexed by output
    // column index; aggregateKinds[i] selects min vs max.
    private final long[] payload;
    private final FooterRecord record;
    private long addr = 0;
    private boolean delivered;
    private long fd = -1;
    private FilesFacade ff;
    private long fileSize = 0;

    ParquetFooterAggregateRecordCursor(boolean[] aggregateKinds) {
        this.aggregateKinds = aggregateKinds;
        this.payload = new long[aggregateKinds.length];
        this.record = new FooterRecord();
    }

    @Override
    public void close() {
        Misc.free(decoder);
        closeFile();
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (delivered) {
            return false;
        }
        delivered = true;
        return true;
    }

    public void of(LPSZ path, int parquetTimestampIndex, SqlExecutionContext executionContext) throws SqlException {
        delivered = false;
        // Pick up FilesFacade from the execution context every of() so cursor
        // reuse across configurations (e.g. test harnesses) sees the right
        // file system instance.
        final CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
        this.ff = configuration.getFilesFacade();
        closeFile();
        this.fd = TableUtils.openRO(ff, path, LOG);
        this.fileSize = ff.length(fd);
        this.addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
        decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

        // The optimiser only routes us here when the file's sorting_columns
        // claim names this column. Min lives in the first row group, max in
        // the last. We still walk every row group so a file with bad sort
        // metadata is forgiving (the global min/max is correct regardless
        // of whether the file is actually sorted).
        final int rowGroupCount = decoder.metadata().getRowGroupCount();
        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (int rg = 0; rg < rowGroupCount; rg++) {
            final long rgMin = decoder.rowGroupMinTimestamp(rg, parquetTimestampIndex);
            final long rgMax = decoder.rowGroupMaxTimestamp(rg, parquetTimestampIndex);
            if (rgMin < globalMin) {
                globalMin = rgMin;
            }
            if (rgMax > globalMax) {
                globalMax = rgMax;
            }
        }
        for (int i = 0; i < aggregateKinds.length; i++) {
            payload[i] = aggregateKinds[i] ? globalMax : globalMin;
        }
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return 1;
    }

    @Override
    public void toTop() {
        delivered = false;
    }

    private void closeFile() {
        if (addr != 0) {
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            addr = 0;
        }
        if (fd != -1) {
            ff.close(fd);
            fd = -1;
        }
        fileSize = 0;
    }

    private class FooterRecord implements Record {
        @Override
        public long getLong(int col) {
            return payload[col];
        }

        @Override
        public long getTimestamp(int col) {
            return payload[col];
        }
    }
}
