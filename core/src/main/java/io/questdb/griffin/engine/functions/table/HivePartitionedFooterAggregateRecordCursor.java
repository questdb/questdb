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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringList;

/**
 * One-row cursor for {@link HivePartitionedFooterAggregateRecordCursorFactory}.
 * <p>
 * On {@link #of}, walks every file in the hive factory's matchedFiles list,
 * opens each via the hive factory's per-file cache (so the open + footer parse
 * cost is paid once and reused on toTop), reads per-row-group min/max from the
 * parquet footer, and accumulates a global aggregate across all files. No row
 * decode, no decompression, no page reads.
 * <p>
 * Cost shape: O(#files × #row-groups-per-file) JNI calls. For a typical hive
 * dashboard (8 day partitions × 50 row groups each = 400 footer reads ≈ 200 µs)
 * this is several orders of magnitude under the prior full-scan cost.
 * <p>
 * The parquet-side timestamp index is resolved per-file from the decoder's
 * metadata - hive files are written by QuestDB so they all share the same
 * column layout, but the per-file lookup is cheap and avoids a planning-time
 * assumption that could go stale if a glob ever spans files with different
 * column orderings.
 */
class HivePartitionedFooterAggregateRecordCursor implements NoRandomAccessRecordCursor {
    private final boolean[] aggregateKinds;
    // Null = legacy designated-timestamp path; non-null = generic typed
    // path resolved per-file by name against the parquet schema.
    private final String aggregateColumnName;
    // Single-row payload: same length as aggregateKinds. Indexed by output
    // column index; aggregateKinds[i] selects min vs max.
    private final long[] payload;
    private final FooterRecord record;
    private boolean delivered;

    HivePartitionedFooterAggregateRecordCursor(boolean[] aggregateKinds, String aggregateColumnName) {
        this.aggregateKinds = aggregateKinds;
        this.aggregateColumnName = aggregateColumnName;
        this.payload = new long[aggregateKinds.length];
        this.record = new FooterRecord();
    }

    @Override
    public void close() {
        // The cached files belong to the hive factory; it owns their lifetime.
        // Nothing to free here.
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

    public void of(HivePartitionedReadParquetRecordCursorFactory base, SqlExecutionContext executionContext) throws SqlException {
        delivered = false;
        final CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        final DirectUtf8StringList matchedFiles = base.getMatchedFiles();

        final boolean useGenericNative = aggregateColumnName != null;
        // Pre-walk first file to discover the column type tag so we pick
        // the right native and global accumulator. Skipped if matchedFiles
        // is empty - the empty-glob branch below already handles that.
        int columnTypeTag = ColumnType.UNDEFINED;
        for (int i = 0, n = matchedFiles.size(); i < n && columnTypeTag == ColumnType.UNDEFINED; i++) {
            final DirectUtf8Sequence filePath = matchedFiles.getQuick(i);
            final HivePartitionedReadParquetRecordCursorFactory.CachedFile cached =
                    base.openCachedFile(filePath, ff);
            final ParquetFileDecoder decoder = cached.decoder;
            final int parquetColumnIndex = useGenericNative
                    ? decoder.metadata().getColumnIndex(aggregateColumnName)
                    : decoder.metadata().getTimestampIndex();
            if (parquetColumnIndex >= 0) {
                columnTypeTag = useGenericNative
                        ? ColumnType.tagOf(decoder.metadata().getColumnType(parquetColumnIndex))
                        : ColumnType.TIMESTAMP;
            }
        }

        if (columnTypeTag == ColumnType.DOUBLE) {
            double globalMinD = Double.POSITIVE_INFINITY;
            double globalMaxD = Double.NEGATIVE_INFINITY;
            boolean anyFileContributed = false;
            outer:
            for (int i = 0, n = matchedFiles.size(); i < n; i++) {
                final DirectUtf8Sequence filePath = matchedFiles.getQuick(i);
                final HivePartitionedReadParquetRecordCursorFactory.CachedFile cached =
                        base.openCachedFile(filePath, ff);
                final ParquetFileDecoder decoder = cached.decoder;
                final int parquetColumnIndex = decoder.metadata().getColumnIndex(aggregateColumnName);
                if (parquetColumnIndex < 0) {
                    continue;
                }
                final int rowGroupCount = decoder.metadata().getRowGroupCount();
                for (int rg = 0; rg < rowGroupCount; rg++) {
                    final double rgMin = decoder.rowGroupMinValueDouble(rg, parquetColumnIndex);
                    final double rgMax = decoder.rowGroupMaxValueDouble(rg, parquetColumnIndex);
                    if (Double.isNaN(rgMin) || Double.isNaN(rgMax)) {
                        globalMinD = Double.NaN;
                        globalMaxD = Double.NaN;
                        anyFileContributed = true;
                        break outer;
                    }
                    if (rgMin < globalMinD) {
                        globalMinD = rgMin;
                    }
                    if (rgMax > globalMaxD) {
                        globalMaxD = rgMax;
                    }
                    anyFileContributed = true;
                }
            }
            if (!anyFileContributed) {
                for (int i = 0; i < aggregateKinds.length; i++) {
                    payload[i] = Long.MIN_VALUE;
                }
                return;
            }
            for (int i = 0; i < aggregateKinds.length; i++) {
                payload[i] = Double.doubleToRawLongBits(aggregateKinds[i] ? globalMaxD : globalMinD);
            }
            return;
        }

        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        boolean anyFileContributed = false;

        for (int i = 0, n = matchedFiles.size(); i < n; i++) {
            final DirectUtf8Sequence filePath = matchedFiles.getQuick(i);
            final HivePartitionedReadParquetRecordCursorFactory.CachedFile cached =
                    base.openCachedFile(filePath, ff);
            final ParquetFileDecoder decoder = cached.decoder;
            final int parquetColumnIndex = useGenericNative
                    ? decoder.metadata().getColumnIndex(aggregateColumnName)
                    : decoder.metadata().getTimestampIndex();
            if (parquetColumnIndex < 0) {
                // Defensive: planner gate requires the column to exist and be
                // declared sorted. Skip files lacking it rather than passing
                // -1 to the native and surfacing a confusing decoder error.
                continue;
            }
            final int rowGroupCount = decoder.metadata().getRowGroupCount();
            for (int rg = 0; rg < rowGroupCount; rg++) {
                final long rgMin;
                final long rgMax;
                if (useGenericNative) {
                    rgMin = decoder.rowGroupMinValueLong(rg, parquetColumnIndex);
                    rgMax = decoder.rowGroupMaxValueLong(rg, parquetColumnIndex);
                } else {
                    rgMin = decoder.rowGroupMinTimestamp(rg, parquetColumnIndex);
                    rgMax = decoder.rowGroupMaxTimestamp(rg, parquetColumnIndex);
                }
                if (rgMin < globalMin) {
                    globalMin = rgMin;
                }
                if (rgMax > globalMax) {
                    globalMax = rgMax;
                }
                anyFileContributed = true;
            }
        }

        // Empty glob - no files contributed. Emit Long.MIN_VALUE in every
        // output slot. The caller's downstream NULL handling will surface
        // empty-input semantics naturally via the LIMIT / outer projection;
        // the cursor itself still emits exactly one row, matching the
        // single-file factory's behaviour for an empty file.
        if (!anyFileContributed) {
            for (int i = 0; i < aggregateKinds.length; i++) {
                payload[i] = Long.MIN_VALUE;
            }
            return;
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

    private class FooterRecord implements Record {
        @Override
        public double getDouble(int col) {
            return Double.longBitsToDouble(payload[col]);
        }

        @Override
        public int getInt(int col) {
            return (int) payload[col];
        }

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
