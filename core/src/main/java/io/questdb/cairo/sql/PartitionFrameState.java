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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.std.Unsafe;

/**
 * Stable Java view of the fixed ABI header in an opaque Rust partition-state handle.
 */
public final class PartitionFrameState {
    public static final long HEADER_BYTES = 13L * Long.BYTES;
    public static final int SUMMARY_BASE_ROWS = 0;
    public static final int SUMMARY_FLAGS = 3;
    public static final int SUMMARY_LOGICAL_ROWS = 2;
    public static final int SUMMARY_LONGS = 4;
    public static final long WINDOW_HAS_DELTA = 1;
    /**
     * Set when the window's logical row count is only an upper bound: decoding may drop
     * rows (e.g. duplicate resolution), so frames over this window must report
     * {@code getRowCountLo() == 0} and consumers must use the decoded row count.
     */
    public static final long WINDOW_ROW_COUNT_INEXACT = 2;
    private static final int HEADER_BASE_ID = 6;
    private static final int HEADER_BASE_ROWS = 5;
    private static final int HEADER_DEDUP_KEYS_ADDR = 11;
    private static final int HEADER_DEDUP_KEY_COUNT = 12;
    private static final int HEADER_HAS_CUSTOM_FRAMES = 3;
    private static final int HEADER_LOGICAL_ROWS = 2;
    private static final int HEADER_PARQUET_ADDR = 9;
    private static final int HEADER_PARQUET_FILE_SIZE = 10;
    private static final int HEADER_PARQUET_METADATA_ADDR = 7;
    private static final int HEADER_PARQUET_METADATA_SIZE = 8;
    private static final int HEADER_SUBFRAME_SIZE = 4;
    private static final int HEADER_SUMMARIES_ADDR = 0;
    private static final int HEADER_WINDOW_COUNT = 1;

    private PartitionFrameState() {
    }

    /**
     * The pinned base's catalog identity; a reader keeps its partition open while it stays the same.
     */
    public static long getBaseId(long state) {
        return headerValue(state, HEADER_BASE_ID);
    }

    public static long getBasePartitionRowCount(long state) {
        return headerValue(state, HEADER_BASE_ROWS);
    }

    public static long getBaseRowCount(long state, int window) {
        return summaryValue(state, window, SUMMARY_BASE_ROWS);
    }

    /**
     * Column ID (original writer index) of the i-th non-timestamp dedup key. Frames over an
     * inexact window must supply these columns to the decoder even when the query does not
     * select them, because duplicate resolution compares them.
     */
    public static int getDedupKeyColumnId(long state, int index) {
        return (int) dedupKeyCell(state, index, 0);
    }

    /**
     * Number of non-timestamp dedup keys the table declares; zero without dedup.
     */
    public static int getDedupKeyCount(long state) {
        return Math.toIntExact(headerValue(state, HEADER_DEDUP_KEY_COUNT));
    }

    /**
     * Column type of the i-th non-timestamp dedup key.
     */
    public static int getDedupKeyType(long state, int index) {
        return (int) dedupKeyCell(state, index, 1);
    }

    public static long getLogicalPartitionRowCount(long state) {
        return headerValue(state, HEADER_LOGICAL_ROWS);
    }

    public static long getLogicalRowCount(long state, int window) {
        return summaryValue(state, window, SUMMARY_LOGICAL_ROWS);
    }

    /**
     * The shared mapping of a local Parquet base's data file, or 0 for a remote or native base.
     */
    public static long getParquetAddr(long state) {
        return headerValue(state, HEADER_PARQUET_ADDR);
    }

    public static long getParquetFileSize(long state) {
        return headerValue(state, HEADER_PARQUET_FILE_SIZE);
    }

    /**
     * The shared mapping of a Parquet base's committed _pm prefix, or 0 for a native base.
     */
    public static long getParquetMetadataAddr(long state) {
        return headerValue(state, HEADER_PARQUET_METADATA_ADDR);
    }

    public static long getParquetMetadataSize(long state) {
        return headerValue(state, HEADER_PARQUET_METADATA_SIZE);
    }

    public static long getSubframeSize(long state) {
        return headerValue(state, HEADER_SUBFRAME_SIZE);
    }

    public static int getWindowCount(long state) {
        return Math.toIntExact(headerValue(state, HEADER_WINDOW_COUNT));
    }

    public static boolean hasCustomFrames(long state) {
        return headerValue(state, HEADER_HAS_CUSTOM_FRAMES) != 0;
    }

    /**
     * Whether any window of this bound state has an inexact row count, i.e. decoding
     * the partition may drop rows; see {@link #isExactRowCount(long, int)}.
     */
    public static boolean hasInexactWindows(long state) {
        for (int window = 0, n = getWindowCount(state); window < n; window++) {
            if (!isExactRowCount(state, window)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Data summaries exist only after TableReader binds the snapshot for data-frame access.
     */
    public static boolean isBound(long state) {
        return headerValue(state, HEADER_SUMMARIES_ADDR) != 0;
    }

    /**
     * Whether the window's logical row count is exact. A missing proof is never treated as
     * exact: the Rust binding sets {@link #WINDOW_ROW_COUNT_INEXACT} whenever it cannot show
     * that decoding will yield every logical row.
     */
    public static boolean isExactRowCount(long state, int window) {
        return (summaryValue(state, window, SUMMARY_FLAGS) & WINDOW_ROW_COUNT_INEXACT) == 0;
    }

    public static boolean requiresMaterialization(long state, int window) {
        return (summaryValue(state, window, SUMMARY_FLAGS) & WINDOW_HAS_DELTA) != 0;
    }

    private static long dedupKeyCell(long state, int index, int cell) {
        final long addr = headerValue(state, HEADER_DEDUP_KEYS_ADDR);
        final int count = getDedupKeyCount(state);
        if (addr == 0 || index < 0 || index >= count) {
            throw CairoException.critical(0)
                    .put("invalid partition frame state dedup key index [index=").put(index)
                    .put(", count=").put(count).put(']');
        }
        return Unsafe.getLong(addr + ((long) index * 2 + cell) * Long.BYTES);
    }

    private static long headerValue(long state, int cell) {
        if (state == 0) {
            throw CairoException.critical(0).put("partition frame state pointer is null");
        }
        return Unsafe.getLong(state + (long) cell * Long.BYTES);
    }

    private static long summaryValue(long state, int window, int cell) {
        final long summariesAddr = headerValue(state, HEADER_SUMMARIES_ADDR);
        final int windowCount = getWindowCount(state);
        if (summariesAddr == 0 || window < 0 || window >= windowCount) {
            throw CairoException.critical(0)
                    .put("invalid partition frame state summary index [window=").put(window)
                    .put(", windowCount=").put(windowCount).put(']');
        }
        return Unsafe.getLong(
                summariesAddr + ((long) window * SUMMARY_LONGS + cell) * Long.BYTES
        );
    }
}
