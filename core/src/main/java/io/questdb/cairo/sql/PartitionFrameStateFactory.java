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
import io.questdb.cairo.PartitionDeltaStats;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Opens and owns opaque native partition-frame states. The returned handle is
 * retained by {@link TableReader}; worker-local {@link PartitionFrameDecoder}
 * instances borrow it while materializing page frames.
 */
public interface PartitionFrameStateFactory extends QuietCloseable {
    /**
     * Binds data windows before TableReader exposes the handle to page-frame workers.
     */
    void bind(long state);

    /**
     * Returns the index reader for a partition state, or {@code null} when unavailable.
     */
    default @Nullable IndexReader getIndexReader(
            TableReader reader,
            long state,
            int partitionIndex,
            int columnIndex,
            int direction
    ) {
        return null;
    }

    /**
     * Returns the snapshot's row count. Implementations can avoid binding data windows.
     */
    default long getLogicalRowCount(long state) {
        bind(state);
        return PartitionFrameState.getLogicalPartitionRowCount(state);
    }

    /**
     * Returns the rows a full scan of the partition yields. Implementations that
     * resolve duplicates on read decode only what inexact windows need; the reader
     * has opened the partition without its projected columns.
     */
    default long getResolvedRowCount(TableReader reader, long state, int partitionIndex) {
        if (hasInexactRowCount(state)) {
            throw CairoException.critical(0).put("partition frame state cannot resolve row counts [partitionIndex=")
                    .put(partitionIndex).put(']');
        }
        return getLogicalRowCount(state);
    }

    /**
     * Checks row-count exactness. Implementations may rule out dedup from Run
     * metadata without binding windows or opening physical data columns.
     */
    default boolean hasInexactRowCount(long state) {
        bind(state);
        return PartitionFrameState.hasInexactWindows(state);
    }

    /**
     * Pins one immutable snapshot without binding data windows and returns its owning handle.
     */
    long open(TableReader reader, int partitionIndex, long readerSeqTxn);

    /**
     * Reads the physical-source fields from the snapshot's pinned catalog base.
     * Opens no physical files and binds no data windows.
     */
    void readSource(long state, PartitionFrameSource target);

    /**
     * Reads logical rows and Delta-only bounds from the same pinned snapshot.
     */
    void readStats(long state, PartitionDeltaStats target);

    /**
     * Destroys the state owned by TableReader. No other component owns the handle.
     */
    void destroy(long state);
}
