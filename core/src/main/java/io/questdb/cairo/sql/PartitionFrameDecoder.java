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

import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.QuietCloseable;

/**
 * Task-local materializer for an immutable, Rust-owned partition-frame state.
 * The state itself belongs to {@link io.questdb.cairo.TableReader}. A caller
 * prepares one borrowed base-view set for a window, materializes any number of
 * logical subframes, then releases the window before the backing views.
 */
public interface PartitionFrameDecoder extends DecodeResourceReleaser, QuietCloseable {
    /** Supplies keys from a requested preceding base window; views are consumed synchronously. */
    default void addBaseWindow(int windowIndex, long baseViewsAddress, int baseViewCount) {
        throw new UnsupportedOperationException();
    }

    void bind(long state);

    /**
     * Decode a filtered subframe, install its output views into {@code buffers}
     * at {@code columnOffset}, and return the owning, non-zero decode-resource
     * handle. Zero survivors are valid.
     */
    long decodeFilteredSubframe(
            RowGroupBuffers buffers,
            int columnOffset,
            int rowLo,
            int rowHi,
            long survivorRowsAddress,
            long survivorRowCount,
            boolean fillNulls
    );

    /**
     * Decode a full subframe, install its output views into {@code buffers}, and
     * return the owning, non-zero decode-resource handle.
     */
    long decodeSubframe(RowGroupBuffers buffers, int rowLo, int rowHi);

    /**
     * Returns the exact number of output rows the decode owning {@code resource} produced,
     * including zero, or {@code -1} when the decoder does not report it. A frame whose
     * declared row count is not exact ({@link PageFrame#isExactRowCount()}) may only be
     * decoded by a decoder that reports this count.
     */
    default long decodedRowCount(long resource) {
        return -1;
    }

    /**
     * Materialize the requested remaining columns in the supplied order for a previously
     * discovered subframe. Install the views into {@code buffers} at {@code columnOffset}
     * and return the owning, non-zero decode-resource handle. Columns must belong to the
     * prepared remaining projection.
     */
    long materializeRemaining(
            RowGroupBuffers buffers,
            int columnOffset,
            DirectIntList columns,
            int rowLo,
            int rowHi,
            long survivorRowsAddress,
            long survivorRowCount,
            boolean fillNulls
    );

    /**
     * Prepare exactly one native window cursor from the fixed descriptors. Has
     * replace semantics: closes any existing cursor and opens a new one. The
     * projection pairs are one contiguous {@code [columnId, columnType]}
     * array whose first {@code primaryColumnCount} entries are the primary
     * projection and the rest the remaining projection. Each column ID is the
     * original writer index, preserved across type changes.
     */
    void prepareWindow(
            int windowIndex,
            DirectIntList projectionPairs,
            int primaryColumnCount,
            long baseViewsAddress,
            int baseViewCount,
            long memoryTrackerAddress
    );

    /** Earlier base window needed to resolve a shared timestamp, or -1 when ready. */
    default int requiredBaseWindow() {
        return -1;
    }

    @Override
    void releaseDecodeResource(long resource);

    /**
     * Returns the exact source-row sidecar owned by {@code resource}, or {@code 0} when absent.
     */
    default long sourceRowRefsAddress(long resource) {
        return 0;
    }

    /**
     * Returns the logical timestamp column parallel to {@link #sourceRowRefsAddress(long)}.
     */
    default long sourceRowTimestampsAddress(long resource) {
        return 0;
    }

    void releaseWindow();
}
