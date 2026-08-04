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
     * Materialize the remaining projection for a previously discovered subframe,
     * install its output views into {@code buffers} at {@code columnOffset}, and
     * return the owning, non-zero decode-resource handle.
     */
    long materializeRemaining(
            RowGroupBuffers buffers,
            int columnOffset,
            int rowLo,
            int rowHi,
            long survivorRowsAddress,
            long survivorRowCount,
            boolean fillNulls
    );

    /**
     * Prepare exactly one native window cursor from the fixed descriptors. Has
     * replace semantics: closes any existing cursor and opens a new one. The
     * projection pairs are one contiguous {@code [writerIndex, columnType]}
     * array whose first {@code primaryColumnCount} entries are the primary
     * projection and the rest the remaining projection.
     */
    void prepareWindow(
            int windowIndex,
            DirectIntList projectionPairs,
            int primaryColumnCount,
            long baseViewsAddress,
            int baseViewCount,
            long memoryTrackerAddress
    );

    @Override
    void releaseDecodeResource(long resource);

    void releaseWindow();
}
