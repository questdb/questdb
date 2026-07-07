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

package io.questdb.cairo;

/**
 * Per-checkout scan profile that a {@link TableReader} adopts while a caller
 * holds it from the pool. Controls two orthogonal concerns that used to be
 * tangled in a single {@code streamingMode} boolean:
 * <ul>
 *   <li><b>Kernel page-cache hints</b>: whether partition columns are mapped
 *       with {@code MADV_SEQUENTIAL} and unmapped with {@code MADV_DONTNEED},
 *       useful for large sequential scans that should not evict the server's
 *       working set.</li>
 *   <li><b>Partition retention on pool return</b>: whether
 *       {@link TableReader#closeExcessPartitions()} (called from
 *       {@code goPassive()} when the reader returns to the pool) keeps the
 *       cached partition mmaps or releases all of them. Releasing is the
 *       right cleanup backstop for one-shot multi-partition exports;
 *       retaining is required for high-frequency small-query workloads that
 *       reuse the same reader's FdCache entries query after query.</li>
 * </ul>
 * <p>
 * Only the three combinations callers actually need are exposed, so there is
 * no way to ask for nonsensical combinations like "evict partitions on return
 * but skip the kernel hints". The profile is set per checkout (typically via
 * {@link io.questdb.cairo.sql.PageFrameCursor#setScanProfile}) and reset to
 * {@link #DEFAULT} by {@code goPassive()} on every pool return.
 */
public enum ReaderScanProfile {
    /**
     * Normal interactive query workload. No kernel hints; partition mmaps
     * are kept cached up to {@code cairo.inactive.reader.max.open.partitions}.
     */
    DEFAULT,

    /**
     * Sequential streaming scan where the caller will reuse the same reader
     * for many subsequent queries. Partition columns are mapped with
     * {@code MADV_SEQUENTIAL}; any partition that is closed during this
     * checkout is hinted with {@code MADV_DONTNEED} before unmap. Partitions
     * stay cached on pool return so the next checkout finds the FdCache and
     * page cache warm. Used by QWP egress.
     */
    SEQUENTIAL_CACHED,

    /**
     * Sequential streaming scan that may touch many partitions in a single
     * pass. Same kernel hints as {@link #SEQUENTIAL_CACHED}, plus a hard
     * cleanup backstop: every partition opened by this checkout is closed
     * when the reader returns to the pool, so a pool-resident reader does not
     * accumulate thousands of mmaps across exports. Used by Parquet export.
     */
    SEQUENTIAL_EVICT
}
