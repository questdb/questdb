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

package io.questdb;

/**
 * Derives memory sizing from a single declared budget.
 * <p>
 * Pure arithmetic with no configuration dependencies, so the derivation is
 * testable without booting a server. When {@link #isEnabled()} is false every
 * getter returns {@code -1} (and {@link #getSqlPoolCapacity(int)} returns its
 * argument unchanged), so callers fall back to their existing defaults — this
 * is what preserves byte-for-byte compatibility for deployments that declare
 * no budget.
 * <p>
 * Ordering matters here. Measurement showed that at 256 MiB QuestDB is killed
 * during bootstrap, before any append buffer is touched: the limiter is
 * boot-time state, not steady-state write buffers. So the boot-state terms are
 * derived from the budget first and the append terms take what is left.
 */
public class MemoryBudget {
    /**
     * Query-arena bytes required per query worker.
     * <p>
     * Parallel GROUP BY holds one hash map per query worker, so query memory
     * scales with worker count and an unbounded worker count overruns the
     * arena. Measured at a 512 MB budget (96 MB query arena), 15 TSBS queries
     * x 7 repetitions: <b>12 workers</b> rejected {@code double-groupby-1/5/all}
     * in 3 of 7 reps against the query limit; <b>4</b> and <b>2</b> workers were
     * both 105/105 clean. Totals across the set were 672 ms at 4 workers,
     * 1043 ms at 2, and 1832 ms with parallel GROUP BY disabled outright — so
     * bounding parallelism beats switching it off on both reliability and speed.
     * <p>
     * 24 MB/worker yields 4 workers at a 512 MB budget (observed good) and stays
     * well clear of the 8 MB/worker implied by 12 (observed bad). The true
     * boundary lies somewhere in 4..12 and was not bisected, so this is
     * deliberately conservative rather than fitted to an edge.
     */
    private static final long MEM_PER_QUERY_WORKER = 24L * 1024 * 1024;
    /** Smallest sane allocation unit; also the alignment for every derived size. */
    private static final long MIN_PAGE = 4096;
    /** A pool too small to hold a parsed statement costs more than it saves. */
    private static final int MIN_POOL_CAPACITY = 16;
    /** Fractions of the usable budget, boot state first. */
    private static final double SHARE_BOOT = 0.25;
    /**
     * Fraction of the boot arena set aside for the whole copy-buffer class
     * (per-worker share divides this further).
     * <p>
     * Derivation: the baseline measurement found {@link io.questdb.cutlass.text.CopyImportJob}
     * allocating one {@code cairo.sql.copy.buffer.size} buffer (2 MB default) per shared-pool
     * worker at construction time — on a 32-CPU box that is ~67 MB of native memory held for the
     * life of the process, on a server that may never import a single CSV. 1/64th of the boot
     * arena is the fraction that collapses this item to a few MB at a 256 MB budget, so the total
     * import buffer stays a small, fixed slice of the boot arena rather than scaling with worker
     * count. Re-derive by re-measuring {@code CopyImportJob} construction cost if the stock
     * buffer default or worker-sizing changes.
     */
    private static final double SHARE_COPY_BUFFER = 1.0 / 64;
    private static final double SHARE_NETWORK = 0.10;
    private static final double SHARE_QUERY = 0.25;
    private static final double SHARE_WRITE = 0.40;

    private final long bootArenaBytes;
    private final long budgetBytes;
    private final long connectionBufferSize;
    private final boolean enabled;
    private final long networkArenaBytes;
    private final long o3ColumnMemorySize;
    private final double poolScale;
    private final long queryArenaBytes;
    private final int queryWorkerCount;
    private final long sqlCopyBufferSize;
    private final int workerCount;
    private final long writeArenaBytes;
    private final long writerDataAppendPageSize;

    public MemoryBudget(
            long budgetBytes,
            long fixedOverheadBytes,
            int columnCount,
            int writerCount,
            int cpuCount,
            int connectionCount
    ) {
        this.enabled = budgetBytes > 0;

        if (!enabled) {
            this.bootArenaBytes = -1;
            this.budgetBytes = -1;
            this.connectionBufferSize = -1;
            this.networkArenaBytes = -1;
            this.o3ColumnMemorySize = -1;
            this.poolScale = 1.0;
            this.queryArenaBytes = -1;
            this.queryWorkerCount = -1;
            this.sqlCopyBufferSize = -1;
            this.workerCount = -1;
            this.writeArenaBytes = -1;
            this.writerDataAppendPageSize = -1;
            return;
        }

        this.budgetBytes = budgetBytes;

        final int cols = Math.max(1, columnCount);
        final int writers = Math.max(1, writerCount);
        final int cpus = Math.max(1, cpuCount);
        final int conns = Math.max(1, connectionCount);

        // A budget smaller than its declared overhead is a misconfiguration,
        // but must not produce zero or negative sizes.
        final long usable = Math.max(MIN_PAGE * 1024, budgetBytes - fixedOverheadBytes);

        this.bootArenaBytes = (long) (usable * SHARE_BOOT);
        this.networkArenaBytes = (long) (usable * SHARE_NETWORK);
        this.queryArenaBytes = (long) (usable * SHARE_QUERY);
        this.writeArenaBytes = (long) (usable * SHARE_WRITE);

        // Workers cost thread stacks plus a per-worker copy of several
        // buffers, so they are budgeted before anything that multiplies by
        // them. Roughly 32 MB of budget per worker, capped by real CPUs.
        this.workerCount = Math.max(1, Math.min(cpus, (int) (usable / (32 * 1024 * 1024))));

        // The 67 MB item: one import buffer per worker, allocated at boot.
        // Give the whole class a small slice of the boot arena — this is the
        // single largest fixed native allocation on a server that never
        // imports a CSV, so it must collapse hard under a small budget.
        this.sqlCopyBufferSize = floorToPage((long) (bootArenaBytes * SHARE_COPY_BUFFER) / this.workerCount);

        // Heap-side SQL pools scale with the boot arena against a 256 MB
        // reference point, so a larger budget keeps closer to stock capacity.
        this.poolScale = Math.min(1.0, (double) bootArenaBytes / (64.0 * 1024 * 1024));

        // Budgeted separately from shared workers, and far more tightly: see
        // MEM_PER_QUERY_WORKER for the measurement this comes from. Never more
        // than the shared pool has, and never zero.
        this.queryWorkerCount = Math.max(1, Math.min(this.workerCount,
                (int) (queryArenaBytes / MEM_PER_QUERY_WORKER)));

        this.connectionBufferSize = floorToPage(networkArenaBytes / conns);
        final long perColumn = writeArenaBytes / ((long) cols * writers);
        this.writerDataAppendPageSize = floorToPage(perColumn * 2 / 3);
        this.o3ColumnMemorySize = floorToPage(perColumn / 3);
    }

    public long getBootArenaBytes() {
        return bootArenaBytes;
    }

    public long getBudgetBytes() {
        return budgetBytes;
    }

    public long getConnectionBufferSize() {
        return connectionBufferSize;
    }

    public long getNetworkArenaBytes() {
        return networkArenaBytes;
    }

    public long getO3ColumnMemorySize() {
        return o3ColumnMemorySize;
    }

    /**
     * Default chunk for the group-by allocator. Stock is 128 KB.
     *
     * @see #getGroupByAllocatorMaxChunkSize()
     */
    public long getGroupByAllocatorDefaultChunkSize() {
        return enabled ? floorToPage(queryArenaBytes / 512) : -1;
    }

    /**
     * Ceiling on a single group-by allocator chunk.
     * <p>
     * Stock is <b>4 GiB</b>, which is no ceiling at all under a small budget.
     * Measured: with the stock value a 128 MiB server was killed by
     * {@code double-groupby-all} after 2057 ms — a query returning <b>zero
     * rows</b>, so the memory went on group-by setup rather than results.
     * Bounding this to an eighth of the query arena made the full 15-query TSBS
     * set stable over six consecutive repetitions.
     */
    public long getGroupByAllocatorMaxChunkSize() {
        return enabled ? floorToPage(queryArenaBytes / 8) : -1;
    }

    /**
     * Rows per page frame. Stock is 100,000 min / 1,000,000 max, which sizes a
     * scan's working set for a machine with memory to spare.
     */
    public int getPageFrameMaxRows() {
        return enabled ? Math.max(1000, (int) (queryArenaBytes / 800)) : -1;
    }

    public int getPageFrameMinRows() {
        return enabled ? Math.max(100, getPageFrameMaxRows() / 10) : -1;
    }

    /**
     * SQL small-map pages, stock 32 KB. Note {@code cairo.sql.map.page.size} is
     * NOT derived: it is deprecated and superseded by
     * {@code cairo.sql.small.map.page.size}, so setting it has no effect.
     */
    public long getSmallMapPageSize() {
        return enabled ? floorToPage(queryArenaBytes / 512) : -1;
    }

    /** Open readers each pin mapped column segments. Stock is 10. */
    public int getReaderPoolMaxSegments() {
        return enabled ? Math.max(2, (int) (queryArenaBytes / (16 * 1024 * 1024))) : -1;
    }

    public long getQueryArenaBytes() {
        return queryArenaBytes;
    }

    public long getSqlCopyBufferSize() {
        return sqlCopyBufferSize;
    }

    /**
     * Scales a stock SQL pool capacity to the budget. Returns {@code stockCapacity}
     * unchanged when no budget is declared, so callers can apply this
     * unconditionally to their own default.
     */
    public int getSqlPoolCapacity(int stockCapacity) {
        if (!enabled) {
            return stockCapacity;
        }
        return Math.max(MIN_POOL_CAPACITY, (int) (stockCapacity * poolScale));
    }

    /**
     * Query workers, bounded far more tightly than {@link #getWorkerCount()}
     * because parallel GROUP BY holds one hash map per query worker.
     *
     * @see #MEM_PER_QUERY_WORKER
     */
    public int getQueryWorkerCount() {
        return queryWorkerCount;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public long getWriteArenaBytes() {
        return writeArenaBytes;
    }

    public long getWriterDataAppendPageSize() {
        return writerDataAppendPageSize;
    }

    public boolean isEnabled() {
        return enabled;
    }

    private static long floorToPage(long v) {
        return Math.max(MIN_PAGE, (v / MIN_PAGE) * MIN_PAGE);
    }
}
