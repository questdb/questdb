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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * The joined rows a parallel hash join has in flight: one entry per probe frame the join may have
 * dispatched and not yet collected, owned by the join's atom rather than by the reduce tasks.
 * <p>
 * A task holds on to the capacity of its lists until the query's last frames, and over a long query
 * a query's tasks cycle through every slot of the shared reduce queue, so lists of joined rows owned
 * by the tasks would keep the queue's capacity times a frame's tuples, untracked. The ring instead
 * keeps {@code R} {@link DirectLongList}s bound to the execution's memory tracker, where {@code R} is
 * the execution's effective dispatch limit:
 * <pre>
 * R = max(1, min(dispatch limit, queue capacity, ring budget / (max probe frame rows * (1 + N) * 8)))
 * </pre>
 * with {@code N} stages, and the ring budget, {@code cairo.sql.parallel.hash.join.tuple.ring.max.size},
 * capped at a quarter of a configured per-query memory limit. The task of frame {@code f} writes
 * entry {@code f % R}, or, with a dynamic window, see below, {@code f} modulo the entry count. That
 * is safe because {@link PageFrameSequence#next(int, boolean)} and
 * {@link PageFrameSequence#dispatchAhead(int)} keep the frames dispatched and not collected by the
 * owner, the frame the owner reads included, to a contiguous window of at most the dispatch limit
 * they receive, and next() reduces a frame locally only when that window is empty. The owner
 * therefore passes {@code R}, or less, to every {@code next()} and {@code dispatchAhead()} that
 * dispatches tuple-producing tasks. Count-only tasks never touch the ring,
 * neither its tuples nor its per-entry state, because a count-only {@code next()} may pass a wider
 * limit, so that count-only frame {@code f + R} can run while the owner still reads entry
 * {@code f % R}.
 * <p>
 * The owner binds the ring with {@link #of(PageFrameSequence)} once per execution, after
 * {@link PageFrameSequence#prepareForDispatch()} and before the first dispatch, and keeps it bound
 * across {@code toTop()}. Binding creates the entries' list objects, so no worker grows the ring's
 * own lists, but allocates no native memory: an entry's memory grows on the thread that first
 * writes it, charged to the execution's tracker. {@link #clear()} frees it all.
 * <p>
 * Each entry also keeps its frame's resume position, -1 when the task joined every row of the
 * frame, and its kept-row count, -1 when the frame had no probe filter, so that a continuation
 * walks row indexes instead of the task's filtered rows.
 * <p>
 * {@code R} sizes the ring for the worst case, every probe row of the largest frame surviving into
 * one tuple, so a selective probe filter or INNER joins that most rows miss would hold the window
 * at {@code R} for memory that the entries never take. A ring bound with a dynamic window, see
 * {@link #of(PageFrameSequence, boolean)}, instead keeps {@code min(dispatch limit, queue capacity)}
 * entries, which take no memory until a task writes them, and moves its window, the limit that
 * {@link #getDispatchLimit()} returns, as the owner reads frames, see {@link #release(int)}: it starts
 * at {@code R}, and each frame read makes it {@code ringBudget / the largest entry read so far},
 * within one and the entry count. Passing a smaller limit to a later {@code next()} is safe, and so
 * is a larger one up to the entry count, since frames in flight then still take distinct entries.
 * While the window holds fewer frames than the ring has entries, an entry larger than its share of
 * the budget gives its memory back once the owner has read it, so that the entries that the frames
 * in flight do not hold keep no more than their shares, after frames that needed more too.
 */
public class HashJoinTupleRing implements Mutable, QuietCloseable {
    private static final int NO_FRAME = -1;
    private final int configuredDispatchLimit;
    private final ObjList<DirectLongList> entries = new ObjList<>();
    // The frame whose tuples each entry holds; reads assert they find the frame they ask for.
    private final IntList entryFrameIndexes = new IntList();
    private final LongList keptRowCounts = new LongList();
    private final int queueCapacity;
    private final LongList resumePositions = new LongList();
    private final long ringMaxSize;
    private final int tupleStride;
    // The window: the dispatch limit the owner passes to next(); 0 while the ring is not bound.
    private int dispatchLimit;
    // The entries the frames map to, frame f to entry f % entryCount; 0 while the ring is not bound.
    private int entryCount;
    // The largest entry, in bytes, that the owner has read since the ring was bound, 0 before.
    private long largestEntryBytes;
    // The bytes the entries may take, see computeRingBudget().
    private long ringBudget;

    /**
     * @param stageCount the joins the operator runs, so that a tuple is a probe row and one entry
     *                   per stage
     */
    public HashJoinTupleRing(CairoConfiguration configuration, int stageCount) {
        assert stageCount > 0;
        this.configuredDispatchLimit = configuration.getSqlParallelFilterDispatchLimit();
        this.queueCapacity = configuration.getPageFrameReduceQueueCapacity();
        this.ringMaxSize = configuration.getSqlParallelHashJoinTupleRingMaxSize();
        this.tupleStride = 1 + stageCount;
    }

    /**
     * The effective dispatch limit {@code R}, the number of ring entries:
     * {@code max(1, min(dispatchLimit, queueCapacity, ringBudget / (maxProbeFrameRows * tupleStride * 8)))}.
     * The floor of one keeps a dispatch limit of 0, which makes every frame reduce locally, from
     * leaving the ring without an entry; a ring budget below one frame's tuples also leaves one
     * entry. Without probe frames the budget bounds nothing.
     *
     * @param dispatchLimit     {@code cairo.sql.parallel.filter.dispatch.limit}
     * @param queueCapacity     the capacity of the reduce queue, which caps the window anyway
     * @param ringBudget        the bytes the entries may take, see {@link #computeRingBudget(long, long)}
     * @param maxProbeFrameRows the rows of the largest probe frame, 0 without frames
     * @param tupleStride       the longs per tuple, 1 + the stage count
     */
    public static int computeDispatchLimit(
            int dispatchLimit,
            int queueCapacity,
            long ringBudget,
            long maxProbeFrameRows,
            int tupleStride
    ) {
        long limit = Math.min(dispatchLimit, queueCapacity);
        if (maxProbeFrameRows > 0) {
            final long rowBytes = (long) tupleStride * Long.BYTES;
            // A frame too large to size in a long fits no budget.
            final long budgetFrames = maxProbeFrameRows > Long.MAX_VALUE / rowBytes
                    ? 0
                    : ringBudget / (maxProbeFrameRows * rowBytes);
            limit = Math.min(limit, budgetFrames);
        }
        return (int) Math.max(1, limit);
    }

    /**
     * The entries of a ring with a dynamic window: as many as the frames that the window may ever
     * hold, {@code max(1, min(dispatchLimit, queueCapacity))}.
     *
     * @param dispatchLimit {@code cairo.sql.parallel.filter.dispatch.limit}
     * @param queueCapacity the capacity of the reduce queue, which caps the window anyway
     */
    public static int computeEntryCount(int dispatchLimit, int queueCapacity) {
        return Math.max(1, Math.min(dispatchLimit, queueCapacity));
    }

    /**
     * The ring budget of an execution: {@code ringMaxSize}, capped at a quarter of the per-query
     * memory limit when one is configured, so that the ring rarely decides whether a query that
     * the serial plan runs fits.
     *
     * @param ringMaxSize      {@code cairo.sql.parallel.hash.join.tuple.ring.max.size}
     * @param queryMemoryLimit the execution's memory limit, 0 when there is none
     */
    public static long computeRingBudget(long ringMaxSize, long queryMemoryLimit) {
        return queryMemoryLimit > 0 ? Math.min(ringMaxSize, queryMemoryLimit / 4) : ringMaxSize;
    }

    /**
     * Frees every entry's memory and unbinds the ring from its execution. The entries' list
     * objects stay for the next execution.
     */
    @Override
    public void clear() {
        for (int i = 0, n = entries.size(); i < n; i++) {
            final DirectLongList entry = entries.getQuick(i);
            entry.close();
            entry.setMemoryTracker(null);
        }
        entryFrameIndexes.clear();
        keptRowCounts.clear();
        resumePositions.clear();
        dispatchLimit = 0;
        entryCount = 0;
        largestEntryBytes = 0;
        ringBudget = 0;
    }

    @Override
    public void close() {
        clear();
        Misc.freeObjListAndClear(entries);
    }

    /**
     * The native memory the entries hold, in bytes.
     */
    public long getAllocatedBytes() {
        long bytes = 0;
        for (int i = 0, n = entries.size(); i < n; i++) {
            bytes += entries.getQuick(i).getCapacity() * Long.BYTES;
        }
        return bytes;
    }

    /**
     * The execution's effective dispatch limit, the window, which the owner passes to every
     * {@code next()} that dispatches tuple-producing tasks: {@code R} when the ring is bound, and,
     * with a dynamic window, what the frames the owner read since make of it, see
     * {@link #release(int)}; 0 while the ring is not bound.
     */
    public int getDispatchLimit() {
        return dispatchLimit;
    }

    /**
     * The entries that the frames map to: {@code R} for a ring bound with a fixed window, and
     * {@link #computeEntryCount(int, int)} for one bound with a dynamic window; 0 while the ring is
     * not bound.
     */
    public int getEntryCount() {
        return entryCount;
    }

    /**
     * The frame's kept-row count: the probe filter's kept rows, or -1 when the frame had no probe
     * filter.
     */
    public long getKeptRowCount(int frameIndex) {
        return keptRowCounts.getQuick(entryOf(frameIndex));
    }

    /**
     * The kept row the frame's continuation resumes at, or -1 when the frame's task joined every
     * kept row.
     */
    public long getResumePosition(int frameIndex) {
        return resumePositions.getQuick(entryOf(frameIndex));
    }

    public int getTupleStride() {
        return tupleStride;
    }

    /**
     * The tuples the frame's task, or its last continuation chunk, wrote: {@link #getTupleStride()}
     * longs each.
     */
    public DirectLongList getTuples(int frameIndex) {
        return entries.getQuick(entryOf(frameIndex));
    }

    /**
     * Binds the ring to an execution of the probe sequence with a fixed window of {@code R}
     * entries, see {@link #of(PageFrameSequence, boolean)}.
     */
    public void of(PageFrameSequence<?> probeSequence) {
        of(probeSequence, false);
    }

    /**
     * Binds the ring to an execution of the probe sequence and sizes it: the sequence has counted
     * its frames, in {@link PageFrameSequence#prepareForDispatch()}, and dispatched none. The
     * entries charge their memory to the sequence's memory tracker. The window starts at
     * {@code R}. With a fixed window the ring keeps {@code R} entries, and the window never grows;
     * with a dynamic one it keeps {@link #computeEntryCount(int, int)} entries, and the frames the
     * owner reads move the window within them, see {@link #release(int)}.
     */
    public void of(PageFrameSequence<?> probeSequence, boolean isWindowDynamic) {
        assert dispatchLimit == 0 : "clear() the ring before binding it to another execution";
        long maxFrameRows = 0;
        for (int i = 0, n = probeSequence.getFrameCount(); i < n; i++) {
            maxFrameRows = Math.max(maxFrameRows, probeSequence.getFrameRowCount(i));
        }
        final MemoryTracker memoryTracker = probeSequence.getMemoryTracker();
        final long ringBudget = computeRingBudget(ringMaxSize, memoryTracker != null ? memoryTracker.getLimit() : 0);
        final int dispatchLimit = computeDispatchLimit(configuredDispatchLimit, queueCapacity, ringBudget, maxFrameRows, tupleStride);
        // The window's bound, R, never exceeds the dispatch limit and the queue capacity.
        final int entryCount = isWindowDynamic ? computeEntryCount(configuredDispatchLimit, queueCapacity) : dispatchLimit;
        assert entryCount >= dispatchLimit;
        for (int i = entries.size(); i < entryCount; i++) {
            // Keep the list closed: its memory is allocated by the first task that writes it.
            entries.add(new DirectLongList(tupleStride, MemoryTag.NATIVE_JOIN_MAP, true));
        }
        for (int i = 0; i < entryCount; i++) {
            entries.getQuick(i).setMemoryTracker(memoryTracker);
        }
        entryFrameIndexes.setAll(entryCount, NO_FRAME);
        keptRowCounts.setAll(entryCount, -1);
        resumePositions.setAll(entryCount, -1);
        this.ringBudget = ringBudget;
        this.entryCount = entryCount;
        this.largestEntryBytes = 0;
        this.dispatchLimit = dispatchLimit;
    }

    /**
     * Hands the frame's entry, emptied, to the task that joins the frame, or to the owner for a
     * continuation chunk, with room for at least {@code tupleCapacity} tuples (at least one), so
     * that {@link DirectLongList#add(long)} grows it from there. An entry with less room, the first
     * open of an entry in an execution among them, gives its memory back and takes exactly that
     * room, on the calling thread, with nothing to copy. A breach of the per-query memory limit
     * throws from here, or from a later append, and leaves the entry's memory to {@link #clear()}.
     */
    public DirectLongList openTuples(int frameIndex, long tupleCapacity) {
        final int entry = frameIndex % entryCount;
        entryFrameIndexes.setQuick(entry, frameIndex);
        final DirectLongList tuples = entries.getQuick(entry);
        tuples.clear();
        final long capacity = Math.max(1, tupleCapacity) * tupleStride;
        if (tuples.getCapacity() < capacity) {
            tuples.close();
        }
        tuples.ensureCapacity(capacity);
        return tuples;
    }

    /**
     * Tells the ring that the owner has read the frame's entry, and will not read it again before
     * it collects the frame. The entry's memory, which the frame's task and continuation chunks
     * grew it to, sets the window, if it is the largest the owner has read since the ring was
     * bound: {@code max(1, min(entry count, ring budget / entry bytes))}. The entry then gives its
     * memory back if it holds more than its share of the budget, {@code ring budget / entry count},
     * while the window holds fewer frames than the ring has entries: the frames cycle through every
     * entry, so the entries that the frames in flight do not hold would otherwise keep, between
     * them, more than the window leaves of the budget. The next frame that maps to the entry takes
     * what it needs.
     * <p>
     * The owner calls it, for a frame whose task has run, before it collects the frame.
     */
    public void release(int frameIndex) {
        final DirectLongList tuples = entries.getQuick(entryOf(frameIndex));
        final long entryBytes = tuples.getCapacity() * Long.BYTES;
        if (entryBytes > largestEntryBytes) {
            largestEntryBytes = entryBytes;
            dispatchLimit = (int) Math.max(1, Math.min(entryCount, ringBudget / entryBytes));
        }
        if (dispatchLimit < entryCount && entryBytes > ringBudget / entryCount) {
            tuples.close();
        }
    }

    /**
     * Records how far the frame's task got: its kept-row count, -1 without a probe filter, and the
     * kept row a continuation resumes at, -1 when the task joined every kept row.
     */
    public void setResumePosition(int frameIndex, long keptRowCount, long resumePosition) {
        final int entry = entryOf(frameIndex);
        keptRowCounts.setQuick(entry, keptRowCount);
        resumePositions.setQuick(entry, resumePosition);
    }

    private int entryOf(int frameIndex) {
        final int entry = frameIndex % entryCount;
        assert entryFrameIndexes.getQuick(entry) == frameIndex
                : "frame " + frameIndex + " shares ring entry " + entry + " with frame " + entryFrameIndexes.getQuick(entry);
        return entry;
    }
}
