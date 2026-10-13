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

package io.questdb.cairo.sql.async;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class PageFrameSequence<T extends StatefulAtom> extends AbstractPageFrameSequence implements Closeable {
    private static final AtomicLong ID_SEQ = new AtomicLong();
    // tests: frame groups of more than one frame dispatched, counted under -ea only
    private static final AtomicLong GROUPS_DISPATCHED = new AtomicLong();
    private static final long LOCAL_TASK_CURSOR = Long.MAX_VALUE;
    // A frame group (see PageFrameReduceTask.getGroupTask()) takes consecutive native frames while
    // their rows stay within GROUP_ROWS, at most MAX_GROUP_FRAMES of them: tiny frames, as a table of
    // many small partitions has, then cost the queue and the ordered collect once per group.
    private static final long GROUP_ROWS = 65_536;
    static final int MAX_GROUP_FRAMES = 16;
    // Group tasks a sequence keeps, cleared, from one execution to the next. A cleared task holds its
    // lists' initial capacities, 2,304 bytes at the defaults, and a closed cursor may keep little
    // native memory (the factory tests allow 64 KiB), so few are kept cleared. The filter dispatch
    // limit does not bound the tasks in flight: it applies to limited iteration only, which with the
    // default limit sends no groups at all, see groupLimit(). An unlimited filter may hold (reduce
    // queue capacity + 1) x (MAX_GROUP_FRAMES - 1) of them, the one being a group's reserved before its
    // slot is claimed: 255 on a 4-worker pool. Keeping them all cleared would keep about 570 KiB per
    // closed cursor; built anew at every execution they cost the query's thread 1.4-2.0 us each
    // (about 0.4 ms of a 6 ms scan of 8,000 partitions at 4 workers, 14 % of its samples at 16).
    private static final int RETAINED_GROUP_TASKS = 8;
    // Group tasks a sequence keeps parked past those: a parked task holds no native memory, only its
    // objects, and gets its lists back from the thread that reduces its next frame, see
    // reserveGroupTasks(). Past both, tasks are freed at reset.
    private static final int PARKED_GROUP_TASKS = 1024;
    // tests: runs before a group task is allocated, see reserveGroupTasks()
    private static volatile Runnable groupTaskAllocationHook;
    // tests: runs after a reduced slot is released and before its frames are counted, see releaseReducedSlot()
    private static volatile Consumer<PageFrameReduceTask> reducedSlotReleaseHook;
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    private final MillisecondClock clock;
    // native frames come without column addresses, which the threads processing them compute, see
    // PageFrameCursor.deferFrameAddresses()
    private final boolean deferFrameAddresses;
    // tiny frames go out in groups, see groupSize(); filters only, whose reducers keep no state across frames
    private final boolean groupFrames;
    private final LongList frameRowCounts = new LongList();
    // the tasks this sequence lends its frame groups for their later frames, see takeGroupTask()
    private final ObjList<PageFrameReduceTask> freeGroupTasks = new ObjList<>();
    private final ObjList<PageFrameReduceTask> groupTasks = new ObjList<>();
    private final PageFrameReduceTaskFactory localTaskFactory;
    private final MessageBus messageBus;
    // the shared query workers the factory was built for; a limited dispatch splits its frames
    // among them, see groupLimit()
    private final int queryWorkerCount;
    private final AtomicInteger reduceFinishedCounter = new AtomicInteger(0);
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final PageFrameReducer reducer;
    private final byte taskType; // PageFrameReduceTask.TYPE_*
    private final WorkStealingStrategy workStealingStrategy;
    public volatile boolean done;
    private T atom;
    private SCSequence collectSubSeq;
    private int collectedFrameIndex = -1;
    private int dispatchStartFrameIndex;
    private PageFrameAddressCache frameAddressCache;
    private int frameCount;
    private PageFrameCursor frameCursor;
    // the collect cursor of a frame group whose frames are being returned, -1 when none, and the
    // group frame the collector stands at
    private long groupCursor = -1;
    private int groupCursorFrame;
    private long id;
    private boolean isClosing;
    private PageFrameMemoryRecord localRecord;
    // Local reduce task used when there is no slots in the queue to dispatch tasks.
    private PageFrameReduceTask localTask;
    // Per-query native memory tracker captured from the owning SqlExecutionContext
    // at workload start. Null when no per-query limit is configured. Workers read
    // this off the task via task.getFrameSequence().getMemoryTracker() to charge
    // their allocations to the active workload.
    private MemoryTracker memoryTracker;
    private boolean readyToDispatch;
    private RingQueue<PageFrameReduceTask> reduceQueue;
    private int shard;
    private SqlExecutionContext sqlExecutionContext;
    private long startTime;
    private boolean uninterruptible;
    // Must be initialized from the original SQL context's circuit breaker before use.
    private SqlExecutionCircuitBreakerWrapper workStealCircuitBreaker;

    /**
     * Constructs a page frame sequence instance. The returned instance takes ownership of the input atom.
     */
    public PageFrameSequence(
            CairoEngine engine,
            CairoConfiguration configuration,
            MessageBus messageBus,
            T atom,
            PageFrameReducer reducer,
            PageFrameReduceTaskFactory localTaskFactory,
            int sharedQueryWorkerCount,
            byte taskType
    ) {
        try {
            this.atom = atom;
            this.frameAddressCache = new PageFrameAddressCache();
            this.messageBus = messageBus;
            this.reducer = reducer;
            this.clock = configuration.getMillisecondClock();
            this.deferFrameAddresses = configuration.isSqlParallelFrameDispatchEnabled();
            this.groupFrames = deferFrameAddresses && taskType == PageFrameReduceTask.TYPE_FILTER;
            this.localTaskFactory = localTaskFactory;
            this.queryWorkerCount = Math.max(1, sharedQueryWorkerCount);
            this.workStealingStrategy = configuration.getFactoryProvider()
                    .getWorkStealingStrategy(configuration, sharedQueryWorkerCount, atom);
            this.taskType = taskType;
            this.workStealCircuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, configuration.getCircuitBreakerConfiguration());
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    /**
     * Waits for frame sequence completion, fetches remaining pieces of the
     * frame sequence from the queues. This method is not thread safe.
     */
    public void await() {
        LOG.debug()
                .$("awaiting completion [shard=").$(shard)
                .$(", id=").$(id)
                .$(", frameCount=").$(frameCount)
                .I$();

        final MCSequence pageFrameReduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean canPark = dispatcher != null && isFiberSuspendable();
        if (groupCursor > -1) {
            // the collector stopped inside a frame group: the group's slot goes with its last frame
            final PageFrameReduceTask task = reduceQueue.get(groupCursor);
            collectedFrameIndex = task.getFrameIndex() + task.getGroupSize() - 1;
            task.collected(true);
            collectSubSeq.done(groupCursor);
            groupCursor = -1;
            groupCursorFrame = 0;
            if (dispatcher != null) {
                dispatcher.signalProgress(this);
            }
        }
        while (!done) {
            // Sampled before the work checks below: a producer that signals progress after the
            // checks but before the sample would otherwise be missed and this fiber would park.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            // First check the local task: maybe we were reducing locally and got interrupted by an exception?
            if (localTask != null && localTask.getFrameSequence() == this && dispatchStartFrameIndex == localTask.getFrameIndex() + 1) {
                collectedFrameIndex = localTask.getFrameIndex();
                localTask.collected(true);
            }

            if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                // We know that all frames were collected. We're almost done.
                if (!done) {
                    // Looks like not all the frames were dispatched, so no one reached the very last frame and
                    // reset the sequence via calling PageFrameReduceTask#collected(). Let's do it ourselves.
                    markAsDone();
                }
                break;
            }

            // We were asked to steal work from the reduce queue and beyond, as much as we can.
            boolean nothingProcessed = true;
            try {
                nothingProcessed = PageFrameReduceJob.consumeQueue(
                        reduceQueue,
                        pageFrameReduceSubSeq,
                        localRecord,
                        workStealCircuitBreaker,
                        this,
                        dispatcher
                );
            } catch (Throwable th) {
                LOG.error()
                        .$("await error [id=").$(id)
                        .$(", ex=").$(th)
                        .I$();
            }

            if (nothingProcessed) {
                long cursor = collectSubSeq.next();
                if (cursor > -1) {
                    // Discard collected items.
                    final PageFrameReduceTask task = reduceQueue.get(cursor);
                    final PageFrameSequence<?> taskFrameSequence = task.getFrameSequence();
                    if (taskFrameSequence == this) {
                        assert id == task.getFrameSequenceId() : "ids mismatch: " + id + ", " + task.getFrameSequenceId();
                        collectedFrameIndex = task.getFrameIndex() + task.getGroupSize() - 1;
                        task.collected(true);
                    }
                    collectSubSeq.done(cursor);
                    if (dispatcher != null) {
                        if (taskFrameSequence != null) {
                            dispatcher.signalProgress(taskFrameSequence);
                        } else {
                            dispatcher.signalProgress();
                        }
                    }
                } else if (canPark) {
                    awaitProgress(dispatcher, observedProgress, observedGlobalProgress, true);
                } else {
                    Os.pause();
                }
            }
        }

        // It could be the case that one of the workers reduced a page frame, then marked the task as done,
        // but haven't incremented reduce counter yet. In this case, we wait for the desired counter value.
        while (true) {
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            if (reduceFinishedCounter.get() == dispatchStartFrameIndex) {
                break;
            }
            if (canPark) {
                awaitProgress(dispatcher, observedProgress, observedGlobalProgress, true);
            } else {
                Os.pause();
            }
        }
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        isClosing = true;
        try {
            reset();
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        final PageFrameMemoryRecord localRecordToFree = localRecord;
        localRecord = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, localRecordToFree);
        final SqlExecutionCircuitBreakerWrapper circuitBreakerToFree = workStealCircuitBreaker;
        workStealCircuitBreaker = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, circuitBreakerToFree);
        final PageFrameReduceTask localTaskToFree = localTask;
        localTask = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, localTaskToFree);
        final T atomToFree = atom;
        atom = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, atomToFree);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    public void collect(long cursor, boolean forceCollect) {
        assert cursor > -1;
        if (cursor == LOCAL_TASK_CURSOR) {
            collectedFrameIndex = localTask.getFrameIndex();
            localTask.collected();
            return;
        }
        PageFrameReduceTask task = reduceQueue.get(cursor);
        if (cursor == groupCursor) {
            collectedFrameIndex = task.getFrameIndex() + groupCursorFrame;
            if (++groupCursorFrame < task.getGroupSize()) {
                // the group's next frame: the same cursor comes back from next()
                return;
            }
            groupCursor = -1;
            groupCursorFrame = 0;
        } else {
            collectedFrameIndex = task.getFrameIndex();
        }
        task.collected(forceCollect);
        collectSubSeq.done(cursor);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        if (dispatcher != null) {
            dispatcher.signalProgress(this);
        }
    }

    public T getAtom() {
        return atom;
    }

    // warning: the circuit breaker may be thread unsafe, so don't use it concurrently
    @Override
    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return sqlExecutionContext.getCircuitBreaker();
    }

    public int getFrameCount() {
        return frameCount;
    }

    public long getFrameRowCount(int frameIndex) {
        return frameRowCounts.getQuick(frameIndex);
    }

    public long getId() {
        return id;
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public PageFrameAddressCache getPageFrameAddressCache() {
        return frameAddressCache;
    }

    /**
     * Frame groups of more than one frame dispatched since the JVM started, counted under -ea only.
     */
    @TestOnly
    public static long getGroupsDispatched() {
        return GROUPS_DISPATCHED.get();
    }

    public AtomicInteger getReduceFinishedCounter() {
        return reduceFinishedCounter;
    }

    public AtomicInteger getReduceStartedCounter() {
        return reduceStartedCounter;
    }

    public PageFrameReducer getReducer() {
        return reducer;
    }

    public int getShard() {
        return shard;
    }

    public SqlExecutionContext getSqlExecutionContext() {
        return sqlExecutionContext;
    }

    public long getStartTime() {
        return startTime;
    }

    public SymbolTableSource getSymbolTableSource() {
        return frameCursor;
    }

    /**
     * Lends a task for a later frame of a frame group, see {@link PageFrameReduceTask#getGroupTask(int)}.
     * The query's thread lends them as it dispatches groups and takes them back as it collects them,
     * so the sequence holds as many as it has group frames in flight. Allocates nothing: the
     * dispatcher reserves the group's tasks before it claims the group's queue slot, see
     * {@link #reserveGroupTasks(int)}, since nothing may throw between the claim and the publish.
     */
    PageFrameReduceTask takeGroupTask() {
        final int n = freeGroupTasks.size();
        assert n > 0 : "group task not reserved";
        final PageFrameReduceTask task = freeGroupTasks.getQuick(n - 1);
        freeGroupTasks.setPos(n - 1);
        return task;
    }

    @TestOnly
    public static void setGroupTaskAllocationHook(Runnable hook) {
        groupTaskAllocationHook = hook;
    }

    /**
     * Sets a hook that runs on the releasing thread after a reduced slot of the ordered reduce queue
     * is released and before its frames are counted, see {@link #releaseReducedSlot}. A test can hold
     * it there until the collector has collected the slot.
     */
    @TestOnly
    public static void setReducedSlotReleaseHook(Consumer<PageFrameReduceTask> hook) {
        reducedSlotReleaseHook = hook;
    }

    void releaseGroupTask(PageFrameReduceTask task) {
        freeGroupTasks.add(task);
    }

    /**
     * Releases a slot of the ordered reduce queue that a reducer took, and counts its frames. Every
     * path that takes a slot from the ordered queue and is done with it, reduced or not, ends here:
     * the worker and stealing reduce, the fiber reduce, the failed fiber acquisition and the
     * dispatcher's drain.
     * <p>
     * The slot's frame count is read before {@code done()}. Once the slot is released, the collector
     * may collect it, which resets its group size to 1, or dispatch it again with another group. A
     * finished counter short of the dispatched frames, or past them, leaves {@link #await()} spinning.
     *
     * @param task          the slot's task, null when unknown, which counts as one frame
     * @param frameSequence the sequence that dispatched the slot, read before the release too
     * @param dispatcher    the fiber dispatcher to signal, or null
     */
    static void releaseReducedSlot(
            MCSequence subSeq,
            long cursor,
            @Nullable PageFrameReduceTask task,
            PageFrameSequence<?> frameSequence,
            @Nullable PageFrameReduceDispatcher dispatcher
    ) {
        final int taskFrames = task != null ? task.getGroupSize() : 1;
        try {
            subSeq.done(cursor);
        } finally {
            try {
                final Consumer<PageFrameReduceTask> hook = reducedSlotReleaseHook;
                if (hook != null && task != null) {
                    hook.accept(task);
                }
            } finally {
                // counted only once the slot is available to the collector; it counts frames
                frameSequence.getReduceFinishedCounter().addAndGet(taskFrames);
                if (dispatcher != null) {
                    dispatcher.signalProgress(frameSequence);
                }
            }
        }
    }

    public PageFrameReduceTask getTask(long cursor) {
        assert cursor > -1;
        if (cursor == LOCAL_TASK_CURSOR) {
            assert localTask != null && localTask.getFrameSequence() != null;
            return localTask;
        }
        final PageFrameReduceTask task = reduceQueue.get(cursor);
        return cursor == groupCursor ? task.getGroupTask(groupCursorFrame) : task;
    }

    public byte getTaskType() {
        return taskType;
    }

    @Override
    public boolean isUninterruptible() {
        return uninterruptible;
    }

    public void markAsDone() {
        // prepare to resend the same sequence as it might be required by toTop()
        assert !done;
        done = true;
    }

    public long next() {
        return next(Integer.MAX_VALUE, false);
    }

    /**
     * This method is not thread safe. It's always invoked on a single "query owner" thread.
     * <p>
     * Returns a cursor that points either to the reduce queue or to the local reduce task.
     * The caller of this method should avoid accessing the reduce queue directly and,
     * instead, should use getTask and collect methods. <code>Long.MAX_VALUE</code> is the
     * reserved cursor value for the local reduce task case.
     *
     * @param dispatchLimit a cap for the number of in-flight tasks
     * @param countOnly     count-only task flag; used only for filter-only tasks
     * @return the next cursor value, or -1 value if the cursor failed and the caller
     * should retry, or -2 if there are no frames to dispatch
     */
    public long next(int dispatchLimit, boolean countOnly) {
        if (frameCount == 0) {
            return -2;
        }

        assert collectedFrameIndex < frameCount - 1;
        if (groupCursor > -1) {
            // a frame group's frames come back one by one, see collect()
            return groupCursor;
        }
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean canPark = dispatcher != null && isFiberSuspendable();
        while (true) {
            // Sampled before collectSubSeq.next() so a producer signalling progress between the
            // failed collect and the sample cannot be missed.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            long cursor = collectSubSeq.next();
            if (cursor > -1) {
                PageFrameReduceTask task = reduceQueue.get(cursor);
                PageFrameSequence<?> thatFrameSequence = task.getFrameSequence();
                if (thatFrameSequence == this) {
                    assert id == task.getFrameSequenceId() : "ids mismatch: " + id + ", " + task.getFrameSequenceId();
                    if (task.getGroupSize() > 1) {
                        groupCursor = cursor;
                        groupCursorFrame = 0;
                    }
                    return cursor;
                } else {
                    // Not our task, nothing to collect. Go for another spin.
                    collectSubSeq.done(cursor);
                    if (dispatcher != null) {
                        if (thatFrameSequence != null) {
                            dispatcher.signalProgress(thatFrameSequence);
                        } else {
                            dispatcher.signalProgress();
                        }
                    }
                }
            } else if (cursor == -1) {
                if (dispatcher != null && !isActive()) {
                    if (getCancelReason() != SqlExecutionCircuitBreaker.STATE_OK) {
                        throw buildInterruptionException();
                    }
                    if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                        return -2;
                    }
                } else if (dispatch(dispatchLimit, countOnly)) {
                    // We have dispatched something, so let's try to collect it.
                    continue;
                }
                if (dispatcher != null) {
                    if (dispatcher.isCurrentFiberOwned()
                            && dispatchStartFrameIndex == collectedFrameIndex + 1) {
                        reduceLocally(countOnly);
                        return LOCAL_TASK_CURSOR;
                    }
                    // Park only while one of our own frames is in flight: its completion is the
                    // progress we wait for. With nothing in flight, the full ring may be held by
                    // another sequence that this same fiber drives (e.g. the other side of an
                    // ASOF Light join over two async filters), and only this fiber can release
                    // it. Parking would then wait for the query timeout, so fall through to the
                    // local reduce below instead.
                    if (canPark && dispatchStartFrameIndex > collectedFrameIndex + 1) {
                        final boolean isDraining = !isActive();
                        awaitProgress(dispatcher, observedProgress, observedGlobalProgress, isDraining);
                        continue;
                    }
                    if (!isActive()) {
                        return -1;
                    }
                }
                if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                    reduceLocally(countOnly);
                    return LOCAL_TASK_CURSOR;
                }
                return -1;
            } else {
                Os.pause();
            }
        }
    }

    public PageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq,
            int order
    ) throws SqlException {
        sqlExecutionContext = executionContext;
        memoryTracker = executionContext.getMemoryTracker();
        startTime = clock.getTicks();
        uninterruptible = executionContext.isUninterruptible();

        if (localRecord == null) {
            localRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        }

        final Rnd rnd = executionContext.getAsyncRandom();
        try {
            assert frameCursor == null;
            frameCursor = base.getPageFrameCursor(executionContext, order);
            if (deferFrameAddresses) {
                // every frame is read through the address cache and memory pools, which fill it in
                frameCursor.deferFrameAddresses();
            }

            // pass one to cache page addresses
            // this has to be separate pass to ensure there no cache reads
            // while cache might be resizing
            frameAddressCache.of(base.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());

            this.collectSubSeq = collectSubSeq;
            id = ID_SEQ.incrementAndGet();
            done = false;
            resetCancellation();
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            shard = rnd.nextInt(messageBus.getPageFrameReduceShardCount());
            reduceQueue = messageBus.getPageFrameReduceQueue(shard);

            // It is essential to init the atom after we prepared sequence for dispatch.
            // If atom is to fail, we will be releasing whatever we prepared.
            atom.init(frameCursor, executionContext);
        } catch (TableReferenceOutOfDateException e) {
            frameCursor = Misc.freeIfCloseable(frameCursor);
            throw e;
        } catch (Throwable th) {
            // Log the OG exception as the below frame cursor close call may throw.
            LOG.error().$("could not initialize page frame sequence [error=").$(th).I$();
            frameCursor = Misc.free(frameCursor);
            throw th;
        }
        return this;
    }

    /**
     * Must be called before subsequence calls to {@link #next(int, boolean)} to count page frames and
     * initialize page frame cache and filter functions.
     */
    public void prepareForDispatch() {
        if (!readyToDispatch) {
            buildAddressCache();
            readyToDispatch = true;
        }
    }

    public void reset() {
        // reset() must be called only if there are no tasks in progress for this page frame sequence
        assert frameCount == 0 || reduceFinishedCounter.get() == dispatchStartFrameIndex;

        // prepare different frame sequence using the same object instance
        frameCount = 0;
        dispatchStartFrameIndex = 0;
        collectedFrameIndex = -1;
        groupCursor = -1;
        groupCursorFrame = 0;
        readyToDispatch = false;
        // Drop the borrowed tracker reference; the provider owns the native block.
        memoryTracker = null;
        frameRowCounts.clear();

        Throwable cleanupFailure = null;
        try {
            if (atom != null) {
                atom.clear();
            }
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        // Unfreeze the covered posting readers frozen at dispatch BEFORE the
        // address cache (which holds them) and the frame cursor (which owns them)
        // are torn down. reset() runs after the sequence has been awaited (see the
        // close() paths of the async cursors, which call await() then reset()), so
        // every worker cursor has finished and the unfreeze is race-free. A reader
        // left frozen would make its reloadConditionally() a permanent no-op and
        // break the next query against the same partition.
        final PageFrameAddressCache frameAddressCacheToFree = frameAddressCache;
        if (isClosing) {
            frameAddressCache = null;
        }
        if (frameAddressCacheToFree != null) {
            try {
                frameAddressCacheToFree.unfreezeCoveredReaders();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameAddressCacheToFree);
        final PageFrameCursor frameCursorToFree = frameCursor;
        frameCursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameCursorToFree);
        // collect sequence may not be set here when
        // factory is closed without using cursor
        final SCSequence collectSubSeqToRemove = collectSubSeq;
        collectSubSeq = null;
        if (collectSubSeqToRemove != null) {
            try {
                messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeqToRemove);
                final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
                if (dispatcher != null) {
                    dispatcher.signalProgress();
                }
                LOG.debug().$("removed [seq=").$(collectSubSeqToRemove).I$();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        if (localTask != null) {
            try {
                localTask.clear();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        // Every group has been collected by now, which gave its tasks back. A few stay for the next
        // execution, cleared to their initial capacities as the local task is; the others are freed,
        // so that a closed cursor keeps little of their memory. A closing sequence keeps none.
        assert freeGroupTasks.size() == groupTasks.size();
        freeGroupTasks.clear();
        int kept = 0;
        for (int i = 0, n = groupTasks.size(); i < n; i++) {
            final PageFrameReduceTask task = groupTasks.getQuick(i);
            if (!isClosing && (kept < RETAINED_GROUP_TASKS || kept < RETAINED_GROUP_TASKS + PARKED_GROUP_TASKS)) {
                try {
                    if (kept < RETAINED_GROUP_TASKS) {
                        task.clear();
                    } else {
                        task.park();
                    }
                    groupTasks.setQuick(kept++, task);
                    freeGroupTasks.add(task);
                    continue;
                } catch (Throwable th) {
                    cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
                }
            }
            cleanupFailure = Misc.freeBestEffort(cleanupFailure, task);
            groupTasks.setQuick(i, null);
        }
        groupTasks.setPos(kept);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Prepares page frame sequence for retrieving the same data set again. The method
     * is not thread-safe.
     */
    public void toTop() {
        if (frameCount > 0) {
            long newId = ID_SEQ.incrementAndGet();
            LOG.debug()
                    .$("toTop [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", newId=").$(newId)
                    .I$();

            await();

            // done is reset by method call above
            done = false;
            id = newId;
            dispatchStartFrameIndex = 0;
            collectedFrameIndex = -1;
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            resetCancellation();
        }
    }

    private void buildAddressCache() {
        PageFrame frame;
        if (deferFrameAddresses) {
            // sized once for the frames the cursor expects, see PageFrameAddressCache.reserveFrames()
            frameAddressCache.reserveFrames(frameCursor.getFrameCountHint());
        }
        while ((frame = frameCursor.next()) != null) {
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameAddressCache.add(frameCount++, frame);
        }

        // Covered frames decode their columns on the async workers (in
        // PageFrameMemoryPool.navigateTo) by iterating detached cursors over the
        // shared per-partition posting readers. That is only race-free if the
        // readers are positioned at the query txn, cache-warm, and FROZEN before
        // any worker decodes. The eager production decode above (driven through
        // frameAddressCache.add -> the covering page-frame cursor) already
        // positioned + warmed each reader as a side effect of its full iteration,
        // so freeze them now, before dispatch. unfreezeCoveredReaders() in reset()
        // reverses it once the sequence has been awaited.
        frameAddressCache.freezeCoveredReaders();

        // dispatch tasks only if there is anything to dispatch
        if (frameCount > 0) {
            // We need to subscribe publisher sequence before we return
            // control to the caller of this method. However, this sequence
            // will be unsubscribed asynchronously.
            messageBus.getPageFrameCollectFanOut(shard).and(collectSubSeq);
            LOG.debug()
                    .$("added [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", seqCurrent=").$(collectSubSeq.current())
                    .$(", seq=").$(collectSubSeq)
                    .I$();
        }
    }

    /**
     * This method is re-enterable. It has to be in case queue capacity or the dispatch limit is smaller
     * than number of frames to be dispatched. When it is the case, frame count published so far is
     * stored in the `dispatchStartFrameIndex` field. This method has no responsibility to deal with
     * "collect" stage hence it deals with everything to unblock the collect stage.
     *
     * @param dispatchLimit a cap for the number of in-flight tasks
     * @param countOnly     count-only task flag; used only for filter-only tasks
     * @return true if at least one task was dispatched or reduced; false otherwise
     */
    private boolean dispatch(int dispatchLimit, boolean countOnly) {
        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        return dispatch0(dispatchLimit, countOnly, dispatcher, reduceSubSeq, reducePubSeq);
    }

    private boolean dispatch0(
            int dispatchLimit,
            boolean countOnly,
            PageFrameReduceDispatcher dispatcher,
            MCSequence reduceSubSeq,
            MPSequence reducePubSeq
    ) {
        boolean hasPublication = dispatcher == null || dispatcher.tryAcquirePublication();
        boolean idle = true;
        boolean dispatched = false;
        final int collectedFrameCount = collectedFrameIndex + 1;
        int groupSize = 1;

        try {
            if (!hasPublication) {
                if (!dispatcher.isCurrentFiberOwned()) {
                    cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                }
                return false;
            }
            long cursor;
            int i = dispatchStartFrameIndex;
            OUT:
            for (; i < frameCount; i++) {
                // We cannot process work on this thread. If we do the consumer will
                // never get the executions results. Consumer only picks ready to go
                // tasks from the queue.

                while (true) {
                    final int totalDispatched = dispatchStartFrameIndex - collectedFrameCount;
                    // Treat situation when we hit the dispatch limit as if it was a full queue (-1).
                    if (totalDispatched >= dispatchLimit) {
                        cursor = -1;
                    } else {
                        // The group's tasks are in hand before its slot is claimed: an allocation
                        // failure between next() and done() would leave the slot claimed and never
                        // published, and stall every consumer of the shard's queue.
                        groupSize = groupSize(i, Math.min(dispatchLimit - totalDispatched, groupLimit(dispatchLimit)));
                        reserveGroupTasks(groupSize - 1);
                        cursor = reducePubSeq.next();
                        if (cursor > -1) {
                            assert groupSize == 1 || GROUPS_DISPATCHED.incrementAndGet() > 0;
                            reduceQueue.get(cursor).of(this, i, groupSize, countOnly);
                            reducePubSeq.done(cursor);
                        }
                    }
                    if (cursor > -1) {
                        LOG.debug()
                                .$("dispatched [shard=").$(shard)
                                .$(", id=").$(getId())
                                .$(", frameIndex=").$(i)
                                .$(", groupSize=").$(groupSize)
                                .$(", frameCount=").$(frameCount)
                                .$(", cursor=").$(cursor)
                                .I$();
                        i += groupSize - 1;
                        dispatchStartFrameIndex = i + 1;
                        dispatched = true;
                        break;
                    } else if (cursor == -1) {
                        if (!workStealingStrategy.shouldSteal(collectedFrameCount)) {
                            return dispatched;
                        }
                        // start stealing work to unload the queue
                        idle = false;
                        if (dispatcher != null) {
                            dispatcher.releasePublication();
                            hasPublication = false;
                        }
                        if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                            if (reduceFinishedCounter.get() > collectedFrameCount) {
                                // We have something to collect, so let's do it!
                                return true;
                            }
                            if (dispatcher != null) {
                                hasPublication = dispatcher.tryAcquirePublication();
                                if (!hasPublication) {
                                    if (!dispatcher.isCurrentFiberOwned()) {
                                        cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                                    }
                                    return dispatched;
                                }
                            }
                            continue;
                        }
                        break OUT;
                    } else {
                        Os.pause();
                    }
                }
            }

            if (dispatcher != null && hasPublication) {
                dispatcher.releasePublication();
                hasPublication = false;
            }

            if (reduceFinishedCounter.get() > collectedFrameCount) {
                // We have something to collect, so let's do it!
                return true;
            }

            // Reduce counter is here to provide safe backoff point
            // for job stealing code. It is needed because queue is shared
            // and there is possibility of never ending stealing if we don't
            // specifically count only our items

            // join the gang to consume published tasks
            while (reduceFinishedCounter.get() < dispatchStartFrameIndex) {
                idle = false;
                if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                    if (isActive()) {
                        continue;
                    }
                }
                break;
            }

            if (idle) {
                stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker);
            }

            return dispatched;
        } finally {
            if (dispatcher != null && hasPublication) {
                dispatcher.releasePublication();
            }
        }
    }

    // Makes sure that n group tasks are free to lend, allocating what is missing. The dispatcher calls it
    // before it claims a queue slot, see dispatch0(); a task left over after a failure stays free.
    private void reserveGroupTasks(int n) {
        // A parked task is lent as it is: lending it allocates nothing, and the thread that
        // reduces its frame gives it its lists back (PageFrameReduceJob), off the query's thread.
        while (freeGroupTasks.size() < n) {
            final Runnable hook = groupTaskAllocationHook;
            if (hook != null) {
                hook.run();
            }
            final PageFrameReduceTask task = localTaskFactory.getInstance();
            try {
                task.setTaskType(taskType);
                groupTasks.add(task);
            } catch (Throwable th) {
                Misc.free(task);
                throw th;
            }
            freeGroupTasks.add(task);
        }
    }

    // The most frames one group may take under a dispatch limit. A limited iteration (LIMIT, negative
    // LIMIT, expectLimitedIteration()) caps the frames in flight, by default at the query worker count.
    // A group that took them all would leave one slot in flight, one worker busy, and the collector
    // walking that group while nothing else is dispatched. So the limit is split among the workers:
    // with the default limit every slot carries one frame, as without groups, and a larger limit
    // still groups. An unlimited dispatch (Integer.MAX_VALUE) is capped by MAX_GROUP_FRAMES alone.
    private int groupLimit(int dispatchLimit) {
        return Math.max(1, dispatchLimit / queryWorkerCount);
    }

    // The frames the next task takes from frameIndex on: a group of small native frames, see
    // GROUP_ROWS, or the frame alone.
    private int groupSize(int frameIndex, int frameLimit) {
        if (!groupFrames) {
            return 1;
        }
        final int maxFrames = Math.min(Math.min(MAX_GROUP_FRAMES, frameLimit), frameCount - frameIndex);
        long rows = 0;
        int n = 0;
        while (n < maxFrames) {
            final int f = frameIndex + n;
            if (frameAddressCache.getFrameFormat(f) != PartitionFormat.NATIVE || frameAddressCache.isFrameCovered(f)) {
                break;
            }
            rows += frameRowCounts.getQuick(f);
            if (n > 0 && rows > GROUP_ROWS) {
                break;
            }
            n++;
        }
        return Math.max(1, n);
    }

    private void reduceLocally(boolean countOnly) {
        assert dispatchStartFrameIndex < frameCount;

        if (localTask == null) {
            localTask = localTaskFactory.getInstance();
            localTask.setTaskType(taskType);
        }
        localTask.of(this, dispatchStartFrameIndex++, countOnly);

        final boolean isFiberSuspendable = isFiberSuspendable();
        final SuspensionScope.CarrierScope suspensionScope = isFiberSuspendable
                ? null
                : SuspensionScope.scope();
        final SuspensionScope.Mode previousMode = isFiberSuspendable
                ? null
                : SuspensionScope.enterBlocking(suspensionScope);
        final FiberCancellationSignal previousCancellationSignal = isFiberSuspendable
                ? SuspensionScope.getCancellationSignal()
                : null;
        final long previousCancellationSignalGeneration = isFiberSuspendable
                ? SuspensionScope.getCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        final FiberCancellationSignal previousSupplementalCancellationSignal = isFiberSuspendable
                ? SuspensionScope.getSupplementalCancellationSignal()
                : null;
        final long previousSupplementalCancellationSignalGeneration = isFiberSuspendable
                ? SuspensionScope.getSupplementalCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        if (isFiberSuspendable) {
            enterReducerCancellationScope();
        }
        try {
            LOG.debug()
                    .$("reducing locally [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", taskType=").$(taskType)
                    .$(", frameIndex=").$(localTask.getFrameIndex())
                    .$(", frameCount=").$(frameCount)
                    .$(", active=").$(isActive())
                    .I$();
            if (isActive()) {
                workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());
                PageFrameReduceJob.reduce(localRecord, workStealCircuitBreaker, localTask, this, this);
            }
        } catch (Throwable th) {
            if (isReducerFailureReportable(th)) {
                LOG.error()
                        .$("local reduce error [error=").$(th)
                        .$(", id=").$(id)
                        .$(", taskType=").$(taskType)
                        .$(", frameIndex=").$(localTask.getFrameIndex())
                        .$(", frameCount=").$(frameCount)
                        .I$();
                // Route the error through the local task so the collector sees it via
                // task.hasError() and can re-throw the original class via task.buildError().
                // Re-throwing here would let the outer catch in the collector wrap the
                // typed exception into a generic CairoException, losing the original class.
                localTask.setErrorMsg(th);
                cancelOnReducerError(th);
            }
        } finally {
            if (isFiberSuspendable) {
                SuspensionScope.restoreCancellationSignal(
                        previousCancellationSignal,
                        previousCancellationSignalGeneration
                );
                SuspensionScope.enterSupplementalCancellationSignal(
                        previousSupplementalCancellationSignal,
                        previousSupplementalCancellationSignalGeneration
                );
            } else {
                SuspensionScope.restoreMode(suspensionScope, previousMode);
            }
            reduceFinishedCounter.incrementAndGet();
        }
    }

    private boolean stealWork(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            PageFrameMemoryRecord record,
            SqlExecutionCircuitBreakerWrapper circuitBreaker
    ) {
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean isEmpty = PageFrameReduceJob.consumeQueue(
                queue,
                reduceSubSeq,
                record,
                circuitBreaker,
                this,
                dispatcher
        );
        if (isEmpty) {
            Os.pause();
            return false;
        }
        return true;
    }
}
