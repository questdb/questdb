/*******************************************************************************
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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.WorkStealingStrategy;
import io.questdb.cairo.sql.async.WorkStealingStrategyFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Worker;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.tasks.VectorAggregateTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class GroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final static Log LOG = LogFactory.getLog(GroupByRecordCursorFactory.class);
    private final static int ROSTI_MINIMIZED_SIZE = 16; // 16 is the minimum size usable on arm

    private final RecordCursorFactory base;
    private final RostiRecordCursor cursor;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final PageFrameAddressCache frameAddressCache;
    private final ObjList<PageFrameMemoryPool> frameMemoryPools; // per worker pools
    private final int keyColumnIndex;
    private final AtomicInteger oomCounter = new AtomicInteger();
    private final long[] pRosti;
    private final PerWorkerLocks perWorkerLocks; // used to protect pRosti and VAF's internal slots
    private final RostiAllocFacade raf;
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker; // used to signal cancellation to workers
    private final AtomicInteger startedCounter = new AtomicInteger();
    private final ObjList<VectorAggregateFunction> vafList;
    private final WorkStealingStrategy workStealingStrategy;
    private final int workerCount;

    public GroupByRecordCursorFactory(
            CairoEngine engine,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            @Transient ColumnTypes columnTypes,
            int workerCount,
            @Transient ObjList<VectorAggregateFunction> vafList,
            int keyColumnIndexInBase,
            int keyColumnIndexInThisCursor,
            @Transient @Nullable IntList symbolTableSkewIndex
    ) {
        super(metadata);
        try {
            this.workerCount = workerCount;
            entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
            // columnTypes and functions must align in the following way:
            // columnTypes[0] is the type of key, for now single key is supported
            // functions.size = columnTypes.size - 1, functions do not have instance for key, only for values
            // functions[0].type == columnTypes[1]
            // ...
            // functions[n].type == columnTypes[n+1]

            this.base = base;
            this.frameAddressCache = new PageFrameAddressCache();
            perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
            sharedCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            workStealingStrategy = WorkStealingStrategyFactory.getInstance(configuration, workerCount);
            workStealingStrategy.of(startedCounter);
            // first column is INT or SYMBOL
            pRosti = new long[workerCount];
            final int vafCount = vafList.size();
            this.vafList = new ObjList<>(vafCount);
            this.vafList.addAll(vafList);
            raf = configuration.getRostiAllocFacade();
            for (int i = 0; i < workerCount; i++) {
                long ptr = raf.alloc(columnTypes, configuration.getGroupByMapCapacity());
                if (ptr == 0) {
                    for (int k = i - 1; k > -1; k--) {
                        raf.free(pRosti[k]);
                        pRosti[k] = 0;
                    }
                    throw CairoException.nonCritical()
                            .put("could not allocate rosti hash table")
                            .setOutOfMemory(true);
                }
                pRosti[i] = ptr;

                // remember, single key for now
                switch (ColumnType.tagOf(columnTypes.getColumnType(0))) {
                    case ColumnType.INT:
                        Unsafe.getUnsafe().putInt(Rosti.getInitialValueSlot(pRosti[i], 0), Numbers.INT_NULL);
                        break;
                    case ColumnType.SYMBOL:
                        Unsafe.getUnsafe().putInt(Rosti.getInitialValueSlot(pRosti[i], 0), SymbolTable.VALUE_IS_NULL);
                        break;
                    default:
                }

                // configure map with default values
                // when our execution order is sum(x) then min(y) over the same map
                // min(y) may not find any new keys slots(they will be created by first pass with sum(x))
                // for aggregation function to continue, such slots have to be initialized to the
                // appropriate value for the function.
                for (int j = 0; j < vafCount; j++) {
                    this.vafList.getQuick(j).initRosti(pRosti[i]);
                }
            }

            // all maps are the same at this point
            // check where our keys are and pull them to front
            final long pRosti = this.pRosti[0];
            final long columnOffsets = Rosti.getValueOffsets(pRosti);

            // skew logic assumes single key, for multiple keys skew would be different

            final IntList columnSkewIndex = new IntList();
            // key is in the middle, shift aggregates before the key one position left
            addOffsets(columnSkewIndex, this.vafList, 0, keyColumnIndexInThisCursor, columnOffsets);

            // this is offset of the key column
            columnSkewIndex.add(0);

            // add remaining aggregate columns as is
            addOffsets(columnSkewIndex, this.vafList, keyColumnIndexInThisCursor, vafCount, columnOffsets);

            keyColumnIndex = keyColumnIndexInBase;
            if (symbolTableSkewIndex != null && symbolTableSkewIndex.size() > 0) {
                final IntList symbolSkew = new IntList(symbolTableSkewIndex.size());
                symbolSkew.addAll(symbolTableSkewIndex);
                cursor = new RostiRecordCursor(pRosti, columnTypes.getColumnCount(), columnSkewIndex, symbolSkew);
            } else {
                cursor = new RostiRecordCursor(pRosti, columnTypes.getColumnCount(), columnSkewIndex, null);
            }

            this.frameMemoryPools = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                // We're using page frame memory only and do single scan, hence cache size of 1.
                frameMemoryPools.add(new PageFrameMemoryPool(1));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        oomCounter.set(0);
        // clear maps
        for (int i = 0, n = pRosti.length; i < n; i++) {
            raf.clear(pRosti[i]);
        }
        // clear state of aggregate functions
        for (int i = 0, n = vafList.size(); i < n; i++) {
            vafList.getQuick(i).clear();
        }
        final PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext, ORDER_ASC);
        return cursor.of(
                base.getMetadata(),
                pageFrameCursor,
                executionContext.getMessageBus(),
                executionContext.getCircuitBreaker()
        );
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        final int columnType = getMetadata().getColumnType(columnIndex);
        return columnType == ColumnType.LONG || ColumnType.isTimestamp(columnType);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(true);
        sink.meta("workers").val(workerCount);
        sink.attr("keys").val("[").putBaseColumnName(keyColumnIndex).val("]");
        sink.optAttr("values", vafList, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private static void addOffsets(
            IntList columnSkewIndex,
            @Transient ObjList<VectorAggregateFunction> vafList,
            int start,
            int end,
            long columnOffsets
    ) {
        for (int i = start; i < end; i++) {
            columnSkewIndex.add(Unsafe.getUnsafe().getInt(columnOffsets + vafList.getQuick(i).getValueOffset() * 4L));
        }
    }

    private void resetRostiMemorySize() {
        for (int i = 0, n = pRosti.length; i < n; i++) {
            if (!raf.reset(pRosti[i], ROSTI_MINIMIZED_SIZE)) {
                LOG.debug().$("could not minimize rosti memory [i=").$(i).$(", currentSize=").$(Rosti.getSize(pRosti[i])).I$();
            }
        }
    }

    @Override
    protected void _close() {
        Misc.freeObjListAndKeepObjects(frameMemoryPools);
        Misc.freeObjList(vafList);
        for (int i = 0, n = pRosti.length; i < n; i++) {
            if (pRosti[i] != 0) {
                raf.free(pRosti[i]);
                pRosti[i] = 0;
            }
        }
        Misc.free(base);
    }

    private class RostiRecordCursor implements RecordCursor {
        private final int columnCount;
        private final IntList columnSkewIndex;
        private final RostiRecord record;
        private final IntList symbolTableSkewIndex;
        private MessageBus bus;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long count;
        private long ctrl;
        private long ctrlStart;
        private int frameCount;
        private PageFrameCursor frameCursor;
        private boolean isRostiBuilt;
        private long pRostiBig;
        private RostiRecord recordB;
        private long shift;
        private long size;
        private long slots;

        public RostiRecordCursor(long pRosti, int columnCount, IntList columnSkewIndex, IntList symbolTableSkewIndex) {
            this.pRostiBig = pRosti;
            this.columnCount = columnCount;
            this.record = new RostiRecord(columnCount);
            this.symbolTableSkewIndex = symbolTableSkewIndex;
            this.columnSkewIndex = columnSkewIndex;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildRostiConditionally();

            if (count < size) {
                counter.add(size - count);
                count = size;
            }
        }

        @Override
        public void close() {
            Misc.free(frameAddressCache);
            frameCursor = Misc.free(frameCursor);
            raf.reset(pRostiBig, ROSTI_MINIMIZED_SIZE);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            if (recordB != null) {
                return recordB;
            }
            return (recordB = new RostiRecord(columnCount));
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return frameCursor.getSymbolTable(symbolTableSkewIndex.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            buildRostiConditionally();
            while (count < size) {
                byte b = Unsafe.getUnsafe().getByte(ctrl);
                if ((b & 0x80) != 0) {
                    ctrl++;
                    continue;
                }
                count++;
                record.of(slots + ((ctrl - ctrlStart) << shift));
                ctrl++;
                return true;
            }
            return false;
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            buildRostiConditionally();
            final long offset = columnSkewIndex.getQuick(columnIndex);
            while (count < size) {
                byte b = Unsafe.getUnsafe().getByte(ctrl);
                if ((b & 0x80) != 0) {
                    ctrl++;
                    continue;
                }
                count++;
                final long pRow = slots + ((ctrl - ctrlStart) << shift);
                final long v = Unsafe.getUnsafe().getLong(pRow + offset);
                list.add(pRow, v);
                ctrl++;
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return frameCursor.newSymbolTable(symbolTableSkewIndex.getQuick(columnIndex));
        }

        public RostiRecordCursor of(
                RecordMetadata metadata,
                PageFrameCursor frameCursor,
                MessageBus bus,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            this.frameCursor = frameCursor;
            this.bus = bus;
            this.circuitBreaker = circuitBreaker;
            frameAddressCache.of(metadata, frameCursor.getColumnIndexes(), frameCursor.isExternal());
            for (int i = 0; i < workerCount; i++) {
                frameMemoryPools.getQuick(i).of(frameAddressCache);
            }
            frameCount = 0;
            isRostiBuilt = false;
            return this;
        }

        @Override
        public long preComputedStateSize() {
            return isRostiBuilt ? 1 : 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((RostiRecord) record).of(atRowId);
        }

        @Override
        public long size() {
            return isRostiBuilt ? size : -1;
        }

        @Override
        public void toTop() {
            ctrl = ctrlStart = Rosti.getCtrl(pRostiBig);
            slots = Rosti.getSlots(pRostiBig);
            size = raf.getSize(pRostiBig);
            shift = Rosti.getSlotShift(pRostiBig);
            count = 0;
        }

        private void buildRosti() {
            final int vafCount = vafList.size();
            final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
            final MPSequence pubSeq = bus.getVectorAggregatePubSeq();

            sharedCircuitBreaker.reset();
            startedCounter.set(0);
            doneLatch.reset();
            entryPool.clear();

            int queuedCount = 0;
            int ownCount = 0;
            int reclaimed = 0;
            int total = 0;
            int mergedCount = 0; // used for work stealing decisions

            final Thread thread = Thread.currentThread();
            final int workerId;
            if (thread instanceof Worker) {
                // it's a worker thread, potentially from the shared pool
                workerId = ((Worker) thread).getWorkerId() % workerCount;
            } else {
                // it's an embedder's thread, so use a random slot
                workerId = -1;
            }

            try {
                PageFrame frame;
                while ((frame = frameCursor.next()) != null) {
                    frameAddressCache.add(frameCount++, frame);
                }

                for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                    final long frameRowCount = frameAddressCache.getFrameSize(frameIndex);
                    for (int vafIndex = 0; vafIndex < vafCount; vafIndex++) {
                        final VectorAggregateFunction vaf = vafList.getQuick(vafIndex);
                        // when column index = -1 we assume that vector function does not have value
                        // argument, and it can only derive count via memory size
                        final int valueColumnIndex = vaf.getColumnIndex();

                        while (true) {
                            long cursor = pubSeq.next();
                            if (cursor < 0) {
                                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                                if (workStealingStrategy.shouldSteal(mergedCount)) {
                                    VectorAggregateEntry.aggregateUnsafe(
                                            workerId,
                                            oomCounter,
                                            frameIndex,
                                            frameRowCount,
                                            keyColumnIndex,
                                            valueColumnIndex,
                                            pRosti,
                                            frameMemoryPools,
                                            raf,
                                            vaf,
                                            perWorkerLocks,
                                            circuitBreaker
                                    );
                                    ownCount++;
                                    total++;
                                    mergedCount = doneLatch.getCount();
                                    break;
                                }
                                mergedCount = doneLatch.getCount();
                            } else {
                                final VectorAggregateEntry entry = entryPool.next();
                                entry.of(
                                        frameIndex,
                                        frameRowCount,
                                        keyColumnIndex,
                                        valueColumnIndex,
                                        vaf,
                                        pRosti,
                                        frameMemoryPools,
                                        startedCounter,
                                        doneLatch,
                                        oomCounter,
                                        raf,
                                        perWorkerLocks,
                                        sharedCircuitBreaker
                                );
                                queue.get(cursor).entry = entry;
                                pubSeq.done(cursor);
                                queuedCount++;
                                total++;
                                break;
                            }
                        }
                    }
                }
            } catch (Throwable th) {
                sharedCircuitBreaker.cancel();
                throw th;
            } finally {
                // all done? great start consuming the queue we just published
                // how do we get to the end? If we consume our own queue there is chance we will be consuming
                // aggregation tasks not related to this execution (we work in concurrent environment)
                // To deal with that we need to have our own checklist.

                // Make sure we're consuming jobs even when we failed. We cannot close "rosti" when there are
                // tasks in flight.

                reclaimed = GroupByNotKeyedVectorRecordCursorFactory.runWhatsLeft(
                        bus.getVectorAggregateSubSeq(),
                        queue,
                        queuedCount,
                        reclaimed,
                        mergedCount,
                        workerId,
                        doneLatch,
                        circuitBreaker,
                        sharedCircuitBreaker,
                        workStealingStrategy
                );
                // We can't reallocate rosti until tasks are complete because some other thread could be using it.
                if (sharedCircuitBreaker.checkIfTripped()) {
                    resetRostiMemorySize();
                }
                // Release page frame memory now, when no worker is using it.
                Misc.freeObjListAndKeepObjects(frameMemoryPools);
            }

            if (oomCounter.get() > 0) {
                resetRostiMemorySize();
                throw CairoException.nonCritical()
                        .put("could not resize rosti hash table")
                        .setOutOfMemory(true);
            }

            // merge maps only when cursor was fetched successfully
            // otherwise assume error and save CPU cycles
            pRostiBig = pRosti[0];
            try {
                if (pRosti.length > 1) {
                    LOG.debug().$("merging").$();

                    // due to uneven load distribution some rostis could be much bigger and some empty
                    long size = raf.getSize(pRostiBig);
                    for (int i = 1, n = pRosti.length; i < n; i++) {
                        long curSize = raf.getSize(pRosti[i]);
                        if (curSize > size) {
                            size = curSize;
                            pRostiBig = pRosti[i];
                        }
                    }

                    for (int j = 0; j < vafCount; j++) {
                        final VectorAggregateFunction vaf = vafList.getQuick(j);
                        for (int i = 0, n = pRosti.length; i < n; i++) {
                            if (pRostiBig == pRosti[i] || raf.getSize(pRosti[i]) < 1) {
                                continue;
                            }
                            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                            long oldSize = Rosti.getAllocMemory(pRostiBig);
                            if (!vaf.merge(pRostiBig, pRosti[i])) {
                                resetRostiMemorySize();
                                throw CairoException.nonCritical()
                                        .put("could not merge rosti hash table")
                                        .setOutOfMemory(true);
                            }
                            raf.updateMemoryUsage(pRostiBig, oldSize);
                        }

                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                        // some wrapUp() methods can increase rosti size
                        long oldSize = Rosti.getAllocMemory(pRostiBig);
                        if (!vaf.wrapUp(pRostiBig)) {
                            resetRostiMemorySize();
                            throw CairoException.nonCritical()
                                    .put("could not wrap up rosti hash table")
                                    .setOutOfMemory(true);
                        }
                        raf.updateMemoryUsage(pRostiBig, oldSize);
                    }
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    for (int i = 0, n = pRosti.length; i < n; i++) {
                        if (pRostiBig == pRosti[i]) {
                            continue;
                        }

                        if (!raf.reset(pRosti[i], ROSTI_MINIMIZED_SIZE)) {
                            LOG.debug().$("couldn't minimize rosti memory [i=").$(i).$(",currentSize=").$(Rosti.getSize(pRosti[i])).I$();
                        }
                    }
                } else {
                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                    for (int j = 0; j < vafCount; j++) {
                        if (!vafList.getQuick(j).wrapUp(pRostiBig)) {
                            resetRostiMemorySize();
                            throw CairoException.nonCritical()
                                    .put("could not wrap up rosti hash table")
                                    .setOutOfMemory(true);
                        }
                    }
                }
            } catch (Throwable t) {
                resetRostiMemorySize();
                throw t;
            }

            toTop();

            LOG.info().$("done [total=").$(total)
                    .$(", ownCount=").$(ownCount)
                    .$(", reclaimed=").$(reclaimed)
                    .$(", queuedCount=").$(queuedCount).I$();
        }

        private void buildRostiConditionally() {
            if (!isRostiBuilt) {
                buildRosti();
                isRostiBuilt = true;
            }
        }

        private class RostiRecord implements Record {
            private final ObjList<Long256Impl> longs256A;
            private final ObjList<Long256Impl> longs256B;
            private long pRow;

            public RostiRecord(int columnCount) {
                this.longs256A = new ObjList<>(columnCount);
                this.longs256B = new ObjList<>(columnCount);
            }

            @Override
            public long getDate(int col) {
                return getLong(col);
            }

            @Override
            public double getDouble(int col) {
                return Unsafe.getUnsafe().getDouble(getValueAddress(col));
            }

            @Override
            public float getFloat(int col) {
                return 0;
            }

            @Override
            public byte getGeoByte(int col) {
                return getByte(col);
            }

            @Override
            public int getGeoInt(int col) {
                return getInt(col);
            }

            @Override
            public long getGeoLong(int col) {
                return getLong(col);
            }

            @Override
            public short getGeoShort(int col) {
                return getShort(col);
            }

            @Override
            public int getIPv4(int col) {
                return Unsafe.getUnsafe().getInt(getValueAddress(col));
            }

            @Override
            public int getInt(int col) {
                return Unsafe.getUnsafe().getInt(getValueAddress(col));
            }

            @Override
            public long getLong(int col) {
                return Unsafe.getUnsafe().getLong(getValueAddress(col));
            }

            @Override
            public void getLong256(int col, CharSink<?> sink) {
                Long256Impl v = (Long256Impl) getLong256A(col);
                v.toSink(sink);
            }

            @Override
            public Long256 getLong256A(int col) {
                return getLong256Value(long256A(col), col);
            }

            @Override
            public Long256 getLong256B(int col) {
                return getLong256Value(long256B(col), col);
            }

            public Long256 getLong256Value(Long256 dst, int col) {
                dst.fromAddress(getValueAddress(col));
                return dst;
            }

            @Override
            public long getRowId() {
                return pRow;
            }

            @Override
            public short getShort(int col) {
                return 0;
            }

            @Override
            public CharSequence getStrA(int col) {
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                return null;
            }

            @Override
            public int getStrLen(int col) {
                return 0;
            }

            @Override
            public CharSequence getSymA(int col) {
                return frameCursor.getSymbolTable(symbolTableSkewIndex.getQuick(col)).valueOf(getInt(col));
            }

            @Override
            public CharSequence getSymB(int col) {
                return frameCursor.getSymbolTable(symbolTableSkewIndex.getQuick(col)).valueBOf(getInt(col));
            }

            @Override
            public long getTimestamp(int col) {
                return getLong(col);
            }

            public void of(long pRow) {
                this.pRow = pRow;
            }

            private long getValueAddress(int column) {
                return pRow + columnSkewIndex.getQuick(column);
            }

            private Long256Impl long256A(int columnIndex) {
                if (longs256A.getQuiet(columnIndex) == null) {
                    longs256A.extendAndSet(columnIndex, new Long256Impl());
                }
                return longs256A.getQuick(columnIndex);
            }

            private Long256Impl long256B(int columnIndex) {
                if (longs256B.getQuiet(columnIndex) == null) {
                    longs256B.extendAndSet(columnIndex, new Long256Impl());
                }
                return longs256B.getQuick(columnIndex);
            }
        }
    }
}
