/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
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
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.CharSinkBase;
import io.questdb.tasks.VectorAggregateTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class GroupByRecordCursorFactory extends AbstractRecordCursorFactory {

    private final static Log LOG = LogFactory.getLog(GroupByRecordCursorFactory.class);

    private final static int ROSTI_MINIMIZED_SIZE = 16; // 16 is the minimum size usable on arm
    private final RecordCursorFactory base;
    private final RostiRecordCursor cursor;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final int keyColumnIndex;
    private final AtomicInteger oomCounter = new AtomicInteger();
    private final long[] pRosti;
    private final PerWorkerLocks perWorkerLocks; // used to protect pRosti and VAF's internal slots
    private final RostiAllocFacade raf;
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker; // used to signal cancellation to workers
    private final ObjList<VectorAggregateFunction> vafList;
    private final int workerCount;

    public GroupByRecordCursorFactory(
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
        this.workerCount = workerCount;
        entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
        // columnTypes and functions must align in the following way:
        // columnTypes[0] is the type of key, for now single key is supported
        // functions.size = columnTypes.size - 1, functions do not have instance for key, only for values
        // functions[0].type == columnTypes[1]
        // ...
        // functions[n].type == columnTypes[n+1]

        perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        sharedCircuitBreaker = new AtomicBooleanCircuitBreaker();
        this.base = base;
        // first column is INT or SYMBOL
        pRosti = new long[workerCount];
        final int vafCount = vafList.size();
        this.vafList = new ObjList<>(vafCount);
        raf = configuration.getRostiAllocFacade();
        for (int i = 0; i < workerCount; i++) {
            long ptr = raf.alloc(columnTypes, configuration.getGroupByMapCapacity());
            if (ptr == 0) {
                for (int k = i - 1; k > -1; k--) {
                    raf.free(pRosti[k]);
                }
                throw new OutOfMemoryError();
            }
            pRosti[i] = ptr;

            // remember, single key for now
            switch (ColumnType.tagOf(columnTypes.getColumnType(0))) {
                case ColumnType.INT:
                    Unsafe.getUnsafe().putInt(Rosti.getInitialValueSlot(pRosti[i], 0), Numbers.INT_NaN);
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
                vafList.getQuick(j).initRosti(pRosti[i]);
            }
        }

        // all maps are the same at this point
        // check where our keys are and pull them to front
        final long pRosti = this.pRosti[0];
        final long columnOffsets = Rosti.getValueOffsets(pRosti);

        // skew logic assumes single key, for multiple keys skew would be different

        final IntList columnSkewIndex = new IntList();
        // key is in the middle, shift aggregates before the key one position left
        addOffsets(columnSkewIndex, vafList, 0, keyColumnIndexInThisCursor, columnOffsets);

        // this is offset of the key column
        columnSkewIndex.add(0);

        // add remaining aggregate columns as is
        addOffsets(columnSkewIndex, vafList, keyColumnIndexInThisCursor, vafCount, columnOffsets);

        this.vafList.addAll(vafList);
        keyColumnIndex = keyColumnIndexInBase;
        if (symbolTableSkewIndex != null && symbolTableSkewIndex.size() > 0) {
            final IntList symbolSkew = new IntList(symbolTableSkewIndex.size());
            symbolSkew.addAll(symbolTableSkewIndex);
            cursor = new RostiRecordCursor(pRosti, columnSkewIndex, symbolSkew);
        } else {
            cursor = new RostiRecordCursor(pRosti, columnSkewIndex, null);
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
        return cursor.of(pageFrameCursor, executionContext.getMessageBus(), executionContext.getCircuitBreaker());
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
        sink.attr("keys").val("[").putBaseColumnNameNoRemap(keyColumnIndex).val("]");
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
                LOG.debug().$("Couldn't minimize rosti memory [i=").$(i).$(",current_size=").$(Rosti.getSize(pRosti[i])).I$();
            }
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.freeObjList(vafList);
        for (int i = 0, n = pRosti.length; i < n; i++) {
            raf.free(pRosti[i]);
        }
    }

    private class RostiRecordCursor implements RecordCursor {
        private final IntList columnSkewIndex;
        private final RostiRecord record;
        private final IntList symbolTableSkewIndex;
        private MessageBus bus;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long count;
        private long ctrl;
        private long ctrlStart;
        private boolean isRostiBuilt;
        private long pRostiBig;
        private PageFrameCursor pageFrameCursor;
        private RostiRecord recordB;
        private long shift;
        private long size;
        private long slots;

        public RostiRecordCursor(long pRosti, IntList columnSkewIndex, IntList symbolTableSkewIndex) {
            pRostiBig = pRosti;
            record = new RostiRecord();
            this.symbolTableSkewIndex = symbolTableSkewIndex;
            this.columnSkewIndex = columnSkewIndex;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isRostiBuilt) {
                buildRosti();
                isRostiBuilt = true;
            }

            if (count < size) {
                counter.add(size - count);
                count = size;
            }
        }

        @Override
        public void close() {
            Misc.free(pageFrameCursor);
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
            return (recordB = new RostiRecord());
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return pageFrameCursor.getSymbolTable(symbolTableSkewIndex.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            if (!isRostiBuilt) {
                buildRosti();
                isRostiBuilt = true;
            }
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
        public SymbolTable newSymbolTable(int columnIndex) {
            return pageFrameCursor.newSymbolTable(symbolTableSkewIndex.getQuick(columnIndex));
        }

        public RostiRecordCursor of(PageFrameCursor pageFrameCursor, MessageBus bus, SqlExecutionCircuitBreaker circuitBreaker) {
            this.pageFrameCursor = pageFrameCursor;
            this.bus = bus;
            this.circuitBreaker = circuitBreaker;
            isRostiBuilt = false;
            return this;
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
            entryPool.clear();
            int queuedCount = 0;
            int ownCount = 0;
            int reclaimed = 0;
            int total = 0;

            doneLatch.reset();

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
                while ((frame = pageFrameCursor.next()) != null) {
                    final long keyAddress = frame.getPageAddress(keyColumnIndex);
                    for (int i = 0; i < vafCount; i++) {
                        final VectorAggregateFunction vaf = vafList.getQuick(i);
                        // when column index = -1 we assume that vector function does not have value
                        // argument, and it can only derive count via memory size
                        final int columnIndex = vaf.getColumnIndex();
                        // for functions like `count()`, that do not have arguments we are required to provide
                        // count of rows in table in a form of "pageSize >> shr". Since `vaf` doesn't provide column
                        // this code used column 0. Assumption here that column 0 is fixed size.
                        // This assumption only holds because our aggressive algorithm for "top down columns", e.g.
                        // the algorithm that forces page frame to provide only columns required by the select. At the time
                        // of writing this code there is no way to return variable length column out of non-keyed aggregation
                        // query. This might change if we introduce something like `first(string)`. When this happens we will
                        // need to rethink our way of computing size for the count. This would be either type checking column
                        // 0 and working out size differently or finding any fixed-size column and using that.
                        final long valueAddress = columnIndex > -1 ? frame.getPageAddress(columnIndex) : 0;
                        final int pageColIndex = columnIndex > -1 ? columnIndex : 0;
                        final int columnSizeShr = frame.getColumnShiftBits(pageColIndex);
                        final long valueAddressSize = frame.getPageSize(pageColIndex);

                        long cursor = pubSeq.next();
                        if (cursor < 0) {
                            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                            // acquire the slot and DIY the func
                            final int slot = perWorkerLocks.acquireSlot(workerId, circuitBreaker);
                            try {
                                if (keyAddress == 0) {
                                    vaf.aggregate(valueAddress, valueAddressSize, columnSizeShr, slot);
                                } else {
                                    long oldSize = Rosti.getAllocMemory(pRosti[slot]);
                                    if (!vaf.aggregate(pRosti[slot], keyAddress, valueAddress, valueAddressSize, columnSizeShr, slot)) {
                                        oomCounter.incrementAndGet();
                                    }
                                    raf.updateMemoryUsage(pRosti[slot], oldSize);
                                }
                                ownCount++;
                            } finally {
                                perWorkerLocks.releaseSlot(slot);
                            }
                        } else {
                            final VectorAggregateEntry entry = entryPool.next();
                            queuedCount++;
                            if (keyAddress == 0) {
                                entry.of(
                                        vaf,
                                        null,
                                        0,
                                        valueAddress,
                                        valueAddressSize,
                                        columnSizeShr,
                                        doneLatch,
                                        oomCounter,
                                        null,
                                        perWorkerLocks,
                                        sharedCircuitBreaker
                                );
                            } else {
                                entry.of(
                                        vaf,
                                        pRosti,
                                        keyAddress,
                                        valueAddress,
                                        valueAddressSize,
                                        columnSizeShr,
                                        doneLatch,
                                        oomCounter,
                                        raf,
                                        perWorkerLocks,
                                        sharedCircuitBreaker
                                );
                            }
                            queue.get(cursor).entry = entry;
                            pubSeq.done(cursor);
                        }
                        total++;
                    }
                }
            } catch (DataUnavailableException e) {
                // We're not yet done, so no need to cancel the circuit breaker. 
                throw e;
            } catch (Throwable e) {
                sharedCircuitBreaker.cancel();
                throw e;
            } finally {
                // all done? great start consuming the queue we just published
                // how do we get to the end? If we consume our own queue there is chance we will be consuming
                // aggregation tasks not related to this execution (we work in concurrent environment)
                // To deal with that we need to have our own checklist.

                // Make sure we're consuming jobs even when we failed. We cannot close "rosti" when there are
                // tasks in flight.

                reclaimed = GroupByNotKeyedVectorRecordCursorFactory.getRunWhatsLeft(
                        bus.getVectorAggregateSubSeq(),
                        queue,
                        queuedCount,
                        reclaimed,
                        workerId,
                        doneLatch,
                        circuitBreaker,
                        sharedCircuitBreaker
                );
                // we can't reallocate rosti until tasks are complete because some other thread could be using it
                if (sharedCircuitBreaker.isCanceled()) {
                    resetRostiMemorySize();
                }
            }

            if (oomCounter.get() > 0) {
                resetRostiMemorySize();
                throw new OutOfMemoryError();
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
                                throw new OutOfMemoryError();
                            }
                            raf.updateMemoryUsage(pRostiBig, oldSize);
                        }

                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                        // some wrapUp() methods can increase rosti size
                        long oldSize = Rosti.getAllocMemory(pRostiBig);
                        if (!vaf.wrapUp(pRostiBig)) {
                            resetRostiMemorySize();
                            throw new OutOfMemoryError();
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
                            throw new OutOfMemoryError();
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

        private class RostiRecord implements Record {
            private final Long256Impl long256A = new Long256Impl();
            private final Long256Impl long256B = new Long256Impl();
            private long pRow;

            @Override
            public long getDate(int col) {
                return getLong(col);
            }

            @Override
            public double getDouble(int col) {
                return Unsafe.getUnsafe().getDouble(getValueOffset(col));
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
                return Unsafe.getUnsafe().getInt(getValueOffset(col));
            }

            @Override
            public int getInt(int col) {
                return Unsafe.getUnsafe().getInt(getValueOffset(col));
            }

            @Override
            public long getLong(int col) {
                return Unsafe.getUnsafe().getLong(getValueOffset(col));
            }

            @Override
            public void getLong256(int col, CharSinkBase<?> sink) {
                Long256Impl v = (Long256Impl) getLong256A(col);
                v.toSink(sink);
            }

            @Override
            public Long256 getLong256A(int col) {
                return getLong256Value(long256A, col);
            }

            @Override
            public Long256 getLong256B(int col) {
                return getLong256Value(long256B, col);
            }

            public Long256 getLong256Value(Long256 dst, int col) {
                final long offset = getValueOffset(col);
                final long l0 = Unsafe.getUnsafe().getLong(offset);
                final long l1 = Unsafe.getUnsafe().getLong(offset + Long.BYTES);
                final long l2 = Unsafe.getUnsafe().getLong(offset + 2 * Long.BYTES);
                final long l3 = Unsafe.getUnsafe().getLong(offset + 3 + Long.BYTES);
                dst.setAll(l0, l1, l2, l3);
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
            public CharSequence getStr(int col) {
                return null;
            }

            @Override
            public void getStr(int col, CharSink sink) {
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
            public CharSequence getSym(int col) {
                return pageFrameCursor.getSymbolTable(symbolTableSkewIndex.getQuick(col)).valueOf(getInt(col));
            }

            @Override
            public CharSequence getSymB(int col) {
                return pageFrameCursor.getSymbolTable(symbolTableSkewIndex.getQuick(col)).valueBOf(getInt(col));
            }

            @Override
            public long getTimestamp(int col) {
                return getLong(col);
            }

            public void of(long pRow) {
                this.pRow = pRow;
            }

            private long getValueOffset(int column) {
                return pRow + columnSkewIndex.getQuick(column);
            }
        }
    }
}
