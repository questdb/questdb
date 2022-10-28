/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.mp.Worker;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.tasks.VectorAggregateTask;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class GroupByRecordCursorFactory extends AbstractRecordCursorFactory {

    private final static Log LOG = LogFactory.getLog(GroupByRecordCursorFactory.class);

    private final static int ROSTI_MINIMIZED_SIZE = 16;//16 is the minimum size usable on arm 

    private final RecordCursorFactory base;
    private final ObjList<VectorAggregateFunction> vafList;
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final ObjList<VectorAggregateEntry> activeEntries;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();

    private final long[] pRosti;
    private final int keyColumnIndex;
    private final RostiRecordCursor cursor;
    private final RostiAllocFacade raf;
    private final AtomicInteger oomCounter = new AtomicInteger();
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker;//used to signal cancellation to workers

    private final CairoConfiguration configuration;

    public GroupByRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            @Transient ColumnTypes columnTypes,
            int workerCount,
            @Transient ObjList<VectorAggregateFunction> vafList,
            int keyColumnIndexInBase,
            int keyColumnIndexInThisCursor,
            @Transient IntList symbolTableSkewIndex
    ) {
        super(metadata);
        this.configuration = configuration;
        this.entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
        this.activeEntries = new ObjList<>(configuration.getGroupByPoolCapacity());
        // columnTypes and functions must align in the following way:
        // columnTypes[0] is the type of key, for now single key is supported
        // functions.size = columnTypes.size - 1, functions do not have instance for key, only for values
        // functions[0].type == columnTypes[1]
        // ...
        // functions[n].type == columnTypes[n+1]

        this.sharedCircuitBreaker = new AtomicBooleanCircuitBreaker();
        this.base = base;
        // first column is INT or SYMBOL
        this.pRosti = new long[workerCount];
        final int vafCount = vafList.size();
        this.vafList = new ObjList<>(vafCount);
        this.raf = configuration.getRostiAllocFacade();
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
        this.keyColumnIndex = keyColumnIndexInBase;
        if (symbolTableSkewIndex.size() > 0) {
            final IntList symbolSkew = new IntList(symbolTableSkewIndex.size());
            symbolSkew.addAll(symbolTableSkewIndex);
            this.cursor = new RostiRecordCursor(pRosti, columnSkewIndex, symbolSkew, configuration.getGroupByMapCapacity());
        } else {
            this.cursor = new RostiRecordCursor(pRosti, columnSkewIndex, null, configuration.getGroupByMapCapacity());
        }
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

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.freeObjList(vafList);
        for (int i = 0, n = pRosti.length; i < n; i++) {
            raf.free(pRosti[i]);
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

        // clear maps
        for (int i = 0, n = pRosti.length; i < n; i++) {
            raf.clear(pRosti[i]);
        }

        this.oomCounter.set(0);

        final MessageBus bus = executionContext.getMessageBus();

        final PageFrameCursor cursor = base.getPageFrameCursor(executionContext, ORDER_ASC);
        final int vafCount = vafList.size();

        // clear state of aggregate functions
        for (int i = 0; i < vafCount; i++) {
            vafList.getQuick(i).clear();
        }

        final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
        final Sequence pubSeq = bus.getVectorAggregatePubSeq();

        this.sharedCircuitBreaker.reset();
        this.entryPool.clear();
        this.activeEntries.clear();
        int queuedCount = 0;
        int ownCount = 0;
        int reclaimed = 0;
        int total = 0;

        doneLatch.reset();

        // check if this executed via worker pool
        final Thread thread = Thread.currentThread();
        final int workerId;
        if (thread instanceof Worker) {
            workerId = ((Worker) thread).getWorkerId();
        } else {
            workerId = pRosti.length - 1;//to avoid clashing with other worker with id=0 in tests 
        }

        try {
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
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

                    long seq = pubSeq.next();
                    if (seq < 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                        if (keyAddress == 0) {
                            vaf.aggregate(valueAddress, valueAddressSize, columnSizeShr, workerId);
                        } else {
                            long oldSize = Rosti.getAllocMemory(pRosti[workerId]);
                            if (!vaf.aggregate(pRosti[workerId], keyAddress, valueAddress, valueAddressSize, columnSizeShr, workerId)) {
                                oomCounter.incrementAndGet();
                            }
                            raf.updateMemoryUsage(pRosti[workerId], oldSize);
                        }
                        ownCount++;
                    } else {
                        if (keyAddress != 0 || valueAddress != 0) {
                            final VectorAggregateEntry entry = entryPool.next();
                            if (keyAddress == 0) {
                                entry.of(queuedCount++, vaf, null, 0, valueAddress, valueAddressSize, columnSizeShr, doneLatch, oomCounter, null, sharedCircuitBreaker);
                            } else {
                                entry.of(queuedCount++, vaf, pRosti, keyAddress, valueAddress, valueAddressSize, columnSizeShr, doneLatch, oomCounter, raf, sharedCircuitBreaker);
                            }
                            activeEntries.add(entry);
                            queue.get(seq).entry = entry;
                            pubSeq.done(seq);
                        }
                    }
                    total++;
                }
            }
        } catch (Throwable e) {
            sharedCircuitBreaker.cancel();
            Misc.free(cursor);
            throw e;
        } finally {
            // all done? great start consuming the queue we just published
            // how do we get to the end? If we consume our own queue there is chance we will be consuming
            // aggregation tasks not related to this execution (we work in concurrent environment)
            // To deal with that we need to have our own checklist.

            // Make sure we're consuming jobs even when we failed. We cannot close "rosti" when there are
            // tasks in flight.

            // start at the back to reduce chance of clashing
            reclaimed = GroupByNotKeyedVectorRecordCursorFactory.getRunWhatsLeft(queuedCount, reclaimed, workerId, activeEntries, doneLatch, LOG, circuitBreaker, sharedCircuitBreaker);
            //we can't reallocate rosti until tasks are complete because some other thread could be using it
            if (sharedCircuitBreaker.isCanceled()) {
                resetRostiMemorySize();
            }
        }

        if (oomCounter.get() > 0) {
            Misc.free(cursor);
            resetRostiMemorySize();

            throw new OutOfMemoryError();
        }

        // merge maps only when cursor was fetched successfully
        // otherwise assume error and save CPU cycles
        long pRostiBig = pRosti[0];
        try {
            if (pRosti.length > 1) {
                LOG.debug().$("merging").$();

                //due to uneven load distribution some rostis could be much bigger and some empty
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
                        if (pRostiBig == pRosti[i] ||
                                raf.getSize(pRosti[i]) < 1) {
                            continue;
                        }
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                        long oldSize = Rosti.getAllocMemory(pRostiBig);
                        if (!vaf.merge(pRostiBig, pRosti[i])) {
                            Misc.free(cursor);
                            resetRostiMemorySize();
                            throw new OutOfMemoryError();
                        }
                        raf.updateMemoryUsage(pRostiBig, oldSize);
                    }

                    circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                    //some wrapUp() methods can increase rosti size  
                    long oldSize = Rosti.getAllocMemory(pRostiBig);
                    if (!vaf.wrapUp(pRostiBig)) {
                        Misc.free(cursor);
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
                        LOG.debug().$("Couldn't minimize rosti memory [i=").$(i).$(",current_size=").$(Rosti.getSize(pRosti[i])).I$();
                    }
                }

            } else {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                for (int j = 0; j < vafCount; j++) {
                    if (!vafList.getQuick(j).wrapUp(pRostiBig)) {
                        Misc.free(cursor);
                        resetRostiMemorySize();
                        throw new OutOfMemoryError();
                    }
                }
            }
        } catch (Throwable t) {
            Misc.free(cursor);
            resetRostiMemorySize();

            throw t;
        }

        LOG.info().$("done [total=").$(total).$(", ownCount=").$(ownCount).$(", reclaimed=").$(reclaimed).$(", queuedCount=").$(queuedCount).$(']').$();
        return this.cursor.of(pRostiBig, cursor);
    }

    private void resetRostiMemorySize() {
        for (int i = 0, n = pRosti.length; i < n; i++) {
            if (!raf.reset(pRosti[i], ROSTI_MINIMIZED_SIZE)) {
                LOG.debug().$("Couldn't minimize rosti memory [i=").$(i).$(",current_size=").$(Rosti.getSize(pRosti[i])).I$();
            }
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupByRecord");
        sink.meta("vectorized").val(true);
        sink.attr("groupByFunctions").val(vafList);
        sink.attr("keyColumnIndex").val(keyColumnIndex);
        sink.child(base);
    }

    private class RostiRecordCursor implements RecordCursor {
        private final RostiRecord record;
        private long pRosti;
        private final IntList symbolTableSkewIndex;
        private final IntList columnSkewIndex;
        private RostiRecord recordB;
        private long ctrlStart;
        private long ctrl;
        private long slots;
        private long shift;
        private long size;
        private long count;
        private PageFrameCursor parent;
        private final int defaultMapSize;

        public RostiRecordCursor(long pRosti, IntList columnSkewIndex, IntList symbolTableSkewIndex, int defaultMapSize) {
            this.pRosti = pRosti;
            this.record = new RostiRecord();
            this.symbolTableSkewIndex = symbolTableSkewIndex;
            this.columnSkewIndex = columnSkewIndex;
            this.defaultMapSize = defaultMapSize;
        }

        public RostiRecordCursor of(long pRosti, PageFrameCursor parent) {
            this.pRosti = pRosti;
            this.parent = parent;
            this.toTop();
            return this;
        }

        @Override
        public void close() {
            Misc.free(parent);
            raf.reset(pRosti, ROSTI_MINIMIZED_SIZE);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
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
        public Record getRecordB() {
            if (recordB != null) {
                return recordB;
            }
            return (recordB = new RostiRecord());
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((RostiRecord) record).of(atRowId);
        }

        @Override

        public void toTop() {
            this.ctrl = this.ctrlStart = Rosti.getCtrl(pRosti);
            this.slots = Rosti.getSlots(pRosti);
            this.size = raf.getSize(pRosti);
            this.shift = Rosti.getSlotShift(pRosti);
            this.count = 0;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return parent.getSymbolTable(symbolTableSkewIndex.getQuick(columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return parent.newSymbolTable(symbolTableSkewIndex.getQuick(columnIndex));
        }

        private class RostiRecord implements Record {
            private long pRow;

            private final Long256Impl long256A = new Long256Impl();
            private final Long256Impl long256B = new Long256Impl();

            public void of(long pRow) {
                this.pRow = pRow;
            }

            @Override
            public BinarySequence getBin(int col) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getBinLen(int col) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean getBool(int col) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte getByte(int col) {
                throw new UnsupportedOperationException();
            }

            @Override
            public char getChar(int col) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getDate(int col) {
                return getLong(col);
            }

            private long getValueOffset(int column) {
                return pRow + columnSkewIndex.getQuick(column);
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
            public int getInt(int col) {
                return Unsafe.getUnsafe().getInt(getValueOffset(col));
            }

            @Override
            public long getLong(int col) {
                return Unsafe.getUnsafe().getLong(getValueOffset(col));
            }

            @Override
            public void getLong256(int col, CharSink sink) {
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
                return parent.getSymbolTable(symbolTableSkewIndex.getQuick(col)).valueOf(getInt(col));
            }

            @Override
            public CharSequence getSymB(int col) {
                return parent.getSymbolTable(symbolTableSkewIndex.getQuick(col)).valueBOf(getInt(col));
            }

            @Override
            public long getTimestamp(int col) {
                return getLong(col);
            }

            @Override
            public byte getGeoByte(int col) {
                return getByte(col);
            }

            @Override
            public short getGeoShort(int col) {
                return getShort(col);
            }

            @Override
            public int getGeoInt(int col) {
                return getInt(col);
            }

            @Override
            public long getGeoLong(int col) {
                return getLong(col);
            }
        }
    }

}
