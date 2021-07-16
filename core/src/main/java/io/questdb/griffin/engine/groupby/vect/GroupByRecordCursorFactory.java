/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.*;
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

public class GroupByRecordCursorFactory implements RecordCursorFactory {

    private final static Log LOG = LogFactory.getLog(GroupByRecordCursorFactory.class);

    private final RecordCursorFactory base;
    private final ObjList<VectorAggregateFunction> vafList;
    private final ObjectPool<VectorAggregateEntry> entryPool;
    private final ObjList<VectorAggregateEntry> activeEntries;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final RecordMetadata metadata;

    private final long[] pRosti;
    private final int keyColumnIndex;
    private final RostiRecordCursor cursor;

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

        this.entryPool = new ObjectPool<>(VectorAggregateEntry::new, configuration.getGroupByPoolCapacity());
        this.activeEntries = new ObjList<>(configuration.getGroupByPoolCapacity());
        // columnTypes and functions must align in the following way:
        // columnTypes[0] is the type of key, for now single key is supported
        // functions.size = columnTypes.size - 1, functions do not have instance for key, only for values
        // functions[0].type == columnTypes[1]
        // ...
        // functions[n].type == columnTypes[n+1]

        this.base = base;
        this.metadata = metadata;
        // first column is INT or SYMBOL
        this.pRosti = new long[workerCount];
        final int vafCount = vafList.size();
        this.vafList = new ObjList<>(vafCount);
        for (int i = 0; i < workerCount; i++) {
            pRosti[i] = Rosti.alloc(columnTypes, configuration.getGroupByMapCapacity());

            // todo: init key to null value

            // remember, single key for now
            switch (columnTypes.getColumnType(0)) {
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
            this.cursor = new RostiRecordCursor(pRosti, columnSkewIndex, symbolSkew);
        } else {
            this.cursor = new RostiRecordCursor(pRosti, columnSkewIndex, null);
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
    public void close() {
        Misc.freeObjList(vafList);
        for (int i = 0, n = pRosti.length; i < n; i++) {
            Rosti.free(pRosti[i]);
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {

        // clear maps
        for (int i = 0, n = pRosti.length; i < n; i++) {
            Rosti.clear(pRosti[i]);
        }

        final MessageBus bus = executionContext.getMessageBus();
        assert bus != null;

        final PageFrameCursor cursor = base.getPageFrameCursor(executionContext);
        final int vafCount = vafList.size();

        // clear state of aggregate functions
        for (int i = 0; i < vafCount; i++) {
            vafList.getQuick(i).clear();
        }

        final RingQueue<VectorAggregateTask> queue = bus.getVectorAggregateQueue();
        final Sequence pubSeq = bus.getVectorAggregatePubSeq();

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
            workerId = 0;
        }

        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            final long keyAddress = frame.getPageAddress(keyColumnIndex);
            for (int i = 0; i < vafCount; i++) {
                final VectorAggregateFunction vaf = vafList.getQuick(i);
                // when column index = -1 we assume that vector function does not have value
                // argument and it can only derive count via memory size
                final int columnIndex = vaf.getColumnIndex();
                final long valueAddress = columnIndex > -1 ? frame.getPageAddress(columnIndex) : 0;
                final int pageColIndex = columnIndex > -1 ? columnIndex : keyColumnIndex;
                final int columnSizeShr = frame.getColumnSize(pageColIndex);
                final long valueAddressSize = frame.getPageSize(pageColIndex);

                long seq = pubSeq.next();
                if (seq < 0) {
                    if (keyAddress == 0) {
                        vaf.aggregate(valueAddress, valueAddressSize, columnSizeShr, workerId);
                    } else {
                        vaf.aggregate(pRosti[workerId], keyAddress, valueAddress, valueAddressSize, columnSizeShr, workerId);
                    }
                    ownCount++;
                } else {
                    if (keyAddress != 0 || valueAddress != 0) {
                        final VectorAggregateEntry entry = entryPool.next();
                        if (keyAddress == 0) {
                            entry.of(queuedCount++, vaf, null, 0, valueAddress, valueAddressSize, columnSizeShr, doneLatch);
                        } else {
                            entry.of(queuedCount++, vaf, pRosti, keyAddress, valueAddress, valueAddressSize, columnSizeShr, doneLatch);
                        }
                        activeEntries.add(entry);
                        queue.get(seq).entry = entry;
                        pubSeq.done(seq);
                    }
                }
                total++;
            }
        }

        // all done? great start consuming the queue we just published
        // how do we get to the end? If we consume our own queue there is chance we will be consuming
        // aggregation tasks not related to this execution (we work in concurrent environment)
        // To deal with that we need to have our own checklist.

        // start at the back to reduce chance of clashing
        reclaimed = GroupByNotKeyedVectorRecordCursorFactory.getRunWhatsLeft(queuedCount, reclaimed, workerId, activeEntries, doneLatch, LOG);
        long pRosti0 = pRosti[0];

        if (pRosti.length > 1) {
            LOG.debug().$("merging").$();

            for (int j = 0; j < vafCount; j++) {
                final VectorAggregateFunction vaf = vafList.getQuick(j);
                for (int i = 1, n = pRosti.length; i < n; i++) {
                    vaf.merge(pRosti0, pRosti[i]);
                }
                vaf.wrapUp(pRosti0);
            }
        } else {
            for (int j = 0; j < vafCount; j++) {
                vafList.getQuick(j).wrapUp(pRosti0);
            }
        }

        LOG.info().$("done [total=").$(total).$(", ownCount=").$(ownCount).$(", reclaimed=").$(reclaimed).$(", queuedCount=").$(queuedCount).$(']').$();

        return this.cursor.of(cursor);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    private static class RostiRecordCursor implements RecordCursor {
        private final RostiRecord record;
        private final long pRosti;
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

        public RostiRecordCursor(long pRosti, IntList columnSkewIndex, IntList symbolTableSkewIndex) {
            this.pRosti = pRosti;
            this.record = new RostiRecord();
            this.symbolTableSkewIndex = symbolTableSkewIndex;
            this.columnSkewIndex = columnSkewIndex;
        }

        public RostiRecordCursor of(PageFrameCursor parent) {
            this.parent = parent;
            this.toTop();
            return this;
        }

        @Override
        public void close() {
            Misc.free(parent);
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
            this.size = Rosti.getSize(pRosti);
            this.shift = Rosti.getSlotShift(pRosti);
            this.count = 0;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return parent.getSymbolMapReader(symbolTableSkewIndex.getQuick(columnIndex));
        }

        private class RostiRecord implements Record {
            private long pRow;

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

            }

            @Override
            public Long256 getLong256A(int col) {
                return null;
            }

            @Override
            public Long256 getLong256B(int col) {
                return null;
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
                return parent.getSymbolMapReader(symbolTableSkewIndex.getQuick(col)).valueOf(getInt(col));
            }

            @Override
            public CharSequence getSymB(int col) {
                return parent.getSymbolMapReader(symbolTableSkewIndex.getQuick(col)).valueBOf(getInt(col));
            }

            @Override
            public long getTimestamp(int col) {
                return getLong(col);
            }
        }
    }

}
