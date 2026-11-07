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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HashJoinLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final int columnSplit;
    private final RecordSink masterSink;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final RecordSink slaveKeySink;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private HashJoinRecordCursor cursor;
    private boolean masterDetermined = false;
    // a condition on slave columns that every row of the join meets, see setSlaveFilter()
    private @Nullable Function slaveFilter;
    private @Nullable JoinSymbolTableSource slaveFilterSymbolTableSource;
    private @Nullable SymbolTranslatingRecord symbolTranslatingRecord;

    public HashJoinLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just 2 INTs, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveKeySink,
            int columnSplit,
            JoinContext joinContext,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.columnSplit = columnSplit;
        this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
        this.symbolTranslatingRecord = masterSymbolKeyColumnIndices != null ?
                new SymbolTranslatingRecord(configuration, Math.max(masterFactory.getMetadata().getColumnCount(), slaveFactory.getMetadata().getColumnCount()),
                        masterSymbolKeyColumnIndices.length) : null;
        try {
            this.masterSink = masterSink;
            this.slaveKeySink = slaveKeySink;
            this.cursor = new HashJoinRecordCursor(columnSplit, configuration, joinColumnTypes, valueTypes);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        boolean followOrderBy = masterFactory.followedOrderByAdvice();
        masterDetermined |= followOrderBy;
        return followOrderBy;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            boolean swapped = false;
            if (masterFactory.recordCursorSupportsRandomAccess() && !masterDetermined) {
                long masterSize = masterCursor.size();
                long slaveSize = slaveCursor.size();

                if (masterSize > 0 && slaveSize > 0 && masterSize < slaveSize) {
                    RecordCursor temp = masterCursor;
                    masterCursor = slaveCursor;
                    slaveCursor = temp;
                    swapped = true;
                }
            }

            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            cursor.of(masterCursor, slaveCursor, executionContext, swapped);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() binds the per-query tracker and reopens the join map, slave chain and caches before it adopts
            // the cursors; close() frees them under that tracker and resets isOpen so the factory is reusable.
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        int scanDirection = masterFactory.getScanDirection();
        masterDetermined |= scanDirection != RecordCursorFactory.SCAN_DIRECTION_OTHER;
        return scanDirection;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    // Takes a condition that reads slave columns only and that every row of the join meets, such as a
    // condition after the join that SqlCodeGenerator rewrote onto the slave's join keys. When the cursor
    // hashes the master, it reads each slave row once, so one evaluation decides every row of the join
    // that the slave row makes. Otherwise, the cursor evaluates it for each row of the join, as a
    // filter after the join would.
    public void setSlaveFilter(@NotNull Function slaveFilter) {
        assert this.slaveFilter == null;
        this.slaveFilter = slaveFilter;
        this.slaveFilterSymbolTableSource = new JoinSymbolTableSource(columnSplit);
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash Join Light");
        sink.attr("condition").val(joinContext);
        if (symbolTranslatingRecord != null) {
            sink.attr("symbolKeyJoin").val(true);
        }
        if (slaveFilter != null) {
            sink.attr("filter").val(slaveFilter);
        }
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    private static void populateRowIDHashMap(
            SqlExecutionCircuitBreaker circuitBreaker,
            RecordCursor cursor,
            Map keyMap,
            RecordSink recordSink,
            LongChain rowIDChain,
            Record keyRecord
    ) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();

            MapKey key = keyMap.withKey();
            key.put(keyRecord, recordSink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                value.putInt(0, rowIDChain.put(record.getRowId(), -1));
                value.putInt(1, 1);
            } else {
                value.putInt(0, rowIDChain.put(record.getRowId(), value.getInt(0)));
                value.addInt(1, 1);
            }
        }
    }

    @Override
    protected void _close() {
        final HashJoinRecordCursor cursor = this.cursor;
        this.cursor = null;
        final SymbolTranslatingRecord symbolTranslatingRecord = this.symbolTranslatingRecord;
        this.symbolTranslatingRecord = null;
        final Function slaveFilter = this.slaveFilter;
        this.slaveFilter = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, symbolTranslatingRecord);
        failure = Misc.freeBestEffort(failure, slaveFilter);
        CairoException.rethrowCleanupFailure(failure);
    }

    // The hash table that a CROSS or nested loop LEFT join reading the master side of every key consults,
    // see SqlCodeGenerator.generateJoins(). Such a master does not support random access, so the cursor
    // does not swap the sides, and the table holds the slave keys.
    public JoinKeyFilter getJoinKeyFilter() {
        return cursor;
    }

    private class HashJoinRecordCursor extends AbstractJoinCursor implements JoinKeyFilter {
        private final Map joinKeyMap;
        private final JoinRecord record;
        private final LongChain slaveChain;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isKnownEmpty;
        private boolean isMapBuilt;
        private boolean isMapEmpty;
        private boolean isOpen;
        private RecordSink masterCursorSink;
        private Record masterRecord;
        private LongChain.Cursor slaveChainCursor;
        private RecordSink slaveCursorSink;
        private Record slaveRecord;
        private boolean swapped;

        public HashJoinRecordCursor(int columnSplit, CairoConfiguration configuration, ColumnTypes joinColumnTypes, ColumnTypes valueTypes) {
            super(columnSplit);
            try {
                record = new JoinRecord(columnSplit);
                // Lazy variant: the map skeleton is constructed but the native backing is not
                // allocated until the first cursor's of() binds a MemoryTracker and reopens it.
                joinKeyMap = MapFactory.createUnorderedMap(configuration, joinColumnTypes, valueTypes, false, false);
                slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages(), true);
                isOpen = false;
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildMapOfSlaveRecords();
            if (slaveFilter != null) {
                calculateSizeFiltered(circuitBreaker, counter, slaveFilter);
                return;
            }
            while (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveChainCursor.next();
                counter.inc();
            }
            if (isMapEmpty) {
                return;
            }

            final Record masterRecord = masterCursor.getRecord();
            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    counter.add(value.getInt(1));
                }
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(joinKeyMap);
                Misc.free(slaveChain);
                Misc.free(symbolTranslatingRecord);
                super.close();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                RecordCursor cursor = swapped ? slaveCursor : masterCursor;
                return cursor.getSymbolTable(columnIndex);
            } else {
                RecordCursor cursor = swapped ? masterCursor : slaveCursor;
                return cursor.getSymbolTable(columnIndex - columnSplit);
            }
        }

        @Override
        public boolean hasMatch(Record record) {
            // hasNext() builds the table before it reads the master; a swapped cursor holds the master keys
            if (!isMapBuilt || swapped) {
                return true;
            }
            final MapKey key = joinKeyMap.withKey();
            key.put(record, masterCursorSink);
            return key.findValue() != null;
        }

        @Override
        public boolean hasNext() {
            buildMapOfSlaveRecords();
            if (slaveFilter != null) {
                return hasNextFiltered(slaveFilter);
            }
            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                return true;
            }
            // no master row matches a key of an empty table, so the master goes unread
            if (isMapEmpty) {
                return false;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    // we know cursor has values
                    // advance to get the first value
                    slaveChainCursor.hasNext();
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isKnownEmpty() {
            return isKnownEmpty;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                RecordCursor cursor = swapped ? slaveCursor : masterCursor;
                return cursor.newSymbolTable(columnIndex);
            } else {
                RecordCursor cursor = swapped ? masterCursor : slaveCursor;
                return cursor.newSymbolTable(columnIndex - columnSplit);
            }
        }

        @Override
        public long preComputedStateSize() {
            return isMapBuilt ? 1 : 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveChainCursor = null;
            if (!isMapBuilt) {
                slaveCursor.toTop();
                joinKeyMap.clear();
                slaveChain.clear();
            }
            if (slaveFilter != null) {
                slaveFilter.toTop();
            }
        }

        private void buildMapOfSlaveRecords() {
            if (!isMapBuilt) {
                Record keyRecord = symbolTranslatingRecord != null ? symbolTranslatingRecord : slaveCursor.getRecord();
                populateRowIDHashMap(circuitBreaker, slaveCursor, joinKeyMap, slaveCursorSink, slaveChain, keyRecord);
                isMapBuilt = true;
                isMapEmpty = joinKeyMap.size() == 0;
            }
        }

        // Counts as hasNextFiltered() reads. A cursor that does not swap evaluates the filter for each entry of a
        // hash chain, so it checks the circuit breaker for each entry, as counting the rows of a filter after the
        // join would.
        private void calculateSizeFiltered(SqlExecutionCircuitBreaker circuitBreaker, Counter counter, Function slaveFilter) {
            if (slaveChainCursor != null) {
                while (slaveChainCursor.hasNext()) {
                    final long rowId = slaveChainCursor.next();
                    // a swapped cursor checked the slave row before it started on the chain
                    if (swapped) {
                        counter.inc();
                    } else {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        slaveCursor.recordAt(slaveRecord, rowId);
                        if (slaveFilter.getBool(record)) {
                            counter.inc();
                        }
                    }
                }
            }
            if (isMapEmpty) {
                return;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null) {
                    if (swapped) {
                        if (slaveFilter.getBool(record)) {
                            counter.add(value.getInt(1));
                        }
                    } else {
                        slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                        while (slaveChainCursor.hasNext()) {
                            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                            slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                            if (slaveFilter.getBool(record)) {
                                counter.inc();
                            }
                        }
                    }
                }
            }
        }

        // The slave filter reads slave columns only. A swapped cursor reads the slave row by row, so one
        // evaluation decides the whole hash chain of a slave row, and the cursor evaluates the filter only for
        // the slave rows whose key the master has. Otherwise, it evaluates the filter for each slave row of
        // the hash chain of a master row.
        private boolean hasNextFiltered(Function slaveFilter) {
            if (slaveChainCursor != null) {
                while (slaveChainCursor.hasNext()) {
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    // a swapped cursor checked the slave row before it started on the chain
                    if (swapped || slaveFilter.getBool(record)) {
                        return true;
                    }
                }
            }
            // no master row matches a key of an empty table, so the master goes unread
            if (isMapEmpty) {
                return false;
            }

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterCursorSink);
                MapValue value = key.findValue();
                if (value != null && (!swapped || slaveFilter.getBool(record))) {
                    slaveChainCursor = slaveChain.getCursor(value.getInt(0));
                    while (slaveChainCursor.hasNext()) {
                        slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                        if (swapped || slaveFilter.getBool(record)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private void of(
                RecordCursor masterCursor,
                RecordCursor slaveCursor,
                SqlExecutionContext executionContext,
                boolean swapped
        ) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.setMemoryTracker(executionContext.getMemoryTracker());
                joinKeyMap.reopen();
                slaveChain.setMemoryTracker(executionContext.getMemoryTracker());
                slaveChain.reopen();
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecordB();
            this.swapped = swapped;
            if (swapped) {
                record.of(slaveRecord, masterRecord);
                this.masterCursorSink = slaveKeySink;
                this.slaveCursorSink = masterSink;
            } else {
                record.of(masterRecord, slaveRecord);
                this.masterCursorSink = masterSink;
                this.slaveCursorSink = slaveKeySink;
            }
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(slaveCursor.getRecord());
                symbolTranslatingRecord.setMemoryTracker(executionContext.getMemoryTracker());
                if (swapped) {
                    symbolTranslatingRecord.initSources(slaveCursor, masterCursor,
                            masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices);
                } else {
                    symbolTranslatingRecord.initSources(slaveCursor, masterCursor,
                            slaveSymbolKeyColumnIndices, masterSymbolKeyColumnIndices);
                }
            }
            slaveChainCursor = null;
            isMapBuilt = false;
            // A side that knows it has no rows, see RecordCursor.isKnownEmpty(), empties the join. The hash table
            // of the slave then stays empty, and the cursor reads neither side.
            isKnownEmpty = masterCursor.isKnownEmpty() || slaveCursor.isKnownEmpty();
            if (isKnownEmpty) {
                isMapBuilt = true;
                isMapEmpty = true;
            }
            if (slaveFilter != null) {
                assert slaveFilterSymbolTableSource != null;
                // the filter reads the columns of the join's slave, which a swapped cursor hashes as its master
                if (swapped) {
                    slaveFilterSymbolTableSource.of(slaveCursor, masterCursor);
                } else {
                    slaveFilterSymbolTableSource.of(masterCursor, slaveCursor);
                }
                slaveFilter.init(slaveFilterSymbolTableSource, executionContext);
            }
            // Adopt the cursors last so an initSources() or init() breach above leaves them unset for the
            // getCursor() catch.
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
        }
    }
}
