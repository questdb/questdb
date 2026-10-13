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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.ColumnFunctionSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * An ASOF JOIN without join keys to a small table, as functions of the master row: a slave
 * column reads the value of the last slave row at or before the master row's timestamp, NULL
 * when there is none, as {@link AsOfJoinNoKeyFastRecordCursorFactory} joins it. A GROUP BY over
 * the join then reads the master's page frames in parallel, the slave columns being functions
 * of its rows, see SqlCodeGenerator.
 * <p>
 * Each function's init() loads the slave into arrays, read-only from then on, so that the
 * functions are thread-safe. The slave factory is released with the last function and the
 * compiler's own reference, see {@link #release()}.
 */
public final class AsOfLookup implements QuietCloseable {
    private final int masterTimestampIndex;
    private final long masterTimestampScale;
    // per lookup column: its slave column and type
    private final IntList slaveColumns = new IntList();
    private final IntList slaveColumnTypes = new IntList();
    private final CharSequence slaveName;
    private final int slaveTimestampIndex;
    private final long slaveTimestampScale;
    private int refCount = 1;
    private RecordCursorFactory slaveFactory;
    private volatile Snapshot snapshot;

    public AsOfLookup(
            RecordCursorFactory slaveFactory,
            CharSequence slaveName,
            int masterTimestampIndex,
            int masterTimestampType,
            int slaveTimestampType
    ) {
        this.slaveFactory = slaveFactory;
        this.slaveName = slaveName;
        this.masterTimestampIndex = masterTimestampIndex;
        this.slaveTimestampIndex = slaveFactory.getMetadata().getTimestampIndex();
        if (masterTimestampType == slaveTimestampType) {
            masterTimestampScale = slaveTimestampScale = 1L;
        } else {
            masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
            slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
        }
    }

    /**
     * Whether a slave column of the type can be read by a lookup function.
     */
    public static boolean isSupportedType(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.SYMBOL:
                return true;
            default:
                return false;
        }
    }

    /**
     * The metadata the GROUP BY compiles against: the master's columns at their own indexes, named
     * as {@code joined} names them (the others under names no query can write), then the slave
     * columns {@code joined} selects, which compile to lookup functions.
     *
     * @param joined      the join's metadata as the query reads it
     * @param crossIndex  per {@code joined} column, its column of the join: master columns first
     * @param master      the master's metadata
     * @param masterCount the master's column count
     */
    public RecordMetadata newMetadata(RecordMetadata joined, IntList crossIndex, RecordMetadata master, int masterCount) {
        final LookupMetadata metadata = new LookupMetadata(masterCount);
        for (int m = 0; m < masterCount; m++) {
            int named = -1;
            for (int i = 0, n = joined.getColumnCount(); i < n; i++) {
                if (crossIndex.getQuick(i) == m) {
                    named = i;
                    break;
                }
            }
            final TableColumnMetadata column = master.getColumnMetadata(m);
            final String name = named >= 0 ? joined.getColumnName(named) : "\u0001master" + m;
            metadata.add(new TableColumnMetadata(
                    name,
                    column.getColumnType(),
                    column.getIndexType(),
                    column.getIndexValueBlockCapacity(),
                    column.isSymbolTableStatic(),
                    column.getMetadata()
            ));
        }
        for (int i = 0, n = joined.getColumnCount(); i < n; i++) {
            final int c = crossIndex.getQuick(i);
            if (c >= masterCount) {
                slaveColumns.add(c - masterCount);
                slaveColumnTypes.add(joined.getColumnType(i));
                // a lookup's symbol table is its own, not static
                metadata.add(new TableColumnMetadata(joined.getColumnName(i), joined.getColumnType(i), IndexType.NONE, 0, false, null));
            }
        }
        metadata.setTimestampIndex(master.getTimestampIndex());
        return metadata;
    }

    /**
     * Drops a reference: the compiler's, or a function's on close. The last frees the slave.
     */
    public void release() {
        if (--refCount == 0) {
            slaveFactory = Misc.free(slaveFactory);
        }
    }

    @Override
    public void close() {
        release();
    }

    private void acquire() {
        refCount++;
    }

    // the last slave row at or before the master row's timestamp, or -1
    private int find(Snapshot s, Record rec) {
        final long t = AbstractAsOfJoinFastRecordCursor.scaleTimestamp(rec.getTimestamp(masterTimestampIndex), masterTimestampScale);
        final long[] ts = s.timestamps;
        int lo = 0;
        int hi = s.rowCount - 1;
        int found = -1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (ts[mid] <= t) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found;
    }

    private void load(SqlExecutionContext executionContext) throws SqlException {
        final int columnCount = slaveColumns.size();
        final LongList timestamps = new LongList();
        final ObjList<LongList> values = new ObjList<>(columnCount);
        final ObjList<IntObjHashMap<String>> symbols = new ObjList<>(columnCount);
        for (int c = 0; c < columnCount; c++) {
            values.add(new LongList());
            symbols.add(ColumnType.isSymbol(slaveColumnTypes.getQuick(c)) ? new IntObjHashMap<>() : null);
        }
        try (RecordCursor cursor = slaveFactory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                timestamps.add(AbstractAsOfJoinFastRecordCursor.scaleTimestamp(record.getTimestamp(slaveTimestampIndex), slaveTimestampScale));
                for (int c = 0; c < columnCount; c++) {
                    final int column = slaveColumns.getQuick(c);
                    final int type = slaveColumnTypes.getQuick(c);
                    final long value;
                    switch (ColumnType.tagOf(type)) {
                        case ColumnType.INT:
                            value = record.getInt(column);
                            break;
                        case ColumnType.DOUBLE:
                            value = Double.doubleToRawLongBits(record.getDouble(column));
                            break;
                        case ColumnType.DATE:
                            value = record.getDate(column);
                            break;
                        case ColumnType.TIMESTAMP:
                            value = record.getTimestamp(column);
                            break;
                        case ColumnType.SYMBOL: {
                            final int key = record.getInt(column);
                            value = key;
                            final IntObjHashMap<String> map = symbols.getQuick(c);
                            if (key != SymbolTable.VALUE_IS_NULL && map.keyIndex(key) > -1) {
                                map.put(key, Chars.toString(record.getSymA(column)));
                            }
                            break;
                        }
                        default:
                            value = record.getLong(column);
                            break;
                    }
                    values.getQuick(c).add(value);
                }
            }
        }
        final int rowCount = timestamps.size();
        final long[] ts = new long[rowCount];
        for (int r = 0; r < rowCount; r++) {
            ts[r] = timestamps.getQuick(r);
        }
        final long[][] columnValues = new long[columnCount][];
        for (int c = 0; c < columnCount; c++) {
            final LongList list = values.getQuick(c);
            final long[] v = new long[rowCount];
            for (int r = 0; r < rowCount; r++) {
                v[r] = list.getQuick(r);
            }
            columnValues[c] = v;
        }
        snapshot = new Snapshot(rowCount, ts, columnValues, symbols);
    }

    private Function newFunction(int lookupColumn) {
        acquire();
        final int type = slaveColumnTypes.getQuick(lookupColumn);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.INT:
                return new IntLookup(lookupColumn);
            case ColumnType.DOUBLE:
                return new DoubleLookup(lookupColumn);
            case ColumnType.DATE:
                return new DateLookup(lookupColumn);
            case ColumnType.TIMESTAMP:
                return new TimestampLookup(lookupColumn, type);
            case ColumnType.SYMBOL:
                return new SymbolLookup(lookupColumn);
            default:
                return new LongLookup(lookupColumn);
        }
    }

    private void toPlan(PlanSink sink, int lookupColumn) {
        sink.val("asof(").val(slaveName).val('.').val(slaveFactory != null ? slaveFactory.getMetadata().getColumnName(slaveColumns.getQuick(lookupColumn)) : "?").val(')');
    }

    /**
     * Marks a function that reads a joined column by lookup: the GROUP BY keys on it as a
     * function, not as a column of its base.
     */
    public interface Column {
    }

    private static final class Snapshot {
        final int rowCount;
        final IntObjHashMap<String>[] symbols;
        final long[] timestamps;
        final long[][] values;

        @SuppressWarnings("unchecked")
        Snapshot(int rowCount, long[] timestamps, long[][] values, ObjList<IntObjHashMap<String>> symbols) {
            this.rowCount = rowCount;
            this.timestamps = timestamps;
            this.values = values;
            this.symbols = new IntObjHashMap[symbols.size()];
            for (int i = 0, n = symbols.size(); i < n; i++) {
                this.symbols[i] = symbols.getQuick(i);
            }
        }
    }

    private final class DateLookup extends DateFunction implements Column {
        private final int column;

        DateLookup(int column) {
            this.column = column;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public long getDate(Record rec) {
            final Snapshot s = snapshot;
            final int row = find(s, rec);
            return row < 0 ? Numbers.LONG_NULL : s.values[column][row];
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            load(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            AsOfLookup.this.toPlan(sink, column);
        }
    }

    private final class DoubleLookup extends DoubleFunction implements Column {
        private final int column;

        DoubleLookup(int column) {
            this.column = column;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public double getDouble(Record rec) {
            final Snapshot s = snapshot;
            final int row = find(s, rec);
            return row < 0 ? Double.NaN : Double.longBitsToDouble(s.values[column][row]);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            load(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            AsOfLookup.this.toPlan(sink, column);
        }
    }

    private final class IntLookup extends IntFunction implements Column {
        private final int column;

        IntLookup(int column) {
            this.column = column;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public int getInt(Record rec) {
            final Snapshot s = snapshot;
            final int row = find(s, rec);
            return row < 0 ? Numbers.INT_NULL : (int) s.values[column][row];
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            load(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            AsOfLookup.this.toPlan(sink, column);
        }
    }

    private final class LongLookup extends LongFunction implements Column {
        private final int column;

        LongLookup(int column) {
            this.column = column;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public long getLong(Record rec) {
            final Snapshot s = snapshot;
            final int row = find(s, rec);
            return row < 0 ? Numbers.LONG_NULL : s.values[column][row];
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            load(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            AsOfLookup.this.toPlan(sink, column);
        }
    }

    private final class LookupMetadata extends GenericRecordMetadata implements ColumnFunctionSource {
        private final int masterCount;

        LookupMetadata(int masterCount) {
            this.masterCount = masterCount;
        }

        @Override
        public @Nullable Function newColumnFunction(int columnIndex) {
            return columnIndex < masterCount ? null : newFunction(columnIndex - masterCount);
        }
    }

    private final class SymbolLookup extends SymbolFunction implements Column {
        private final int column;

        SymbolLookup(int column) {
            this.column = column;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public int getInt(Record rec) {
            final Snapshot s = snapshot;
            final int row = find(s, rec);
            return row < 0 ? SymbolTable.VALUE_IS_NULL : (int) s.values[column][row];
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return valueOf(getInt(rec));
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return getSymbol(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            load(executionContext);
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public @Nullable SymbolTable newSymbolTable() {
            return this;
        }

        @Override
        public void toPlan(PlanSink sink) {
            AsOfLookup.this.toPlan(sink, column);
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key == SymbolTable.VALUE_IS_NULL ? null : snapshot.symbols[column].get(key);
        }
    }

    private final class TimestampLookup extends TimestampFunction implements Column {
        private final int column;

        TimestampLookup(int column, int type) {
            super(type);
            this.column = column;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public long getTimestamp(Record rec) {
            final Snapshot s = snapshot;
            final int row = find(s, rec);
            return row < 0 ? Numbers.LONG_NULL : s.values[column][row];
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            load(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            AsOfLookup.this.toPlan(sink, column);
        }
    }
}
