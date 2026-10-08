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


package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * A GROUP BY whose groups arrive one after another, as the last step of an Async Window chain
 * (see {@link AsyncWindowStage}): every key of the GROUP BY is a column of the rows so far, one is
 * the scan's key (unless the scan walks a single key), and the others are at most one column that
 * never decreases within a key of the scan, such as a running sum of non-negative values. The rows
 * of a group are then consecutive, so the stage aggregates the current group only, in fields, and
 * outputs it once the next group starts, or the rows end ({@link #flush()}). Each group sees its
 * rows in the scan's order, and its aggregates are computed by the same functions, with the same
 * {@code computeFirst}/{@code computeNext} calls, as the serial GROUP BY makes: the values are the
 * serial plan's, bit for bit.
 * <p>
 * The output columns are those of the GROUP BY's projection, read from the closed group: the
 * aggregates' value slots first, then the key columns, which is the layout of the serial GROUP
 * BY's map record the projection was compiled against.
 */
public class AsyncWindowGroupByStage extends AsyncWindowStage {
    private final GroupRecord closedRecord = new GroupRecord();
    private final ObjList<GroupByFunction> groupByFunctions;
    private final int groupByFunctionsCount;
    // the input columns of the keys, and their column types
    private final int[] keyColumns;
    private final int[] keyTypes;
    private final VirtualRecord output;
    private final ObjList<Function> projection;
    // the layout of the step's input rows, for the chains that capture a task's head rows
    private ColumnTypes headTypes;
    private RecordSink headSink;
    private final int valueCount;
    private long[] closedKeys;
    private SimpleMapValue closedValue;
    private long[] currentKeys;
    private SimpleMapValue currentValue;
    // the rows of a task's head group, see beginHeadCapture()
    private RecordChain headChain;
    private final long[] headKeys;
    private long headPrevOffset = -1;
    private long headRowCount;
    private Record input;
    private SymbolTableSource inputSymbols;
    private boolean isCapturing;
    private boolean isGroupOpen;
    private boolean isOpen;
    private long rowId;
    // calls the aggregates' computeFirst/computeNext through generated, monomorphic call sites, as
    // the serial GROUP BY does; null to call them in a loop
    private GroupByFunctionsUpdater updater;

    /**
     * @param projection       the GROUP BY's output functions, compiled against the serial map
     *                         record's layout, value slots then keys; owned by the stage, and
     *                         {@code groupByFunctions} are among them
     * @param groupByFunctions the aggregates, in value slot order
     * @param keyColumns       the input columns of the keys, in the map record's key order
     * @param keyTypes         the column types of the keys
     * @param valueCount       the value slots of the aggregates
     */
    public AsyncWindowGroupByStage(
            @NotNull ObjList<Function> projection,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            int @NotNull [] keyColumns,
            int @NotNull [] keyTypes,
            int valueCount
    ) {
        super(KIND_GROUP_BY, projection, null, 0);
        this.projection = projection;
        this.groupByFunctions = groupByFunctions;
        this.groupByFunctionsCount = groupByFunctions.size();
        this.keyColumns = keyColumns;
        this.keyTypes = keyTypes;
        this.valueCount = valueCount;
        this.currentKeys = new long[keyColumns.length];
        this.headKeys = new long[keyColumns.length];
        this.closedKeys = new long[keyColumns.length];
        this.currentValue = new SimpleMapValue(valueCount);
        this.closedValue = new SimpleMapValue(valueCount);
        this.output = new VirtualRecord(projection);
        output.of(closedRecord);
    }

    // The key's bits as the serial map would compare them: raw bits, so -0.0 and 0.0 differ.
    private static long readKey(Record record, int column, int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN -> record.getBool(column) ? 1 : 0;
            case ColumnType.BYTE -> record.getByte(column);
            case ColumnType.GEOBYTE -> record.getGeoByte(column);
            case ColumnType.SHORT -> record.getShort(column);
            case ColumnType.GEOSHORT -> record.getGeoShort(column);
            case ColumnType.CHAR -> record.getChar(column);
            case ColumnType.INT, ColumnType.SYMBOL -> record.getInt(column);
            case ColumnType.IPv4 -> record.getIPv4(column);
            case ColumnType.GEOINT -> record.getGeoInt(column);
            case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(column));
            case ColumnType.LONG -> record.getLong(column);
            case ColumnType.DATE -> record.getDate(column);
            case ColumnType.TIMESTAMP -> record.getTimestamp(column);
            case ColumnType.GEOLONG -> record.getGeoLong(column);
            case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(column));
            default -> throw new UnsupportedOperationException("key type " + ColumnType.nameOf(type));
        };
    }

    /**
     * Whether a key column of this type can be held, see {@link #readKey}.
     */
    public static boolean isKeyTypeSupported(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.GEOBYTE, ColumnType.SHORT, ColumnType.GEOSHORT,
                 ColumnType.CHAR, ColumnType.INT, ColumnType.SYMBOL, ColumnType.IPv4, ColumnType.GEOINT,
                 ColumnType.FLOAT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.GEOLONG,
                 ColumnType.DOUBLE -> true;
            default -> false;
        };
    }

    @Override
    public Record bind(Record input, SymbolTableSource inputSymbols) {
        this.input = input;
        this.inputSymbols = inputSymbols;
        return output;
    }

    @Override
    public void close() {
        // the aggregates are among the projection's functions
        Misc.freeObjList(projection);
        Misc.free(currentValue);
        Misc.free(closedValue);
    }

    @Override
    public void closeCursor() {
        if (isOpen) {
            isOpen = false;
            for (int i = 0, n = projection.size(); i < n; i++) {
                projection.getQuick(i).cursorClosed();
            }
            isGroupOpen = false;
            isCapturing = false;
            headChain = null;
        }
    }

    /**
     * Makes {@code value} and {@code keys} the open group, as if this stage had aggregated its
     * rows: the group a task left open at its end, which the next task's rows may continue.
     */
    public void adoptGroup(SimpleMapValue value, long[] keys) {
        currentValue.copy(value);
        System.arraycopy(keys, 0, currentKeys, 0, keys.length);
        isGroupOpen = true;
    }

    /**
     * Until the rows of the first group end, appends them to {@code headChain} instead of
     * aggregating them: a task that continues a key from the task before it cannot know the
     * aggregates of the group its first rows continue, which the cursor completes by replaying
     * them, see {@link #replay}. The rows after them aggregate as usual.
     */
    public void beginHeadCapture(RecordChain headChain) {
        this.headChain = headChain;
        headChain.clear();
        headPrevOffset = -1;
        headRowCount = 0;
        isCapturing = true;
        isGroupOpen = false;
    }

    /**
     * Closes the open group, if any: {@link #getOutputRecord()} then holds it.
     */
    public boolean closeOpenGroup() {
        if (isGroupOpen) {
            closeGroup();
            return true;
        }
        return false;
    }

    /**
     * Moves the open group, if any, out of this stage into {@code value} and {@code keys}.
     */
    public boolean exportOpenGroup(SimpleMapValue value, long[] keys) {
        if (isGroupOpen) {
            value.copy(currentValue);
            System.arraycopy(currentKeys, 0, keys, 0, keys.length);
            isGroupOpen = false;
            return true;
        }
        return false;
    }

    /**
     * Rows captured since {@link #beginHeadCapture}.
     */
    public long getHeadRowCount() {
        return headRowCount;
    }

    public RecordSink getHeadSink() {
        return headSink;
    }

    public ColumnTypes getHeadTypes() {
        return headTypes;
    }

    public int getKeyCount() {
        return keyColumns.length;
    }

    /**
     * Calls the aggregates through an updater of a class
     * {@link GroupByFunctionsUpdaterFactory#getInstanceClass} generated for their count.
     */
    public void setUpdaterClass(Class<? extends GroupByFunctionsUpdater> updaterClass) {
        this.updater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, groupByFunctions);
    }

    /**
     * The layout of the step's input rows, which the chains of a task's head rows hold, see
     * {@link #beginHeadCapture}.
     */
    public void setHeadLayout(ColumnTypes headTypes, RecordSink headSink) {
        this.headTypes = headTypes;
        this.headSink = headSink;
    }

    /**
     * A key of the open group, as its raw bits.
     */
    public long getOpenKey(int keyIndex) {
        return currentKeys[keyIndex];
    }

    /**
     * The record the GROUP BY outputs, the projection over the last group closed.
     */
    public Record getOutputRecord() {
        return output;
    }

    public int getValueCount() {
        return valueCount;
    }

    public boolean isCapturing() {
        return isCapturing;
    }

    public boolean isGroupOpen() {
        return isGroupOpen;
    }

    /**
     * Adds a row to its group as {@link #computeNext()} adds the input's current row; for the
     * rows a task captured, see {@link #beginHeadCapture}. Returns true when it closed a group.
     */
    public boolean replay(Record record, long rowId) {
        this.rowId = rowId;
        return accept(record);
    }

    /**
     * The row id the next row is given, its position in the scan's walk: aggregates such as
     * {@code first()} keep the row with the lowest id, which must be the earliest also when a
     * group's rows came from several tasks.
     */
    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    /**
     * Adds the input's current row to its group. Returns true when the row starts a new group
     * after another: the record the next stage reads then holds the group it closed.
     */
    @Override
    public boolean computeNext() {
        final Record record = input;
        if (isCapturing) {
            if (headPrevOffset == -1) {
                for (int i = 0, n = keyColumns.length; i < n; i++) {
                    headKeys[i] = readKey(record, keyColumns[i], keyTypes[i]);
                }
                headPrevOffset = headChain.put(record, -1);
                headRowCount++;
                rowId++;
                return false;
            }
            boolean same = true;
            for (int i = 0, n = keyColumns.length; i < n; i++) {
                if (readKey(record, keyColumns[i], keyTypes[i]) != headKeys[i]) {
                    same = false;
                    break;
                }
            }
            if (same) {
                headPrevOffset = headChain.put(record, headPrevOffset);
                headRowCount++;
                rowId++;
                return false;
            }
            // the head group ended: the cursor completes it, this stage goes on with the rows after it
            isCapturing = false;
            startGroup(record);
            return false;
        }
        return accept(record);
    }

    private boolean accept(Record record) {
        if (isGroupOpen) {
            boolean same = true;
            for (int i = 0, n = keyColumns.length; i < n; i++) {
                if (readKey(record, keyColumns[i], keyTypes[i]) != currentKeys[i]) {
                    same = false;
                    break;
                }
            }
            if (same) {
                final SimpleMapValue value = currentValue;
                if (updater != null) {
                    updater.updateExisting(value, record, rowId);
                } else {
                    for (int i = 0; i < groupByFunctionsCount; i++) {
                        groupByFunctions.getQuick(i).computeNext(value, record, rowId);
                    }
                }
                rowId++;
                return false;
            }
            closeGroup();
            startGroup(record);
            return true;
        }
        startGroup(record);
        return false;
    }

    @Override
    public boolean flush() {
        if (isGroupOpen) {
            closeGroup();
            return true;
        }
        return false;
    }

    public ObjList<GroupByFunction> getGroupByFunctions() {
        return groupByFunctions;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) projection.getQuick(columnIndex);
    }

    @Override
    public boolean isRowPreserving() {
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) projection.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public void open(SqlExecutionContext executionContext, boolean cloneSymbolTables) throws SqlException {
        isOpen = true;
        isGroupOpen = false;
        rowId = 0;
        final boolean current = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(cloneSymbolTables);
        try {
            // the projection reads the keys' symbol tables by their input columns, see MapSymbolColumn
            Function.init(projection, inputSymbols, executionContext, null);
        } finally {
            executionContext.setCloneSymbolTables(current);
        }
    }

    @Override
    public void toTop() {
        isGroupOpen = false;
        isCapturing = false;
        rowId = 0;
        for (int i = 0, n = projection.size(); i < n; i++) {
            projection.getQuick(i).toTop();
        }
    }

    // The current group becomes the closed one, which the output reads.
    private void closeGroup() {
        final SimpleMapValue value = closedValue;
        closedValue = currentValue;
        currentValue = value;
        final long[] keys = closedKeys;
        closedKeys = currentKeys;
        currentKeys = keys;
        isGroupOpen = false;
    }

    private void startGroup(Record record) {
        final long[] keys = currentKeys;
        for (int i = 0, n = keyColumns.length; i < n; i++) {
            keys[i] = readKey(record, keyColumns[i], keyTypes[i]);
        }
        final SimpleMapValue value = currentValue;
        value.clear();
        if (updater != null) {
            updater.updateNew(value, record, rowId);
        } else {
            for (int i = 0; i < groupByFunctionsCount; i++) {
                groupByFunctions.getQuick(i).computeFirst(value, record, rowId);
            }
        }
        rowId++;
        isGroupOpen = true;
    }

    /**
     * The closed group as the serial GROUP BY's map record lays it out: the value slots, then the
     * keys.
     */
    private class GroupRecord implements Record {
        @Override
        public boolean getBool(int col) {
            return col < valueCount ? closedValue.getBool(col) : closedKeys[col - valueCount] != 0;
        }

        @Override
        public byte getByte(int col) {
            return col < valueCount ? closedValue.getByte(col) : (byte) closedKeys[col - valueCount];
        }

        @Override
        public char getChar(int col) {
            return col < valueCount ? closedValue.getChar(col) : (char) closedKeys[col - valueCount];
        }

        @Override
        public long getDate(int col) {
            return col < valueCount ? closedValue.getDate(col) : closedKeys[col - valueCount];
        }

        @Override
        public double getDouble(int col) {
            return col < valueCount ? closedValue.getDouble(col) : Double.longBitsToDouble(closedKeys[col - valueCount]);
        }

        @Override
        public float getFloat(int col) {
            return col < valueCount ? closedValue.getFloat(col) : Float.intBitsToFloat((int) closedKeys[col - valueCount]);
        }

        @Override
        public byte getGeoByte(int col) {
            return col < valueCount ? closedValue.getGeoByte(col) : (byte) closedKeys[col - valueCount];
        }

        @Override
        public int getGeoInt(int col) {
            return col < valueCount ? closedValue.getGeoInt(col) : (int) closedKeys[col - valueCount];
        }

        @Override
        public long getGeoLong(int col) {
            return col < valueCount ? closedValue.getGeoLong(col) : closedKeys[col - valueCount];
        }

        @Override
        public short getGeoShort(int col) {
            return col < valueCount ? closedValue.getGeoShort(col) : (short) closedKeys[col - valueCount];
        }

        @Override
        public int getIPv4(int col) {
            return col < valueCount ? closedValue.getIPv4(col) : (int) closedKeys[col - valueCount];
        }

        @Override
        public int getInt(int col) {
            return col < valueCount ? closedValue.getInt(col) : (int) closedKeys[col - valueCount];
        }

        @Override
        public long getLong(int col) {
            return col < valueCount ? closedValue.getLong(col) : closedKeys[col - valueCount];
        }

        @Override
        public short getShort(int col) {
            return col < valueCount ? closedValue.getShort(col) : (short) closedKeys[col - valueCount];
        }

        @Override
        public long getTimestamp(int col) {
            return col < valueCount ? closedValue.getTimestamp(col) : closedKeys[col - valueCount];
        }
    }
}
