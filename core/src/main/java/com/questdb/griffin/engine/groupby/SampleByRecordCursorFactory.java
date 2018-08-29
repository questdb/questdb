/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.*;
import com.questdb.cairo.map.*;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.functions.TimestampFunction;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.columns.IntColumn;
import com.questdb.griffin.engine.functions.columns.TimestampColumn;
import com.questdb.griffin.engine.table.EmptyTableRecordCursor;
import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.QueryColumn;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class SampleByRecordCursorFactory implements RecordCursorFactory {

    private final Map map;
    private final RecordCursorFactory base;
    private final SampleByRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final RecordSink mapSink;
    private final RecordMetadata metadata;

    public SampleByRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull QueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull FunctionParser functionParser,
            @Transient @NotNull SqlExecutionContext executionContext,
            @Transient @NotNull BytecodeAssembler asm) throws SqlException {
        final int columnCount = model.getColumns().size();
        final RecordMetadata metadata = base.getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        // fail?
        assert timestampIndex != -1;
        final ObjList<GroupByFunction> groupByFunctions = new ObjList<>(columnCount);
        final ObjList<Function> recordFunctions = new ObjList<>(columnCount);
        final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();

        // transient ?
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();

        listColumnFilter.clear();

        // first value is always timestamp
        valueTypes.add(ColumnType.LONG);

        // Process group-by functions first to get the idea of
        // how many map values we will have.
        // Map value count is needed to calculate offsets for
        // map key columns.

        for (int i = 0; i < columnCount; i++) {
            final QueryColumn column = model.getColumns().getQuick(i);
            ExpressionNode node = column.getAst();

            if (node.type != ExpressionNode.LITERAL) {
                // this can fail
                final Function function = functionParser.parseFunction(
                        column.getAst(),
                        metadata,
                        executionContext
                );

                // configure map value columns for group-by functions
                // some functions may need more than one column in values
                // so we have them do all the work
                assert function instanceof GroupByFunction;
                GroupByFunction func = (GroupByFunction) function;
                func.pushValueTypes(valueTypes);
                groupByFunctions.add(func);
            }
        }

        int keyColumnIndex = valueTypes.getColumnCount();
        int valueColumnIndex = 0;
        final IntIntHashMap symbolTableIndex = new IntIntHashMap();

        for (int i = 0; i < columnCount; i++) {
            final QueryColumn column = model.getColumns().getQuick(i);
            final ExpressionNode node = column.getAst();
            final int type;

            if (node.type == ExpressionNode.LITERAL) {
                // this is key
                int index = metadata.getColumnIndex(node.token);
                type = metadata.getColumnType(index);
                if (index != timestampIndex) {
                    listColumnFilter.add(index);
                    keyTypes.add(type);
                    switch (type) {
                        case ColumnType.INT:
                            recordFunctions.add(new IntColumn(node.position, keyColumnIndex));
                            break;
                        case ColumnType.SYMBOL:
                            symbolTableIndex.put(keyColumnIndex, index);
                            recordFunctions.add(new MapSymbolColumn(node.position, keyColumnIndex));
                            break;
                        case ColumnType.TIMESTAMP:
                            recordFunctions.add(new TimestampColumn(node.position, keyColumnIndex));
                            break;
                        default:
                            assert false;
                    }

                    keyColumnIndex++;
                } else {
                    // set this function to null, cursor will replace it with an instance class
                    // timestamp function returns value of class member which makes it impossible
                    // to create these columns in advance of cursor instantiation
                    recordFunctions.add(null);
                    groupByMetadata.setTimestampIndex(i);
                    assert type == ColumnType.TIMESTAMP;
                }
            } else {
                // add group-by function as a record function as well
                // so it can produce column values
                final GroupByFunction groupByFunction = groupByFunctions.getQuick(valueColumnIndex++);
                recordFunctions.add(groupByFunction);
                type = groupByFunction.getType();
            }

            // and finish with populating metadata for this factory
            groupByMetadata.add(new TableColumnMetadata(
                    Chars.toString(column.getName()),
                    type
            ));
        }

        // sink will be storing record columns to map key
        this.mapSink = RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false);
        // this is the map itself, which we must not forget to free when factory closes
        this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.base = base;
        this.cursor = new SampleByRecordCursor(
                map,
                mapSink,
                groupByFunctions,
                recordFunctions,
                timestampIndex,
                timestampSampler,
                symbolTableIndex
        );
        this.recordFunctions = recordFunctions;
        this.metadata = groupByMetadata;
        this.groupByFunctions = groupByFunctions;
    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        final RecordCursor baseCursor = base.getCursor(bindVariableService);
        map.clear();

        // This factory fills gaps in data. To do that we
        // have to know all possible key values. Essentially, every time
        // we sample we return same set of key values with different
        // aggregation results and timestamp

        int n = groupByFunctions.size();
        while (baseCursor.hasNext()) {
            MapKey key = map.withKey();
            mapSink.copy(baseCursor.next(), key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                // timestamp is always stored in value field 0
                value.putLong(0, Numbers.LONG_NaN);
                // have functions reset their columns to "zero" state
                // this would set values for when keys are not found right away
                for (int i = 0; i < n; i++) {
                    groupByFunctions.getQuick(i).zero(value);
                }
            }
        }

        // empty map? this means that base cursor was empty
        if (map.size() == 0) {
            return EmptyTableRecordCursor.INSTANCE;
        }

        // because we pass base cursor twice we have to go back to top
        // for the second run
        baseCursor.toTop();
        boolean next = baseCursor.hasNext();
        // we know base cursor has value
        assert next;
        cursor.of(baseCursor);

        // init all record function for this cursor, in case functions require metadata and/or symbol tables
        for (int i = 0, m = recordFunctions.size(); i < m; i++) {
            recordFunctions.getQuick(i).init(cursor, bindVariableService);
        }
        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private static class SampleByRecordCursor implements RecordCursor {
        private final Map map;
        private final RecordSink keyMapSink;
        private final ObjList<GroupByFunction> groupByFunctions;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final SampleByRecord record;
        private final IntIntHashMap symbolTableIndex;
        private RecordCursor base;
        private Iterator<MapRecord> mapIterator;
        private Record baseRecord;
        private long lastTimestamp;
        private long nextTimestamp;

        public SampleByRecordCursor(
                Map map,
                RecordSink keyMapSink,
                ObjList<GroupByFunction> groupByFunctions,
                ObjList<Function> recordFunctions,
                int timestampIndex, // index of timestamp column in base cursor
                TimestampSampler timestampSampler,
                IntIntHashMap symbolTableIndex) {
            this.map = map;
            this.groupByFunctions = groupByFunctions;
            this.timestampIndex = timestampIndex;
            this.keyMapSink = keyMapSink;
            this.timestampSampler = timestampSampler;
            this.record = new SampleByRecord(recordFunctions, map.getRecord());
            this.symbolTableIndex = symbolTableIndex;
            for (int i = 0, n = recordFunctions.size(); i < n; i++) {
                Function f = recordFunctions.getQuick(i);
                if (f == null) {
                    recordFunctions.setQuick(i, new TimestampFunc(0));
                }
            }
            this.mapIterator = map.iterator();
        }

        @Override
        public void close() {
            base.close();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(symbolTableIndex.get(columnIndex));
        }

        @Override
        public Record newRecord() {
            return null;
        }

        @Override
        public Record recordAt(long rowId) {
            return null;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void toTop() {
            this.base.toTop();
        }

        @Override
        public boolean hasNext() {
            //
            if (mapIterator.hasNext()) {
                // scroll down the map iterator
                // next() will return record that uses current map position
                return true;
            }

            if (baseRecord == null) {
                return false;
            }

            // key map has been flushed
            // before we build another one we need to check
            // for timestamp gaps

            // what is the next timestamp we are expecting?
            long nextTimestamp = timestampSampler.nextTimestamp(lastTimestamp);

            // is data timestamp ahead of next expected timestamp?
            if (this.nextTimestamp > nextTimestamp) {
                this.lastTimestamp = nextTimestamp;
                // reset iterator on map and stream contents
                return map.iterator().hasNext();
            }

            this.lastTimestamp = this.nextTimestamp;

            // looks like we need to populate key map

            int n = groupByFunctions.size();
            while (true) {
                long timestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
                if (lastTimestamp == timestamp) {
                    final MapKey key = map.withKey();
                    keyMapSink.copy(baseRecord, key);
                    final MapValue value = key.findValue();
                    assert value != null;

                    if (value.getLong(0) != timestamp) {
                        value.putLong(0, timestamp);
                        for (int i = 0; i < n; i++) {
                            groupByFunctions.getQuick(i).computeFirst(value, baseRecord);
                        }
                    } else {
                        for (int i = 0; i < n; i++) {
                            groupByFunctions.getQuick(i).computeNext(value, baseRecord);
                        }
                    }

                    // carry on with the loop if we still have data
                    if (base.hasNext()) {
                        base.next();
                        continue;
                    }

                    // we ran out of data, make sure hasNext() returns false at the next
                    // opportunity, after we stream map that is.
                    baseRecord = null;
                } else {
                    // timestamp changed, make sure we keep the value of 'lastTimestamp'
                    // unchanged. Timestamp columns uses this variable
                    // When map is exhausted we would assign 'nextTimestamp' to 'lastTimestamp'
                    // and build another map
                    this.nextTimestamp = timestamp;
                }

                return this.map.iterator().hasNext();
            }
        }

        @Override
        public Record next() {
            mapIterator.next();
            return record;
        }

        void of(RecordCursor base) {
            // factory guarantees that base cursor is not empty
            this.base = base;
            this.baseRecord = base.next();
            this.nextTimestamp = timestampSampler.round(baseRecord.getTimestamp(timestampIndex));
            this.lastTimestamp = this.nextTimestamp;
        }

        private class TimestampFunc extends TimestampFunction {

            public TimestampFunc(int position) {
                super(position);
            }

            @Override
            public long getTimestamp(Record rec) {
                return lastTimestamp;
            }
        }
    }

    private static class SampleByRecord implements Record {
        private final ObjList<Function> functions;
        private final Record record;

        public SampleByRecord(ObjList<Function> functions, Record record) {
            this.functions = functions;
            this.record = record;
        }

        @Override
        public double getDouble(int col) {
            return getFunc(col).getDouble(record);
        }

        @Override
        public int getInt(int col) {
            return getFunc(col).getInt(record);
        }

        @Override
        public CharSequence getSym(int col) {
            return getFunc(col).getSymbol(record);
        }

        @Override
        public long getTimestamp(int col) {
            return getFunc(col).getTimestamp(record);
        }

        private Function getFunc(int column) {
            return functions.get(column);
        }
    }
}
