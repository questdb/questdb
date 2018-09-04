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
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapFactory;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.columns.*;
import com.questdb.griffin.engine.functions.constants.DoubleConstant;
import com.questdb.griffin.engine.functions.constants.FloatConstant;
import com.questdb.griffin.engine.functions.constants.IntConstant;
import com.questdb.griffin.engine.functions.constants.LongConstant;
import com.questdb.griffin.engine.table.EmptyTableRecordCursor;
import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.QueryColumn;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNullRecordCursorFactory implements RecordCursorFactory {

    private final Map map;
    private final RecordCursorFactory base;
    private final SampleByFillNullRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final RecordSink mapSink;
    private final RecordMetadata metadata;

    public SampleByFillNullRecordCursorFactory(
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
        final ObjList<Function> placeholderFunctions = new ObjList<>(columnCount);
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

        // when we have same column several times in a row
        // we only add it once to map keys
        int lastIndex = -1;
        for (int i = 0; i < columnCount; i++) {
            final QueryColumn column = model.getColumns().getQuick(i);
            final ExpressionNode node = column.getAst();
            final int type;

            if (node.type == ExpressionNode.LITERAL) {
                // this is key
                int index = metadata.getColumnIndex(node.token);
                type = metadata.getColumnType(index);
                if (index != timestampIndex) {
                    if (lastIndex != index) {
                        listColumnFilter.add(index);
                        keyTypes.add(type);
                        keyColumnIndex++;
                        lastIndex = index;
                    }

                    final Function fun;
                    switch (type) {
                        case ColumnType.BOOLEAN:
                            fun = new BooleanColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.BYTE:
                            fun = new ByteColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.SHORT:
                            fun = new ShortColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.INT:
                            fun = new IntColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.LONG:
                            fun = new LongColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.FLOAT:
                            fun = new FloatColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.DOUBLE:
                            fun = new DoubleColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.STRING:
                            fun = new StrColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.SYMBOL:
                            symbolTableIndex.put(keyColumnIndex - 1, index);
                            fun = new MapSymbolColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.DATE:
                            fun = new DateColumn(node.position, keyColumnIndex - 1);
                            break;
                        case ColumnType.TIMESTAMP:
                            fun = new TimestampColumn(node.position, keyColumnIndex - 1);
                            break;
                        default:
                            fun = new BinColumn(node.position, keyColumnIndex - 1);
                            break;
                    }

                    recordFunctions.add(fun);
                    placeholderFunctions.add(fun);

                } else {
                    // set this function to null, cursor will replace it with an instance class
                    // timestamp function returns value of class member which makes it impossible
                    // to create these columns in advance of cursor instantiation
                    recordFunctions.add(null);
                    placeholderFunctions.add(null);
                    if (groupByMetadata.getTimestampIndex() == -1) {
                        groupByMetadata.setTimestampIndex(i);
                    }
                    assert type == ColumnType.TIMESTAMP;
                }
            } else {
                // add group-by function as a record function as well
                // so it can produce column values
                final GroupByFunction groupByFunction = groupByFunctions.getQuick(valueColumnIndex++);
                recordFunctions.add(groupByFunction);
                type = groupByFunction.getType();
                switch (type) {
                    case ColumnType.INT:
                        placeholderFunctions.add(new IntConstant(groupByFunction.getPosition(), Numbers.INT_NaN));
                        break;
                    case ColumnType.LONG:
                        placeholderFunctions.add(new LongConstant(groupByFunction.getPosition(), Numbers.LONG_NaN));
                        break;
                    case ColumnType.FLOAT:
                        placeholderFunctions.add(new FloatConstant(groupByFunction.getPosition(), Float.NaN));
                        break;
                    case ColumnType.DOUBLE:
                        placeholderFunctions.add(new DoubleConstant(groupByFunction.getPosition(), Double.NaN));
                        break;
                    default:
                        assert false;
                }
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
        this.cursor = new SampleByFillNullRecordCursor(
                map,
                mapSink,
                groupByFunctions,
                recordFunctions,
                placeholderFunctions,
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

}
