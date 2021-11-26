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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.UpdateModel;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

public class UpdateStatementBuilder implements RecordCursorFactory, Closeable {

    private final int position;
    private RecordMetadata setValuesMetadata;
    private ObjList<Function> setValuesFunctions;
    private Function masterFilter;
    private Function postJoinFilter;
    private RecordCursorFactory rowIdFactory;
    private RecordMetadata joinMetadata;
    private UpdateStatementMasterCursorFactory joinRecordCursorFactory;
    private IntList selectChooseColumnMaps;

    public UpdateStatementBuilder(int position, RecordCursorFactory noSelectFactory) {
        this.position = position;
        this.rowIdFactory = noSelectFactory;
    }

    public UpdateStatement buildUpdate(
            UpdateModel updateModel,
            TableReaderMetadata updateTableMetadata,
            BindVariableService bindVariableService
    ) throws SqlException {
        if (joinRecordCursorFactory == null && setValuesFunctions == null && selectChooseColumnMaps == null) {
            // This is update of column to the same values, e.g. changing nothing
            return UpdateStatement.EMPTY;
        }

        // Check that virtualColumnFunctions match types of updateTableMetadata
        RecordMetadata valuesResultMetadata = getMetadata();
        boolean implicitCastNeeded = false;

        for (int i = 0, n = valuesResultMetadata.getColumnCount(); i < n; i++) {
            int virtualColumnType = valuesResultMetadata.getColumnType(i);
            CharSequence updateColumnName = valuesResultMetadata.getColumnName(i);
            int columnType = updateTableMetadata.getColumnType(updateColumnName);

            if (virtualColumnType != columnType) {
                implicitCastNeeded = true;
                Function virtualColumn = setValuesFunctions != null ? setValuesFunctions.get(i) : null;
                if (!implicitCastAllowed(virtualColumnType, columnType, virtualColumn, bindVariableService)) {
                    // get column position
                    ExpressionNode setRhs = updateModel.getUpdateColumnExpressions().get(i);
                    int position = setRhs.position;
                    throw SqlException.inconvertibleTypes(position, virtualColumnType, "", columnType, updateColumnName);
                }
            }
        }

        RecordColumnMapper columnMapper;
        if (setValuesFunctions != null) {
            columnMapper = new FunctionsColumnMapper().of(setValuesFunctions);
        } else if (selectChooseColumnMaps != null) {
            if (!implicitCastNeeded) {
                columnMapper = new IndexColumnMapper().of(selectChooseColumnMaps);
            } else {
                columnMapper = createColumnCastColumnMapper(selectChooseColumnMaps, valuesResultMetadata);
            }
        } else {
            throw SqlException.$(position, "unsupported column mapping for update");
        }

        UpdateStatement updateStatement = new UpdateStatement(
                updateModel.getUpdateTableName(),
                updateModel.getPosition(),
                rowIdFactory,
                masterFilter,
                postJoinFilter,
                valuesResultMetadata,
                joinRecordCursorFactory,
                columnMapper
        );

        // Closing responsibility is within resulting updateStatement
        rowIdFactory = null;
        masterFilter = null;
        postJoinFilter = null;
        setValuesFunctions = null;
        setValuesMetadata = null;
        joinRecordCursorFactory = null;

        return updateStatement;
    }

    private RecordColumnMapper createColumnCastColumnMapper(
            IntList inputColumnIndexMap,
            RecordMetadata valuesResultMetadata
    ) {
        int columnCount = valuesResultMetadata.getColumnCount();
        ObjList<Function> columnFunctions = new ObjList<>(columnCount);
        for(int i = 0; i < columnCount; i++) {
            int virtualColumnType = valuesResultMetadata.getColumnType(i);
            int inputUnmappedIndex = inputColumnIndexMap.get(i);

            Function columnFunction = createColumnFunction(inputUnmappedIndex, virtualColumnType);
            columnFunctions.add(columnFunction);
        }
        return new FunctionsColumnMapper().of(columnFunctions);
    }

    private Function createColumnFunction(int inputIndex, int inputType) {
        switch (inputType) {
            case ColumnType.INT:
                return new IntColumn(inputIndex);
            case ColumnType.SHORT:
                return new ShortColumn(inputIndex);
            case ColumnType.FLOAT:
                return new FloatColumn(inputIndex);
            case ColumnType.LONG:
                return new LongColumn(inputIndex);
            case ColumnType.BYTE:
                return new ByteColumn(inputIndex);
            case ColumnType.CHAR:
                return new CharColumn(inputIndex);
            case ColumnType.TIMESTAMP:
                return new TimestampColumn(inputIndex);
            case ColumnType.DATE:
                return new DateColumn(inputIndex);
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(setValuesFunctions);
        masterFilter = Misc.free(masterFilter);
        postJoinFilter = Misc.free(postJoinFilter);
        rowIdFactory = Misc.free(rowIdFactory);
        setValuesMetadata = Misc.free(setValuesMetadata);
        joinRecordCursorFactory = Misc.free(joinRecordCursorFactory);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordMetadata getMetadata() {
        return setValuesMetadata != null ? setValuesMetadata : (joinMetadata != null ? joinMetadata : rowIdFactory.getMetadata());
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    public UpdateStatementBuilder withFilter(Function filter) throws SqlException {
        if (joinRecordCursorFactory == null) {
            if (masterFilter != null) {
                throw SqlException.$(position, "Post join filter composing is not supported");
            }
            masterFilter = filter;
        } else {
            if (postJoinFilter != null) {
                throw SqlException.$(position, "Post join filter composing is not supported");
            }
            postJoinFilter = filter;
        }
        return this;
    }

    public UpdateStatementBuilder withJoin(UpdateStatementMasterCursorFactory updateRecordFactory) {
        this.joinRecordCursorFactory = updateRecordFactory;
        this.joinMetadata = updateRecordFactory.getMetadata();
        return this;
    }

    public UpdateStatementBuilder withSelectChoose(GenericRecordMetadata selectChooseMetadata, IntList selectChooseColumnMaps) {
        this.setValuesMetadata = selectChooseMetadata;
        this.selectChooseColumnMaps = selectChooseColumnMaps;
        return this;
    }

    public UpdateStatementBuilder withSelectVirtual(RecordMetadata virtualMetadata, ObjList<Function> virtualFunctions) throws SqlException {
        if (setValuesMetadata != null) {
            throw SqlException.$(position, "UPDATE of this shape not supported. Cannot compose select-virtual and select-choose");
        }
        this.setValuesMetadata = virtualMetadata;
        this.setValuesFunctions = virtualFunctions;
        return this;
    }

    private boolean implicitCastAllowed(int fromColumnType, int toColumnType, Function fromFunction, BindVariableService bindVariableService) throws SqlException {
        switch (fromColumnType) {
            case ColumnType.NULL:
                return true;
            case ColumnType.UNDEFINED:
                // Bind variables
                fromFunction.assignType(toColumnType, bindVariableService);
                return true;
            case ColumnType.BYTE:
                if (toColumnType == ColumnType.SHORT) {
                    return true;
                }
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                if (toColumnType == ColumnType.INT || toColumnType == ColumnType.CHAR || toColumnType == ColumnType.SHORT) {
                    return true;
                }
            case ColumnType.INT:
                return toColumnType == ColumnType.LONG || toColumnType == ColumnType.FLOAT || toColumnType == ColumnType.DOUBLE;
            case ColumnType.LONG:
                return toColumnType == ColumnType.TIMESTAMP || toColumnType == ColumnType.DOUBLE || toColumnType == ColumnType.FLOAT;
            case ColumnType.FLOAT:
                return toColumnType == ColumnType.DOUBLE;
            case ColumnType.TIMESTAMP:
                return toColumnType == ColumnType.LONG;
            case ColumnType.DATE:
                return toColumnType == ColumnType.TIMESTAMP;

        }
        return false;
    }
}
