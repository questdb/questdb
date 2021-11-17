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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.UpdateModel;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

public class UpdateStatementBuilder implements RecordCursorFactory, Closeable {
    private RecordMetadata setValuesMetadata;
    private ObjList<Function> setValuesFunctions;
    private Function filter;
    private RecordCursorFactory rowIdFactory;

    public UpdateStatementBuilder(RecordCursorFactory noSelectFactory) {
        this.rowIdFactory = noSelectFactory;
    }

    public UpdateStatement buildUpdate(
            UpdateModel updateModel,
            TableReaderMetadata updateTableMetadata,
            BindVariableService bindVariableService
    ) throws SqlException {
        // Check that virtualColumnFunctions match types of updateTableMetadata
        for (int i = 0, n = setValuesMetadata.getColumnCount(); i < n; i++) {
            Function virtualColumn = setValuesFunctions.get(i);
            int virtualColumnType = virtualColumn.getType();
            CharSequence updateColumnName = setValuesMetadata.getColumnName(i);
            int columnType = updateTableMetadata.getColumnType(updateColumnName);

            if (virtualColumnType != columnType && !implicitCastAllowed(virtualColumnType, columnType, virtualColumn, bindVariableService)) {
                // get column position
                ExpressionNode setRhs = updateModel.getUpdateColumnExpressions().get(i);
                int position = setRhs.position;
                throw SqlException.inconvertibleTypes(position, virtualColumnType, "", columnType, updateColumnName);
            }
        }

        UpdateStatement updateStatement = new UpdateStatement(
                updateModel.getUpdateTableName(),
                updateModel.getPosition(),
                rowIdFactory,
                filter,
                setValuesFunctions,
                setValuesMetadata
        );

        // Closing responsibility is within resulting updateStatement
        rowIdFactory = null;
        filter = null;
        setValuesFunctions = null;
        setValuesMetadata = null;

        return updateStatement;
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
            case ColumnType.SHORT:
                if (toColumnType == ColumnType.INT) {
                    return true;
                }
            case ColumnType.INT:
                return toColumnType == ColumnType.LONG || toColumnType == ColumnType.FLOAT || toColumnType == ColumnType.DOUBLE;
            case ColumnType.LONG:
                return toColumnType == ColumnType.TIMESTAMP || toColumnType == ColumnType.DOUBLE || toColumnType == ColumnType.FLOAT;
            case ColumnType.FLOAT:
                return toColumnType == ColumnType.DOUBLE;
        }
        return false;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordMetadata getMetadata() {
        return setValuesMetadata != null ? setValuesMetadata : rowIdFactory.getMetadata();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return rowIdFactory.recordCursorSupportsRandomAccess();
    }

    @Override
    public void close() {
        Misc.freeObjList(setValuesFunctions);
        Misc.free(filter);
        Misc.free(rowIdFactory);
        Misc.free(setValuesMetadata);
    }

    public UpdateStatementBuilder withFilter(Function filter) {
        this.filter = filter;
        return this;
    }

    public UpdateStatementBuilder withSelectVirtual(RecordMetadata virtualMetadata, ObjList<Function> virtualFunctions) {
        this.setValuesMetadata = virtualMetadata;
        this.setValuesFunctions = virtualFunctions;
        return this;
    }
}
