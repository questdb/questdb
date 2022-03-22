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

package io.questdb.griffin.update;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

public class UpdateStatement implements Closeable {
    private String tableName;
    private int tableId;
    private long tableVersion;
    private RecordCursorFactory updateToDataCursorFactory;
    private QueryModel selectQueryModel;
    private QueryModel updateQueryModel;
    private SqlCodeGenerator codeGenerator;
    private UpdateExecution updateExecution;
    private SqlExecutionContext executionContext;

    public UpdateStatement of(
            String tableName,
            int tableId,
            long tableVersion,
            QueryModel selectQueryModel,
            QueryModel updateQueryModel,
            SqlCodeGenerator codeGenerator,
            UpdateExecution updateExecution,
            SqlExecutionContext executionContext
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableVersion = tableVersion;
        this.selectQueryModel = selectQueryModel;
        this.updateQueryModel = updateQueryModel;
        this.codeGenerator = codeGenerator;
        this.updateExecution = updateExecution;
        this.executionContext = executionContext;
        return this;
    }

    @Override
    public void close() {
        updateToDataCursorFactory = Misc.free(updateToDataCursorFactory);
    }

    public int getTableId() {
        return tableId;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public long apply(TableWriter tableWriter) throws SqlException {
        return updateExecution.executeUpdate(tableWriter, this, executionContext);
    }

    public long getTableVersion() {
        return tableVersion;
    }

    public RecordCursorFactory prepareForUpdate() throws SqlException {
        final IntList tableColumnTypes = selectQueryModel.getUpdateTableColumnTypes();
        final ObjList<CharSequence> tableColumnNames = selectQueryModel.getUpdateTableColumnNames();

        updateToDataCursorFactory = codeGenerator.generate(selectQueryModel, executionContext);
        if (!updateToDataCursorFactory.supportsUpdateRowId(tableName)) {
            throw SqlException.$(updateQueryModel.getModelPosition(), "Only simple UPDATE statements without joins are supported");
        }

        // Check that updateDataFactoryMetadata match types of table to be updated exactly
        final RecordMetadata updateDataFactoryMetadata = updateToDataCursorFactory.getMetadata();
        for (int i = 0, n = updateDataFactoryMetadata.getColumnCount(); i < n; i++) {
            int virtualColumnType = updateDataFactoryMetadata.getColumnType(i);
            CharSequence updateColumnName = updateDataFactoryMetadata.getColumnName(i);
            int tableColumnIndex = tableColumnNames.indexOf(updateColumnName);
            int tableColumnType = tableColumnTypes.get(tableColumnIndex);

            if (virtualColumnType != tableColumnType) {
                // get column position
                ExpressionNode setRhs = updateQueryModel.getNestedModel().getColumns().getQuick(i).getAst();
                int position = setRhs.position;
                throw SqlException.inconvertibleTypes(position, virtualColumnType, "", tableColumnType, updateColumnName);
            }
        }
        return updateToDataCursorFactory;
    }
}
