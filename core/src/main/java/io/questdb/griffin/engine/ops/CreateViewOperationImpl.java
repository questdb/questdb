/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CreateViewOperationImpl implements CreateViewOperation {
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    private final String sqlText;
    private final ViewDefinition viewDefinition = new ViewDefinition();
    private CreateTableOperationImpl createTableOperation;

    public CreateViewOperationImpl(
            @NotNull String sqlText,
            @NotNull CreateTableOperationImpl createTableOperation,
            @NotNull @Transient LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies
    ) {
        this.sqlText = sqlText;
        this.createTableOperation = createTableOperation;

        viewDefinition.getDependencies().putAll(dependencies);
        dependencies.clear();
    }

    @Override
    public void close() {
        createTableOperation = Misc.free(createTableOperation);
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return getOperationFuture();
    }

    @Override
    public int getColumnCount() {
        return createTableOperation.getColumnCount();
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        return createTableOperation.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        return createTableOperation.getColumnType(columnIndex);
    }

    @Override
    public CreateTableOperation getCreateTableOperation() {
        return createTableOperation;
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 0;
    }

    @Override
    public long getO3MaxLag() {
        return 0L;
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.CREATE_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return createTableOperation.getOperationFuture();
    }

    @Override
    public int getPartitionBy() {
        return PartitionBy.NOT_APPLICABLE;
    }

    @Override
    public CharSequence getSqlText() {
        return sqlText;
    }

    @Override
    public boolean getSymbolCacheFlag(int index) {
        return false;
    }

    @Override
    public int getSymbolCapacity(int index) {
        return 0;
    }

    @Override
    public CharSequence getTableName() {
        return createTableOperation.getTableName();
    }

    @Override
    public int getTableNamePosition() {
        return createTableOperation.getTableNamePosition();
    }

    @Override
    public int getTimestampIndex() {
        return createTableOperation.getTimestampIndex();
    }

    @Override
    public int getTtlHoursOrMonths() {
        return createTableOperation.getTtlHoursOrMonths();
    }

    @Override
    public ViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    @Override
    public boolean ignoreIfExists() {
        return createTableOperation.ignoreIfExists();
    }

    @Override
    public void init(TableToken viewToken) {
        viewDefinition.init(
                viewToken,
                Chars.toString(createTableOperation.getSelectText()),
                0L
        );
    }

    @Override
    public boolean isDedupKey(int index) {
        return createTableOperation.isDedupKey(index);
    }

    @Override
    public boolean isIndexed(int index) {
        return false;
    }

    @Override
    public boolean isView() {
        return true;
    }

    @Override
    public boolean isWalEnabled() {
        assert createTableOperation.isWalEnabled();
        return true;
    }

    /**
     * This is SQLCompiler side API to set table token after the operation has been executed.
     *
     * @param tableToken table token of the newly created table
     */
    @Override
    public void updateOperationFutureTableToken(TableToken tableToken) {
        createTableOperation.updateOperationFutureTableToken(tableToken);
    }

    @Override
    public void validateAndUpdateMetadataFromModel(
            SqlExecutionContext sqlExecutionContext,
            FunctionFactoryCache functionFactoryCache,
            QueryModel queryModel
    ) throws SqlException {
        // Create view columns based on query.
        final ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        assert columns.size() > 0;

        // We do not know types of columns at this stage.
        // Compiler must put table together using query metadata.
        createColumnModelMap.clear();
        final LowerCaseCharSequenceObjHashMap<TableColumnMetadata> augColumnMetadataMap =
                createTableOperation.getAugmentedColumnMetadata();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn qc = columns.getQuick(i);
            final CharSequence columnName = qc.getName();
            final CreateTableColumnModel model = CreateTableColumnModel.FACTORY.newInstance();
            model.setColumnNamePos(qc.getAst().position);
            model.setColumnType(ColumnType.UNDEFINED);
            // Copy index() definitions from create table op, so that we don't lose them.
            TableColumnMetadata augColumnMetadata = augColumnMetadataMap.get(columnName);
            if (augColumnMetadata != null && augColumnMetadata.isSymbolIndexFlag()) {
                model.setIndexed(true, qc.getAst().position, augColumnMetadata.getIndexValueBlockCapacity());
            }
            createColumnModelMap.put(columnName, model);
        }

        final String timestamp = createTableOperation.getTimestampColumnName();
        final int timestampPos = createTableOperation.getTimestampColumnNamePosition();
        if (timestamp != null) {
            final CreateTableColumnModel timestampModel = createColumnModelMap.get(timestamp);
            if (timestampModel == null) {
                throw SqlException.position(timestampPos)
                        .put("TIMESTAMP column does not exist [name=")
                        .put(timestamp).put(']');
            }
            final int timestampType = timestampModel.getColumnType();
            // type can be -1 for create table as select because types aren't known yet
            if (timestampType != ColumnType.TIMESTAMP && timestampType != ColumnType.UNDEFINED) {
                throw SqlException.position(timestampPos)
                        .put("TIMESTAMP column expected [actual=")
                        .put(ColumnType.nameOf(timestampType)).put(']');
            }
            timestampModel.setIsDedupKey(); // set dedup for timestamp column
        }

        // Don't forget to reset augmented columns in create table op with what we have scraped.
        createTableOperation.initColumnMetadata(createColumnModelMap);
    }

    @Override
    public void validateAndUpdateMetadataFromSelect(RecordMetadata selectMetadata, int scanDirection) throws SqlException {
        createTableOperation.validateAndUpdateMetadataFromSelect(selectMetadata, scanDirection);
    }
}
