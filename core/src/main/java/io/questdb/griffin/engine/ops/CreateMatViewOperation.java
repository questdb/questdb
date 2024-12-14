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

import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MaterializedViewDefinition;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import org.jetbrains.annotations.Nullable;

public class CreateMatViewOperation implements TableStructure, Operation {
    private final String baseTableName;
    private final CreateTableOperation createTableOperation;
    private final long fromMicros;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final String timeZone;
    private final String timeZoneOffset;
    private final long toMicros;
    private final String viewSql;
    private MaterializedViewDefinition matViewDefinition;

    public CreateMatViewOperation(
            CreateTableOperation createTableOperation,
            String baseTableName,
            long fromMicros,
            long samplingInterval,
            char samplingIntervalUnit,
            String timeZone,
            String timeZoneOffset,
            long toMicros,
            String viewSql
    ) {
        this.createTableOperation = createTableOperation;
        this.baseTableName = baseTableName;
        this.fromMicros = fromMicros;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
        this.toMicros = toMicros;
        this.viewSql = viewSql;
    }

    @Override
    public void close() {
        createTableOperation.close();
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try (SqlCompiler compiler = sqlExecutionContext.getCairoEngine().getSqlCompiler()) {
            compiler.execute(this, sqlExecutionContext);
        }
        return createTableOperation.getOperationFuture();
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

    public CreateTableOperation getCreateTableOperation() {
        return createTableOperation;
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return createTableOperation.getIndexBlockCapacity(columnIndex);
    }

    @Override
    public MaterializedViewDefinition getMatViewDefinition() {
        return matViewDefinition;
    }

    @Override
    public int getMaxUncommittedRows() {
        return createTableOperation.getMaxUncommittedRows();
    }

    @Override
    public long getMetadataVersion() {
        return createTableOperation.getMetadataVersion();
    }

    @Override
    public long getO3MaxLag() {
        return createTableOperation.getO3MaxLag();
    }

    @Override
    public int getOperationCode() {
        return OperationCodes.CREATE_MAT_VIEW;
    }

    @Override
    public OperationFuture getOperationFuture() {
        return createTableOperation.getOperationFuture();
    }

    @Override
    public int getPartitionBy() {
        return createTableOperation.getPartitionBy();
    }

    public CharSequence getSqlText() {
        return createTableOperation.getSqlText();
    }

    @Override
    public boolean getSymbolCacheFlag(int index) {
        return createTableOperation.getSymbolCacheFlag(index);
    }

    @Override
    public int getSymbolCapacity(int index) {
        return createTableOperation.getSymbolCapacity(index);
    }

    @Override
    public CharSequence getTableName() {
        return createTableOperation.getTableName();
    }

    public int getTableNamePosition() {
        return createTableOperation.getTableNamePosition();
    }

    @Override
    public int getTimestampIndex() {
        return createTableOperation.getTimestampIndex();
    }

    public String getViewSql() {
        return viewSql;
    }

    public CharSequence getVolumeAlias() {
        return createTableOperation.getVolumeAlias();
    }

    public boolean ignoreIfExists() {
        return createTableOperation.ignoreIfExists();
    }

    @Override
    public void init(TableToken tableToken) {
        matViewDefinition = new MaterializedViewDefinition(
                tableToken, viewSql, baseTableName, samplingInterval, samplingIntervalUnit,
                fromMicros, toMicros, timeZone, timeZoneOffset
        );
    }

    @Override
    public boolean isDedupKey(int index) {
        return createTableOperation.isDedupKey(index);
    }

    @Override
    public boolean isIndexed(int index) {
        return createTableOperation.isIndexed(index);
    }

    @Override
    public boolean isMatView() {
        return true;
    }

    @Override
    public boolean isWalEnabled() {
        return createTableOperation.isWalEnabled();
    }

    /**
     * This is SQLCompiler side API to set table token after the operation has been executed.
     *
     * @param tableToken table token of the newly created table
     */
    public void updateOperationFutureTableToken(TableToken tableToken) {
        createTableOperation.updateOperationFutureTableToken(tableToken);
    }

    public void validateAndUpdateMetadataFromSelect(RecordMetadata metadata) throws SqlException {
        createTableOperation.validateAndUpdateMetadataFromSelect(metadata);

        // validate that all indexes are specified only on columns with symbol type
//        for (int i = 0, n = model.getColumnCount(); i < n; i++) {
//            CharSequence columnName = model.getColumnName(i);
//            int index = metadata.getColumnIndexQuiet(columnName);
//            assert index > -1 : "wtf2? " + columnName;
//            if (!ColumnType.isSymbol(metadata.getColumnType(index)) && model.isIndexed(i)) {
//                throw SqlException.$(0, "indexes are supported only for SYMBOL columns: ").put(columnName);
//            }
//
//            if (ColumnType.isNull(metadata.getColumnType(index))) {
//                throw SqlException.$(0, "cannot create NULL-type column, please use type cast, e.g. ").put(columnName).put("::").put("type");
//            }
//        }

        // Validate designated timestamp column
//        ExpressionNode timestamp;
//        if (metadata.getTimestampIndex() != -1) {
//            if (model.getTimestampIndex() != -1) {
//                if (model.getTimestampIndex() != metadata.getTimestampIndex()) {
//                    // TODO: check that these timestamp column are equivalent
//                }
//                timestamp = model.getTimestamp();
//            } else {
//                timestamp = model.getQueryModel().getBottomUpColumns().get(metadata.getTimestampIndex()).getAst();
//                model.setTimestamp(timestamp);
//            }
//        } else {
//            if (model.getTimestampIndex() == -1) {
//                // Designated timestamp does not exist at query factory and not in the query model
//                throw SqlException.position(0).put("Designated timestamp required");
//            } else {
//                timestamp = model.getQueryModel().getBottomUpColumns().get(metadata.getTimestampIndex()).getAst();
//                model.setTimestamp(timestamp);
//            }
//        }

        // validate type of timestamp column
//        if (metadata.getColumnType(timestamp.token) != ColumnType.TIMESTAMP) {
//            throw SqlException.position(timestamp.position).put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(metadata.getColumnType(timestamp.token))).put(']');
//        }

//        if (!PartitionBy.isPartitioned(model.getPartitionBy())) {
//            throw SqlException.position(0).put("Materialized view has to be partitioned");
//        }
    }
}
