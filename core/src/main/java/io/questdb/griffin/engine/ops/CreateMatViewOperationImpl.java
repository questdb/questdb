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
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

public class CreateMatViewOperationImpl implements CreateMatViewOperation {
    private final ObjList<String> baseKeyColumnNames = new ObjList<>();
    private final String baseTableName;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final String timeZone;
    private final String timeZoneOffset;
    private final String viewSql;
    private CreateTableOperation createTableOperation;
    private MatViewDefinition matViewDefinition;

    public CreateMatViewOperationImpl(
            CreateTableOperation createTableOperation,
            String baseTableName,
            @Transient CharSequenceHashSet baseKeyColumnNames,
            long samplingInterval,
            char samplingIntervalUnit,
            String timeZone,
            String timeZoneOffset,
            String viewSql
    ) {
        this.createTableOperation = createTableOperation;
        this.baseTableName = baseTableName;
        for (int i = 0, n = baseKeyColumnNames.getList().size(); i < n; i++) {
            CharSequence colName = baseKeyColumnNames.getList().get(i);
            this.baseKeyColumnNames.add(Chars.toString(colName));
        }
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
        this.viewSql = viewSql;
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
        return createTableOperation.getOperationFuture();
    }

    @Override
    public CharSequence getBaseTableName() {
        return baseTableName;
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
        return createTableOperation.getIndexBlockCapacity(columnIndex);
    }

    @Override
    public MatViewDefinition getMatViewDefinition() {
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

    @Override
    public int getTtlHoursOrMonths() {
        return createTableOperation.getTtlHoursOrMonths();
    }

    @Override
    public CharSequence getVolumeAlias() {
        return createTableOperation.getVolumeAlias();
    }

    @Override
    public boolean ignoreIfExists() {
        return createTableOperation.ignoreIfExists();
    }

    @Override
    public void init(TableToken matViewToken) {
        matViewDefinition = new MatViewDefinition(
                matViewToken,
                viewSql,
                baseTableName,
                samplingInterval,
                samplingIntervalUnit,
                timeZone,
                timeZoneOffset
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
    public void validateAndUpdateMetadataFromSelect(RecordMetadata selectMetadata, TableReaderMetadata baseTableMetadata) throws SqlException {
        // SELECT validation
        createTableOperation.validateAndUpdateMetadataFromSelect(selectMetadata);
        // Key column validation (best effort):
        // Option 1. Base table has no dedup.
        //           Any key columns are fine in this case.
        // Option 2. Base table has dedup columns.
        //           Key columns in mat view query must be a subset of the base table's dedup columns.
        //           That's to avoid situation when dedup upsert rewrites key column values leading to
        //           inconsistent mat view data.
        boolean baseDedupEnabled = false;
        for (int i = 0, n = baseTableMetadata.getColumnCount(); i < n; i++) {
            if (baseTableMetadata.isDedupKey(i)) {
                baseDedupEnabled = true;
                break;
            }
        }
        if (baseDedupEnabled) {
            for (int i = 0, n = baseKeyColumnNames.size(); i < n; i++) {
                final String baseKeyColumnName = baseKeyColumnNames.getQuick(i);
                final int baseKeyColumnIndex = baseTableMetadata.getColumnIndexQuiet(baseKeyColumnName);
                if (baseKeyColumnIndex > -1 && !baseTableMetadata.isDedupKey(baseKeyColumnIndex)) {
                    throw SqlException.position(0)
                            .put("key column must be one of base table's dedup keys [name=").put(baseKeyColumnName).put(']');
                }
            }
        }
    }
}
