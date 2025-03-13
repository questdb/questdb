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
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlOptimiser;
import io.questdb.griffin.SqlParser;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.mp.SCSequence;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Create mat view operation relies on implicit create table as select operation.
 * <p>
 * The supported clauses are the following:
 * - index
 * - timestamp
 * - partition by
 * - ttl
 * - in volume
 * <p>
 * Other than that, at the execution phase the query is compiled and optimized
 * and validated. All columns are added to the create table operation
 * and key SAMPLE BY columns and timestamp are marked as dedup keys. Sampling interval
 * and unit are also parsed at this stage as we want to support GROUP BY timestamp_floor(ts)
 * queries.
 */
public class CreateMatViewOperationImpl implements CreateMatViewOperation {
    private final CharSequenceHashSet baseKeyColumnNames = new CharSequenceHashSet();
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    private final MatViewDefinition matViewDefinition = new MatViewDefinition();
    private final int refreshType;
    private final String sqlText;
    private final String timeZone;
    private final String timeZoneOffset;
    private String baseTableName;
    private CreateTableOperationImpl createTableOperation;
    private long samplingInterval;
    private char samplingIntervalUnit;
    private CharSequenceHashSet tableNames;

    public CreateMatViewOperationImpl(
            @NotNull String sqlText,
            @NotNull CreateTableOperationImpl createTableOperation,
            int refreshType,
            @Nullable String baseTableName,
            @Nullable String timeZone,
            @Nullable String timeZoneOffset
    ) {
        this.sqlText = sqlText;
        this.createTableOperation = createTableOperation;
        this.refreshType = refreshType;
        this.baseTableName = baseTableName;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
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

    @Override
    public int getRefreshType() {
        return refreshType;
    }

    @Override
    public CharSequence getSqlText() {
        return sqlText;
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
    public CharSequence getVolumeAlias() {
        return createTableOperation.getVolumeAlias();
    }

    @Override
    public boolean ignoreIfExists() {
        return createTableOperation.ignoreIfExists();
    }

    @Override
    public void init(TableToken matViewToken) {
        matViewDefinition.init(
                refreshType,
                matViewToken,
                Chars.toString(createTableOperation.getSelectText()),
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
    public void validateAndUpdateMetadataFromModel(
            SqlExecutionContext sqlExecutionContext,
            SqlOptimiser optimiser,
            QueryModel queryModel
    ) throws SqlException {
        // Create view columns based on query.
        final ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        assert columns.size() > 0;

        // We do not know types of columns at this stage.
        // Compiler must put table together using query metadata.
        createColumnModelMap.clear();
        final LowerCaseCharSequenceObjHashMap<TableColumnMetadata> augColumnMetadataMap = createTableOperation.getAugmentedColumnMetadata();
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
                throw SqlException.position(timestampPos).put("TIMESTAMP column does not exist [name=").put(timestamp).put(']');
            }
            final int timestampType = timestampModel.getColumnType();
            // type can be -1 for create table as select because types aren't known yet
            if (timestampType != ColumnType.TIMESTAMP && timestampType != ColumnType.UNDEFINED) {
                throw SqlException.position(timestampPos).put("TIMESTAMP column expected [actual=").put(ColumnType.nameOf(timestampType)).put(']');
            }
            timestampModel.setIsDedupKey(); // set dedup for timestamp column
        }

        // Find base table name if not set explicitly.
        if (baseTableName == null) {
            if (tableNames == null) {
                tableNames = new CharSequenceHashSet();
            }
            tableNames.clear();
            SqlParser.collectTables(queryModel, tableNames);
            if (tableNames.size() < 1) {
                throw SqlException.$(0, "missing base table, materialized views have to be based on a table");
            }
            if (tableNames.size() > 1) {
                throw SqlException.$(0, "more than one table used in query, base table has to be set using 'WITH BASE'");
            }
            baseTableName = Chars.toString(tableNames.get(0));
        }

        final TableToken baseTableToken = sqlExecutionContext.getTableTokenIfExists(baseTableName);
        if (baseTableToken == null) {
            throw SqlException.tableDoesNotExist(0, baseTableName);
        }
        if (!baseTableToken.isWal()) {
            throw SqlException.$(0, "base table has to be WAL enabled");
        }

        // Find sampling interval.
        ExpressionNode intervalExpr = null;
        final ExpressionNode sampleBy = queryModel.getSampleBy();
        // Vanilla SAMPLE BY.
        if (sampleBy != null && sampleBy.type == ExpressionNode.CONSTANT) {
            intervalExpr = sampleBy;
        }

        // GROUP BY timestamp_floor(ts) (optimized SAMPLE BY).
        if (intervalExpr == null) {
            final ObjList<QueryColumn> queryColumns = queryModel.getBottomUpColumns();
            for (int i = 0, n = queryColumns.size(); i < n; i++) {
                final QueryColumn queryColumn = queryColumns.getQuick(i);
                final ExpressionNode ast = queryColumn.getAst();
                if (ast.type == ExpressionNode.FUNCTION && Chars.equalsIgnoreCase("timestamp_floor", ast.token)) {
                    if (ast.paramCount == 3) {
                        intervalExpr = ast.args.getQuick(2);
                    } else {
                        intervalExpr = ast.lhs;
                    }
                    if (timestamp == null) {
                        createTableOperation.setTimestampColumnName(Chars.toString(queryColumn.getName()));
                        createTableOperation.setTimestampColumnNamePosition(ast.position);
                        final CreateTableColumnModel timestampModel = createColumnModelMap.get(queryColumn.getName());
                        if (timestampModel == null) {
                            throw SqlException.position(ast.position).put("TIMESTAMP column does not exist [name=").put(queryColumn.getName()).put(']');
                        }
                        timestampModel.setIsDedupKey(); // set dedup for timestamp column
                    }
                    break;
                }
            }

            // We haven't found timestamp_floor() in SELECT.
            if (intervalExpr == null) {
                throw SqlException.$(0, "TIMESTAMP column is not present in select list");
            }
        }

        // Parse sampling interval expression.
        final CharSequence interval = GenericLexer.unquote(intervalExpr.token);
        final int samplingIntervalEnd = TimestampSamplerFactory.findIntervalEndIndex(interval, intervalExpr.position);
        assert samplingIntervalEnd < interval.length();
        samplingInterval = TimestampSamplerFactory.parseInterval(interval, samplingIntervalEnd, intervalExpr.position);
        assert samplingInterval > 0;
        samplingIntervalUnit = interval.charAt(samplingIntervalEnd);

        // Check if PARTITION BY wasn't specified in SQL, so that we need
        // to assign it based on the sampling interval.
        if (createTableOperation.getPartitionBy() == PartitionBy.NONE) {
            final TimestampSampler timestampSampler = TimestampSamplerFactory.getInstance(
                    samplingInterval,
                    samplingIntervalUnit,
                    0
            );
            final long approxBucketMicros = timestampSampler.getApproxBucketSize();
            int partitionBy = PartitionBy.DAY;
            if (approxBucketMicros > Timestamps.HOUR_MICROS) {
                partitionBy = PartitionBy.YEAR;
            } else if (approxBucketMicros > Timestamps.MINUTE_MICROS) {
                partitionBy = PartitionBy.WEEK;
            }
            createTableOperation.setPartitionBy(partitionBy);
            final int ttlHoursOrMonths = createTableOperation.getTtlHoursOrMonths();
            if (ttlHoursOrMonths > 0) {
                // Don't forget to validate TTL against PARTITION BY.
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, 0);
            }
        }

        // Mark key columns as dedup keys.
        baseKeyColumnNames.clear();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            if (!optimiser.hasAggregates(column.getAst())) {
                // SAMPLE BY/GROUP BY key, add as dedup key.
                final CreateTableColumnModel model = createColumnModelMap.get(column.getName());
                if (model == null) {
                    throw SqlException.$(0, "missing column [name=" + column.getName() + "]");
                }
                model.setIsDedupKey();
                // Copy column names into builder to be validated later.
                copyBaseTableColumnNames(column.getAst(), queryModel, baseTableName, baseKeyColumnNames);
            }
        }

        // Don't forget to reset augmented columns in create table op with what we have scraped.
        createTableOperation.initColumnMetadata(createColumnModelMap);
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
                final CharSequence baseKeyColumnName = baseKeyColumnNames.get(i);
                final int baseKeyColumnIndex = baseTableMetadata.getColumnIndexQuiet(baseKeyColumnName);
                if (baseKeyColumnIndex > -1 && !baseTableMetadata.isDedupKey(baseKeyColumnIndex)) {
                    throw SqlException.position(0)
                            .put("key column must be one of base table's dedup keys [name=").put(baseKeyColumnName).put(']');
                }
            }
        }
    }

    /**
     * Copies base table column names present in the given node into the target set.
     * The node may contain multiple columns/aliases, e.g. `concat(sym1, sym2)`, which are searched
     * down to their names in the base table.
     * <p>
     * Used to find the list of base table columns used in mat view query keys (best effort validation).
     */
    private static void copyBaseTableColumnNames(
            ExpressionNode node,
            QueryModel model,
            CharSequence baseTableName,
            CharSequenceHashSet target
    ) throws SqlException {
        if (node != null && model != null) {
            if (node.type == ExpressionNode.LITERAL) {
                if (model.getTableName() != null) {
                    // We've found a lowest-level model. Let's check if the column belongs to it.
                    final int dotIndex = Chars.indexOf(node.token, '.');
                    if (dotIndex > -1) {
                        if (Chars.equalsIgnoreCase(model.getName(), node.token, 0, dotIndex)) {
                            if (!Chars.equalsIgnoreCase(model.getTableName(), baseTableName)) {
                                throw SqlException.$(node.position, "only base table columns can be used as keys: ").put(node.token);
                            }
                            target.add(Chars.toString(node.token, dotIndex + 1, node.token.length()));
                            return;
                        }
                    } else {
                        if (!Chars.equalsIgnoreCase(model.getTableName(), baseTableName)) {
                            throw SqlException.$(node.position, "only base table columns can be used as keys: ").put(node.token);
                        }
                        target.add(node.token);
                        return;
                    }
                } else {
                    // Check nested model.
                    final QueryColumn column = model.getAliasToColumnMap().get(node.token);
                    copyBaseTableColumnNames(column != null ? column.getAst() : node, model.getNestedModel(), baseTableName, target);
                }
            }

            // Check node children for functions/operators.
            for (int i = 0, n = node.args.size(); i < n; i++) {
                copyBaseTableColumnNames(node.args.getQuick(i), model, baseTableName, target);
            }
            if (node.lhs != null) {
                copyBaseTableColumnNames(node.lhs, model, baseTableName, target);
            }
            if (node.rhs != null) {
                copyBaseTableColumnNames(node.rhs, model, baseTableName, target);
            }

            // Check join models.
            for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
                copyBaseTableColumnNames(node, model.getJoinModels().getQuick(i), baseTableName, target);
            }
        }
    }
}
