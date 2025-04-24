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
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
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
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.model.ExpressionNode.FUNCTION;
import static io.questdb.griffin.model.ExpressionNode.LITERAL;

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
    private final IntList baseKeyColumnNamePositions = new IntList();
    private final ObjList<String> baseKeyColumnNames = new ObjList<>();
    private final CharSequenceHashSet baseTableDedupKeys = new CharSequenceHashSet();
    private final String baseTableName;
    private final int baseTableNamePosition;
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    private final MatViewDefinition matViewDefinition = new MatViewDefinition();
    private final int refreshType;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final String sqlText;
    private final String timeZone;
    private final String timeZoneOffset;
    private final IntList tmpColumnIndexes = new IntList();
    private final LowerCaseCharSequenceHashSet tmpLiterals = new LowerCaseCharSequenceHashSet();
    private CreateTableOperationImpl createTableOperation;
    private long samplingInterval;
    private char samplingIntervalUnit;

    public CreateMatViewOperationImpl(
            @NotNull String sqlText,
            @NotNull CreateTableOperationImpl createTableOperation,
            int refreshType,
            @NotNull String baseTableName,
            int baseTableNamePosition,
            @Nullable String timeZone,
            @Nullable String timeZoneOffset
    ) {
        this.sqlText = sqlText;
        this.createTableOperation = createTableOperation;
        this.refreshType = refreshType;
        this.baseTableName = baseTableName;
        this.baseTableNamePosition = baseTableNamePosition;
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
    public int getVolumePosition() {
        return createTableOperation.getVolumePosition();
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

        final int selectTextPosition = createTableOperation.getSelectTextPosition();

        final TableToken baseTableToken = sqlExecutionContext.getTableTokenIfExists(baseTableName);
        if (baseTableToken == null) {
            throw SqlException.tableDoesNotExist(baseTableNamePosition, baseTableName);
        }
        if (!baseTableToken.isWal()) {
            throw SqlException.$(baseTableNamePosition, "base table has to be WAL enabled");
        }

        // Find sampling interval.
        CharSequence intervalExpr = null;
        int intervalPos = 0;
        final ExpressionNode sampleBy = findSampleByNode(queryModel);
        // Vanilla SAMPLE BY.
        if (sampleBy != null && sampleBy.type == ExpressionNode.CONSTANT) {
            intervalExpr = sampleBy.token;
            intervalPos = sampleBy.position;
        }

        // GROUP BY timestamp_floor(ts) (optimized SAMPLE BY).
        if (intervalExpr == null) {
            final QueryColumn queryColumn = findTimestampFloorColumn(queryModel);
            if (queryColumn != null) {
                final ExpressionNode ast = queryColumn.getAst();
                // there are three timestamp_floor() overloads, so check all of them
                if (ast.paramCount == 3 || ast.paramCount == 5) {
                    final int idx = ast.paramCount - 1;
                    intervalExpr = ast.args.getQuick(idx).token;
                    intervalPos = ast.args.getQuick(idx).position;
                } else {
                    intervalExpr = ast.lhs.token;
                    intervalPos = ast.lhs.position;
                }
                if (timestamp == null) {
                    createTableOperation.setTimestampColumnName(Chars.toString(queryColumn.getName()));
                    createTableOperation.setTimestampColumnNamePosition(ast.position);
                    final CreateTableColumnModel timestampModel = createColumnModelMap.get(queryColumn.getName());
                    if (timestampModel == null) {
                        throw SqlException.position(selectTextPosition)
                                .put("TIMESTAMP column does not exist or not present in select list [name=")
                                .put(queryColumn.getName()).put(']');
                    }
                    timestampModel.setIsDedupKey(); // set dedup for timestamp column
                }
            }
        }

        // We haven't found timestamp_floor() in SELECT.
        if (intervalExpr == null) {
            throw SqlException.$(selectTextPosition, "TIMESTAMP column is not present in select list");
        }

        // Parse sampling interval expression.
        final CharSequence interval = GenericLexer.unquote(intervalExpr);
        final int samplingIntervalEnd = TimestampSamplerFactory.findIntervalEndIndex(interval, intervalPos);
        assert samplingIntervalEnd < interval.length();
        samplingInterval = TimestampSamplerFactory.parseInterval(interval, samplingIntervalEnd, intervalPos);
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
            final int partitionBy = approxBucketMicros > Timestamps.HOUR_MICROS ? PartitionBy.YEAR
                    : approxBucketMicros > Timestamps.MINUTE_MICROS ? PartitionBy.MONTH
                    : PartitionBy.DAY;
            createTableOperation.setPartitionBy(partitionBy);
            final int ttlHoursOrMonths = createTableOperation.getTtlHoursOrMonths();
            if (ttlHoursOrMonths > 0) {
                // Don't forget to validate TTL against PARTITION BY.
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, createTableOperation.getTtlPosition());
            }
        }

        // Mark key columns as dedup keys.
        baseKeyColumnNames.clear();
        baseKeyColumnNamePositions.clear();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            if (hasNoAggregates(functionFactoryCache, queryModel, i)) {
                // SAMPLE BY/GROUP BY key, add as dedup key.
                final CreateTableColumnModel model = createColumnModelMap.get(column.getName());
                if (model == null) {
                    throw SqlException.$(0, "missing column [name=").put(column.getName()).put(']');
                }
                model.setIsDedupKey();
                // Copy column names into builder to be validated later.
                copyBaseTableColumnNames(
                        column.getAst(),
                        queryModel,
                        baseTableName,
                        baseKeyColumnNames,
                        baseKeyColumnNamePositions,
                        selectTextPosition
                );
            }
        }

        // Don't forget to reset augmented columns in create table op with what we have scraped.
        createTableOperation.initColumnMetadata(createColumnModelMap);
    }

    @Override
    public void validateAndUpdateMetadataFromSelect(
            RecordMetadata selectMetadata, TableReaderMetadata baseTableMetadata
    ) throws SqlException {
        final int selectTextPosition = createTableOperation.getSelectTextPosition();
        // SELECT validation
        if (createTableOperation.getTimestampColumnName() == null) {
            if (selectMetadata.getTimestampIndex() == -1) {
                throw SqlException.position(selectTextPosition)
                        .put("materialized view query is required to have designated timestamp");
            }
        }
        createTableOperation.validateAndUpdateMetadataFromSelect(selectMetadata);
        // Key column validation (best-effort):
        // Option 1. Base table has no dedup.
        //           Any key columns are fine in this case.
        // Option 2. Base table has dedup columns.
        //           Key columns in mat view query must be a subset of the base table's dedup columns.
        //           That's to avoid situation when dedup upsert rewrites key column values leading to
        //           inconsistent mat view data.
        baseTableDedupKeys.clear();
        for (int i = 0, n = baseTableMetadata.getColumnCount(); i < n; i++) {
            if (baseTableMetadata.isDedupKey(i)) {
                baseTableDedupKeys.add(baseTableMetadata.getColumnName(i));
            }
        }
        if (baseTableDedupKeys.size() > 0) {
            for (int i = 0, n = baseKeyColumnNames.size(); i < n; i++) {
                final CharSequence baseKeyColumnName = baseKeyColumnNames.get(i);
                final int baseKeyColumnIndex = baseTableMetadata.getColumnIndexQuiet(baseKeyColumnName);
                if (baseKeyColumnIndex > -1 && !baseTableMetadata.isDedupKey(baseKeyColumnIndex)) {
                    throw SqlException.position(baseKeyColumnNamePositions.get(i) + selectTextPosition)
                            .put("key column must be one of the base table's dedup keys")
                            .put(" [columnName=").put(baseKeyColumnName)
                            .put(", baseTableName=").put(baseTableName)
                            .put(", baseTableDedupKeys=").put(baseTableDedupKeys)
                            .put(']');
                }
            }
        }
    }

    /**
     * Copies base table column names present in the given node into the target set.
     * The node may contain multiple columns/aliases, e.g. `concat(sym1, sym2)`, which are searched
     * down to their names in the base table.
     * <p>
     * Used to find the list of base table columns used in mat view query keys (best-effort validation).
     */
    private static void copyBaseTableColumnNames(
            ExpressionNode node,
            QueryModel model,
            CharSequence baseTableName,
            ObjList<String> baseKeyColumnNames,
            IntList baseKeyColumnNamePositions,
            int selectTextPosition
    ) throws SqlException {
        if (node != null && model != null) {
            if (node.type == ExpressionNode.LITERAL) {
                if (model.getTableName() != null) {
                    // We've found a lowest-level model. Let's check if the column belongs to it.
                    final int dotIndex = Chars.indexOf(node.token, '.');
                    if (dotIndex > -1) {
                        if (Chars.equalsIgnoreCase(model.getName(), node.token, 0, dotIndex)) {
                            if (!Chars.equalsIgnoreCase(model.getTableName(), baseTableName)) {
                                throw SqlException.position(node.position + selectTextPosition)
                                        .put("only base table columns can be used as materialized view keys")
                                        .put(" [invalid key=").put(node.token)
                                        .put(']');
                            }
                            baseKeyColumnNames.add(Chars.toString(node.token, dotIndex + 1, node.token.length()));
                            baseKeyColumnNamePositions.add(node.position);
                            return;
                        }
                    } else {
                        if (!Chars.equalsIgnoreCase(model.getTableName(), baseTableName)) {
                            throw SqlException.position(node.position + selectTextPosition)
                                    .put("only base table columns can be used as materialized view keys")
                                    .put(" [invalid key=").put(node.token)
                                    .put(']');
                        }
                        baseKeyColumnNames.add(Chars.toString(node.token));
                        baseKeyColumnNamePositions.add(node.position);
                        return;
                    }
                } else {
                    // Check nested model.
                    final QueryColumn column = model.getAliasToColumnMap().get(node.token);
                    copyBaseTableColumnNames(
                            column != null ? column.getAst() : node,
                            model.getNestedModel(),
                            baseTableName,
                            baseKeyColumnNames,
                            baseKeyColumnNamePositions,
                            selectTextPosition
                    );
                }
            }

            // Check node children for functions/operators.
            for (int i = 0, n = node.args.size(); i < n; i++) {
                copyBaseTableColumnNames(node.args.getQuick(i), model, baseTableName, baseKeyColumnNames, baseKeyColumnNamePositions, selectTextPosition);
            }
            if (node.lhs != null) {
                copyBaseTableColumnNames(node.lhs, model, baseTableName, baseKeyColumnNames, baseKeyColumnNamePositions, selectTextPosition);
            }
            if (node.rhs != null) {
                copyBaseTableColumnNames(node.rhs, model, baseTableName, baseKeyColumnNames, baseKeyColumnNamePositions, selectTextPosition);
            }

            // Check join models.
            for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
                copyBaseTableColumnNames(node, model.getJoinModels().getQuick(i), baseTableName, baseKeyColumnNames, baseKeyColumnNamePositions, selectTextPosition);
            }
        }
    }

    private static ExpressionNode findSampleByNode(QueryModel model) {
        while (model != null) {
            if (SqlUtil.isNotPlainSelectModel(model)) {
                break;
            }

            final ExpressionNode sampleBy = model.getSampleBy();
            if (sampleBy != null && sampleBy.type == ExpressionNode.CONSTANT) {
                return sampleBy;
            }

            model = model.getNestedModel();
        }
        return null;
    }

    private static QueryColumn findTimestampFloorColumn(QueryModel model) {
        while (model != null) {
            if (SqlUtil.isNotPlainSelectModel(model)) {
                break;
            }

            final ObjList<QueryColumn> queryColumns = model.getBottomUpColumns();
            for (int i = 0, n = queryColumns.size(); i < n; i++) {
                final QueryColumn queryColumn = queryColumns.getQuick(i);
                final ExpressionNode ast = queryColumn.getAst();
                if (ast.type == ExpressionNode.FUNCTION && Chars.equalsIgnoreCase(TimestampFloorFunctionFactory.NAME, ast.token)) {
                    return queryColumn;
                }
            }
            model = model.getNestedModel();
        }
        return null;
    }

    private boolean hasNoAggregates(FunctionFactoryCache functionFactoryCache, QueryModel queryModel, int columnIndex) {
        tmpColumnIndexes.clear();
        tmpColumnIndexes.add(columnIndex);

        for (; ; ) {
            // First, check the columns for aggregate functions
            // and accumulate all literals we've met on the way.
            tmpLiterals.clear();
            for (int i = 0, n = tmpColumnIndexes.size(); i < n; i++) {
                final int idx = tmpColumnIndexes.getQuick(i);
                ExpressionNode node = queryModel.getBottomUpColumns().getQuick(idx).getAst();
                // pre-order iterative tree traversal
                // see: http://en.wikipedia.org/wiki/Tree_traversal
                sqlNodeStack.clear();
                while (!sqlNodeStack.isEmpty() || node != null) {
                    if (node != null) {
                        switch (node.type) {
                            case LITERAL:
                                tmpLiterals.add(node.token);
                                node = null;
                                continue;
                            case FUNCTION:
                                if (functionFactoryCache.isGroupBy(node.token)) {
                                    return false;
                                }
                                break;
                            default:
                                for (int j = 0, m = node.args.size(); j < m; j++) {
                                    sqlNodeStack.add(node.args.getQuick(j));
                                }
                                if (node.rhs != null) {
                                    sqlNodeStack.push(node.rhs);
                                }
                                break;
                        }
                        node = node.lhs;
                    } else {
                        node = sqlNodeStack.poll();
                    }
                }
            }

            // If the model is not an outer select, we're done.
            if (queryModel.getNestedModel() == null || SqlUtil.isNotPlainSelectModel(queryModel)) {
                return true;
            }

            // OK, it's a simple select model, so we need to check the nested model
            // as the column may reference nested aggregates.
            // Example:
            //   SELECT c FROM (SELECT count() AS c FROM x);
            queryModel = queryModel.getNestedModel();

            // Collect column indexes to check in the next iteration and carry on.
            tmpColumnIndexes.clear();
            for (int i = 0, n = queryModel.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn column = queryModel.getBottomUpColumns().getQuick(i);
                if (tmpLiterals.contains(column.getAlias())) {
                    tmpColumnIndexes.add(i);
                }
            }
        }
    }
}
