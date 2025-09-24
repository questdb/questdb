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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.OperationCodes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
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
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
 * and validated. Sampling interval
 * and unit are also parsed at this stage as we want to support GROUP BY timestamp_floor(ts)
 * queries.
 */
public class CreateMatViewOperationImpl implements CreateMatViewOperation {
    private final String baseTableName;
    private final int baseTableNamePosition;
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    private final boolean deferred;
    private final int periodDelay;
    private final char periodDelayUnit;
    private final int periodLength;
    private final char periodLengthUnit;
    private final int refreshType;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final String sqlText;
    private final String timeZone;
    private final String timeZoneOffset;
    private final int timerInterval;
    private final long timerStartUs;
    private final String timerTimeZone;
    private final char timerUnit;
    private final IntList tmpColumnIndexes = new IntList();
    private final LowerCaseCharSequenceHashSet tmpLiterals = new LowerCaseCharSequenceHashSet();
    private final MatViewDefinition viewDefinition = new MatViewDefinition();
    private int baseTableTimestampType;
    private CreateTableOperationImpl createTableOperation;
    private long samplingInterval;
    private char samplingIntervalUnit;

    public CreateMatViewOperationImpl(
            @NotNull String sqlText,
            @NotNull CreateTableOperationImpl createTableOperation,
            int refreshType,
            boolean deferred,
            @NotNull String baseTableName,
            int baseTableNamePosition,
            @Nullable String timeZone,
            @Nullable String timeZoneOffset,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable String timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        this.sqlText = sqlText;
        this.createTableOperation = createTableOperation;
        this.refreshType = refreshType;
        this.deferred = deferred;
        this.baseTableName = baseTableName;
        this.baseTableNamePosition = baseTableNamePosition;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
        this.timerInterval = timerInterval;
        this.timerUnit = timerUnit;
        this.timerStartUs = timerStartUs;
        this.timerTimeZone = timerTimeZone;
        this.periodLength = periodLength;
        this.periodLengthUnit = periodLengthUnit;
        this.periodDelay = periodDelay;
        this.periodDelayUnit = periodDelayUnit;
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
        return viewDefinition;
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
        viewDefinition.init(
                refreshType,
                deferred,
                baseTableTimestampType,
                matViewToken,
                Chars.toString(createTableOperation.getSelectText()),
                baseTableName,
                samplingInterval,
                samplingIntervalUnit,
                timeZone,
                timeZoneOffset,
                0, // refreshLimitHoursOrMonths can only be set via ALTER
                timerInterval,
                timerUnit,
                timerStartUs,
                timerTimeZone,
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit
        );
    }

    @Override
    public boolean isDedupKey(int index) {
        return createTableOperation.isDedupKey(index);
    }

    @Override
    public boolean isDeferred() {
        return deferred;
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
            @NotNull SqlExecutionContext sqlExecutionContext,
            @NotNull FunctionFactoryCache functionFactoryCache,
            @NotNull QueryModel queryModel
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
            if (!ColumnType.isTimestamp(timestampType) && timestampType != ColumnType.UNDEFINED) {
                throw SqlException.position(timestampPos)
                        .put("TIMESTAMP column expected [actual=")
                        .put(ColumnType.nameOf(timestampType)).put(']');
            }
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
                }
            }
        }

        // We haven't found timestamp_floor() in SELECT.
        if (intervalExpr == null) {
            throw SqlException.$(selectTextPosition, "TIMESTAMP column is not present in select list");
        }

        // Parse sampling interval expression.
        final CharSequence interval = GenericLexer.unquote(intervalExpr);
        final int samplingIntervalEnd = TimestampSamplerFactory.findIntervalEndIndex(interval, intervalPos, "sample");
        assert samplingIntervalEnd < interval.length();
        samplingInterval = TimestampSamplerFactory.parseInterval(interval, samplingIntervalEnd, intervalPos, "sample", Numbers.INT_NULL, ' ');
        assert samplingInterval > 0;
        samplingIntervalUnit = interval.charAt(samplingIntervalEnd);

        CairoEngine engine = sqlExecutionContext.getCairoEngine();
        try (TableMetadata baseTableMetadata = engine.getTableMetadata(baseTableToken)) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                if (hasNoAggregates(functionFactoryCache, queryModel, i)) {
                    final CreateTableColumnModel columnModel = createColumnModelMap.get(column.getName());
                    if (columnModel == null) {
                        throw SqlException.$(0, "missing column [name=").put(column.getName()).put(']');
                    }
                    copyBaseTableSymbolColumnCapacity(column.getAst(), queryModel, columnModel, baseTableName, baseTableMetadata);
                }
            }
        }

        // Don't forget to reset augmented columns in create table op with what we have scraped.
        createTableOperation.initColumnMetadata(createColumnModelMap);
    }

    @Override
    public void validateAndUpdateMetadataFromSelect(
            @NotNull RecordMetadata selectMetadata,
            @NotNull TableReaderMetadata baseTableMetadata
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
        updateMatViewTablePartitionBy(createTableOperation.getTimestampType());
        this.baseTableTimestampType = baseTableMetadata.getTimestampType();
    }

    private static void copyBaseTableSymbolColumnCapacity(
            @Nullable ExpressionNode columnNode,
            @Nullable QueryModel queryModel,
            @NotNull CreateTableColumnModel columnModel,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata
    ) {
        if (columnNode != null && queryModel != null) {
            if (columnNode.type == ExpressionNode.LITERAL) {
                if (queryModel.getTableName() != null) {
                    if (Chars.equalsIgnoreCase(queryModel.getTableName(), baseTableName)) {
                        final CharSequence columnName = resolveColumnName(columnNode, queryModel);
                        if (columnName != null) {
                            final int columnIndex = baseTableMetadata.getColumnIndexQuiet(columnName);
                            if (columnIndex > -1) {
                                final TableColumnMetadata baseTableColumnMetadata = baseTableMetadata.getColumnMetadata(columnIndex);
                                if (baseTableColumnMetadata.getColumnType() == ColumnType.SYMBOL) {
                                    columnModel.setSymbolCapacity(baseTableColumnMetadata.getSymbolCapacity());
                                }
                            }
                        }
                    }
                } else {
                    // Check nested queryModel.
                    final QueryColumn column = queryModel.getAliasToColumnMap().get(columnNode.token);
                    copyBaseTableSymbolColumnCapacity(
                            column != null ? column.getAst() : columnNode,
                            queryModel.getNestedModel(),
                            columnModel,
                            baseTableName,
                            baseTableMetadata
                    );
                }
            }

            for (int i = 1, n = queryModel.getJoinModels().size(); i < n; i++) {
                copyBaseTableSymbolColumnCapacity(columnNode, queryModel.getJoinModels().getQuick(i), columnModel, baseTableName, baseTableMetadata);
            }

            copyBaseTableSymbolColumnCapacity(columnNode, queryModel.getUnionModel(), columnModel, baseTableName, baseTableMetadata);
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

    private static @Nullable CharSequence resolveColumnName(ExpressionNode columnNode, QueryModel queryModel) {
        final int dotIndex = Chars.indexOfLastUnquoted(columnNode.token, '.');
        if (dotIndex > -1) {
            if (Chars.equalsIgnoreCase(queryModel.getName(), columnNode.token, 0, dotIndex)) {
                return columnNode.token.subSequence(dotIndex + 1, columnNode.token.length());
            }
        } else {
            return columnNode.token;
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

    private void updateMatViewTablePartitionBy(int timestampType) throws SqlException {
        // Check if PARTITION BY wasn't specified in SQL, so that we need
        // to assign it based on the sampling interval.
        if (createTableOperation.getPartitionBy() == PartitionBy.NONE) {
            TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            final TimestampSampler timestampSampler = TimestampSamplerFactory.getInstance(
                    timestampDriver,
                    samplingInterval,
                    samplingIntervalUnit,
                    0
            );
            final long approxBucket = timestampSampler.getApproxBucketSize();
            final int partitionBy = approxBucket > timestampDriver.fromHours(1) ? PartitionBy.YEAR
                    : approxBucket > timestampDriver.fromMinutes(1) ? PartitionBy.MONTH
                    : PartitionBy.DAY;
            createTableOperation.setPartitionBy(partitionBy);
            final int ttlHoursOrMonths = createTableOperation.getTtlHoursOrMonths();
            if (ttlHoursOrMonths > 0) {
                // Don't forget to validate TTL against PARTITION BY.
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, createTableOperation.getTtlPosition());
            }
        }
    }
}
