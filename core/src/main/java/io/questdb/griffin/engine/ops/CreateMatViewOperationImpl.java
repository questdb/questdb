/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MicrosTimestampDriver;
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
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.griffin.model.ExpressionNode.FUNCTION;
import static io.questdb.griffin.model.ExpressionNode.LITERAL;

/**
 * Create mat view operation relies on implicit create table as select operation.
 * <p>
 * The supported clauses are the following:
 * - column list, which defines the view's columns instead of the query
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
    // the error of the rule isBucketedBaseTimestampColumn() tells, which ALTER MATERIALIZED VIEW ... AS applies too
    public static final String AGGREGATING_TIMESTAMP_ERROR = "aggregating materialized view timestamp must bucket the base table designated timestamp without transformation";
    // the error of the rule isPassthroughTimestampColumn() tells, which ALTER MATERIALIZED VIEW ... AS applies too
    public static final String PASSTHROUGH_TIMESTAMP_ERROR = "passthrough materialized view timestamp must reference the base table designated timestamp without transformation";
    private final String baseTableName;
    private final int baseTableNamePosition;
    // the names of the query columns that take the base table's designated timestamp as the view's kind requires: as
    // it is, for a passthrough view, or bucketed, for an aggregating view
    private final LowerCaseCharSequenceHashSet baseTimestampColumnNames = new LowerCaseCharSequenceHashSet();
    // The view's columns the statement lists, or null when the view takes the query's columns. The CREATE TABLE
    // operation creates them, with the definitions the statement gives, and refresh fills them by name.
    private final @Nullable GenericRecordMetadata columnListMetadata;
    // where the first listed column's name starts in the statement
    private final int columnListPosition;
    // where each listed column's type starts in the statement
    private final @Nullable IntList columnListTypePositions;
    // where each named column of the outermost SELECT starts in the statement, as the parser saw it
    private final LowerCaseCharSequenceIntHashMap columnPositions = new LowerCaseCharSequenceIntHashMap();
    private final CairoConfiguration configuration;
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    // Index definitions the CREATE statement declares. initColumnMetadata() replaces the create-table
    // operation's augmented metadata with each pass's derived columns, which include indexes inherited
    // from the base table, so validateAndUpdateMetadataFromModel() reads the declarations from here.
    private final LowerCaseCharSequenceObjHashMap<TableColumnMetadata> declaredColumnMetadata = new LowerCaseCharSequenceObjHashMap<>();
    // PARTITION BY the CREATE statement declares, or NONE. updateMatViewTablePartitionBy() writes the
    // derived partitioning into the create-table operation, so each pass restores this value first.
    private final int declaredPartitionBy;
    private final boolean deferred;
    // whether the statement declares the view's designated timestamp with TIMESTAMP(...) after the query
    private final boolean isTimestampDeclared;
    private final int periodDelay;
    private final char periodDelayUnit;
    private final int refreshType;
    private final ArrayDeque<ExpressionNode> sqlNodeStack = new ArrayDeque<>();
    private final String sqlText;
    private final String timeZone;
    private final String timeZoneOffset;
    private final int timerInterval;
    private final char timerUnit;
    private final IntList tmpColumnIndexes = new IntList();
    private final LowerCaseCharSequenceHashSet tmpLiterals = new LowerCaseCharSequenceHashSet();
    private final MatViewDefinition viewDefinition = new MatViewDefinition();
    private int baseTableTimestampType;
    private CreateTableOperationImpl createTableOperation;
    private boolean passthrough;
    private int periodLength;
    private char periodLengthUnit;
    private long samplingInterval;
    private char samplingIntervalUnit;
    private long timerStartUs;
    private String timerTimeZone;

    public CreateMatViewOperationImpl(
            @NotNull CairoConfiguration configuration,
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
            char periodDelayUnit,
            @NotNull LowerCaseCharSequenceIntHashMap columnPositions,
            @Nullable GenericRecordMetadata columnListMetadata,
            int columnListPosition,
            @Nullable IntList columnListTypePositions
    ) {
        this.configuration = configuration;
        this.columnListMetadata = columnListMetadata;
        this.columnListPosition = columnListPosition;
        this.columnListTypePositions = columnListTypePositions;
        this.sqlText = sqlText;
        this.createTableOperation = createTableOperation;
        declaredColumnMetadata.putAll(createTableOperation.getAugmentedColumnMetadata());
        this.declaredPartitionBy = createTableOperation.getPartitionBy();
        this.isTimestampDeclared = createTableOperation.getTimestampColumnName() != null;
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
        final ObjList<CharSequence> columnNames = columnPositions.keys();
        for (int i = 0, n = columnNames.size(); i < n; i++) {
            final CharSequence columnName = columnNames.getQuick(i);
            this.columnPositions.put(columnName, columnPositions.get(columnName));
        }
    }

    /**
     * Returns the node of the sampling interval of a materialized view's optimised query: the interval of its SAMPLE
     * BY, or that of its GROUP BY timestamp_floor(), or null when it has neither, as a non-aggregating passthrough
     * view. ALTER MATERIALIZED VIEW ... AS requires the interval of the view's.
     */
    public static @Nullable ExpressionNode findSamplingIntervalNode(@NotNull IQueryModel queryModel) {
        // the optimiser rewrites a materialized view's SAMPLE BY to GROUP BY timestamp_floor()
        final QueryColumn queryColumn = findTimestampFloorColumn(queryModel);
        return queryColumn != null ? SqlUtil.getTimestampFloorInterval(queryColumn.getAst()) : findSampleByNode(queryModel);
    }

    /**
     * Returns the name of the column that CREATE MATERIALIZED VIEW without a TIMESTAMP clause makes the view's
     * designated timestamp in place of the optimised query's own, the column of the query's GROUP BY timestamp_floor()
     * when the query has no SAMPLE BY, or null when it takes the query's designated timestamp.
     */
    public static @Nullable String findTimestampFloorColumnName(@NotNull IQueryModel queryModel) {
        if (findSampleByNode(queryModel) != null) {
            return null;
        }
        final QueryColumn queryColumn = findTimestampFloorColumn(queryModel);
        return queryColumn != null ? SqlUtil.toColumnName(queryColumn.getName()) : null;
    }

    /**
     * Puts into {@code indexes}, keyed by column name, the index each column of a passthrough view's optimised query
     * inherits from the base table under CREATE MATERIALIZED VIEW without a column list and an INDEX clause, see
     * inheritBaseSymbolColumnIndex(): a column that passes an indexed SYMBOL column of the base table through, as the
     * first of the view's columns that pass it. The columns of an aggregating view inherit no index.
     */
    public static void inheritBaseSymbolColumnIndexes(
            @NotNull IQueryModel queryModel,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata,
            @NotNull LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> indexes
    ) {
        final ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        final ObjList<String> passthroughColumnNames = new ObjList<>(baseTableMetadata.getColumnCount());
        collectPassthroughColumnNames(columns, queryModel, baseTableName, baseTableMetadata, passthroughColumnNames);
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final String columnName = SqlUtil.toColumnName(column.getName());
            final CreateTableColumnModel columnModel = CreateTableColumnModel.FACTORY.newInstance();
            inheritBaseSymbolColumnIndex(
                    column.getAst(),
                    queryModel.getNestedModel(),
                    columnModel,
                    columnName,
                    baseTableName,
                    baseTableMetadata,
                    passthroughColumnNames
            );
            if (columnModel.isIndexed()) {
                indexes.put(columnName, columnModel);
            }
        }
    }

    /**
     * Returns true when an output column of an aggregating view's optimised query buckets the base table's designated
     * timestamp: it references that timestamp through aliases, nested selects, the master of a join and the floored
     * argument of timestamp_floor(), which SAMPLE BY lowers to, with no other transformation. Refresh recomputes the
     * buckets of the base table's intervals of that timestamp, so a view whose designated timestamp buckets another
     * column, as under TIMESTAMP(ts2), would miss rows while it reports valid. ALTER MATERIALIZED VIEW ... AS applies
     * the rule as CREATE MATERIALIZED VIEW does.
     */
    public static boolean isBucketedBaseTimestampColumn(
            @NotNull IQueryModel queryModel,
            @NotNull QueryColumn column,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata
    ) {
        // The projection AST references the input model, not its own output aliases.
        return resolveBaseColumnIndex(column.getAst(), queryModel.getNestedModel(), baseTableName, baseTableMetadata, true)
                == baseTableMetadata.getTimestampIndex();
    }

    /**
     * Returns true when a materialized view's optimised query, which has no sampling interval, is a non-aggregating
     * passthrough query over one table, as CREATE MATERIALIZED VIEW tells it.
     */
    public static boolean isPassthroughQuery(@NotNull FunctionFactoryCache functionFactoryCache, @NotNull IQueryModel queryModel) {
        return isPassthrough(functionFactoryCache, queryModel, new IntList(), new LowerCaseCharSequenceHashSet(), new ArrayDeque<>());
    }

    /**
     * Returns true when an output column of a passthrough view's optimised query references the base table's designated
     * timestamp without transformation, through aliases and nested selects, as a passthrough view's designated timestamp
     * must: refresh replaces the base table's intervals of that timestamp in the view. ALTER MATERIALIZED VIEW ... AS
     * applies the rule as CREATE MATERIALIZED VIEW does.
     */
    public static boolean isPassthroughTimestampColumn(
            @NotNull IQueryModel queryModel,
            @NotNull QueryColumn column,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata
    ) {
        // The projection AST references the input model, not its own output aliases.
        return resolveBaseColumnIndex(column.getAst(), queryModel.getNestedModel(), baseTableName, baseTableMetadata, false)
                == baseTableMetadata.getTimestampIndex();
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
    public @Nullable RecordMetadata getColumnListMetadata() {
        return columnListMetadata;
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
    public IntList getCoveringColumnIndices(int columnIndex) {
        return createTableOperation.getCoveringColumnIndices(columnIndex);
    }

    @Override
    public CreateTableOperation getCreateTableOperation() {
        return createTableOperation;
    }

    // The row-expiry policy is parsed onto the underlying CreateTableOperation (mirroring TTL).
    // It must be exposed on the TableStructure that CairoEngine.createMatView() hands to the _meta
    // serializer, so delegate here exactly like getTtlHoursOrMonths(); otherwise CREATE MAT VIEW
    // ... EXPIRE ROWS would silently drop the policy at create time.
    @Override
    public long getExpiryCleanupIntervalMicros() {
        return createTableOperation.getExpiryCleanupIntervalMicros();
    }

    @Override
    public String getExpiryPredicate() {
        return createTableOperation.getExpiryPredicate();
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
                createTableOperation.getSelectText(),
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
                periodDelayUnit,
                passthrough
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
    public boolean isCovering(int index) {
        return createTableOperation.isCovering(index);
    }

    @Override
    public byte getIndexType(int index) {
        return createTableOperation.getIndexType(index);
    }

    @Override
    public boolean isMatView() {
        return true;
    }

    @Override
    public boolean isPassthrough() {
        return passthrough;
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
            @NotNull IQueryModel queryModel
    ) throws SqlException {
        // Create view columns based on query.
        final ObjList<QueryColumn> columns = queryModel.getBottomUpColumns();
        assert columns.size() > 0;

        // We do not know types of columns at this stage.
        // Compiler must put table together using query metadata.
        createColumnModelMap.clear();
        baseTimestampColumnNames.clear();
        passthrough = false;
        createTableOperation.setPartitionBy(declaredPartitionBy);
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn qc = columns.getQuick(i);
            // Key the column-model map by the clean display name, matching the factory metadata names
            // (CreateTableOperation resolves these verbatim). toColumnName is identity for ordinary
            // names, so only a quote-protected alias (operator token / dotted) is affected - without
            // this its index/dedup/cast/symbol-capacity defs would silently miss downstream.
            final CharSequence columnName = SqlUtil.toColumnName(qc.getName());
            final CreateTableColumnModel model = CreateTableColumnModel.FACTORY.newInstance();
            model.setColumnNamePos(qc.getAst().position);
            model.setColumnType(ColumnType.UNDEFINED);
            // Copy the declared index() definitions, so that we don't lose them.
            final TableColumnMetadata declaredMetadata = declaredColumnMetadata.get(columnName);
            if (declaredMetadata != null && declaredMetadata.isIndexed()) {
                model.setIndexType(declaredMetadata.getIndexType(), qc.getAst().position, declaredMetadata.getIndexValueBlockCapacity());
            }
            createColumnModelMap.put(columnName, model);
        }

        final String timestamp = createTableOperation.getTimestampColumnName();
        final int timestampPos = createTableOperation.getTimestampColumnNamePosition();
        if (timestamp != null) {
            final CreateTableColumnModel timestampModel = createColumnModelMap.get(timestamp);
            if (timestampModel == null) {
                if (columnListMetadata != null) {
                    // the parser has checked that the column list has the column
                    throw SqlException.position(timestampPos)
                            .put("materialized view query does not produce the designated timestamp column [column=")
                            .put(timestamp).put(']');
                }
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
        if (baseTableToken.isLiveView()) {
            // A live view is implicitly WAL, so it slips past the isWal() gate above. Reject it for
            // the same reason CREATE LIVE VIEW rejects live-on-live: a mat view refreshes through
            // the apply pipeline that does not support an LV base, and its refresh reads the LV
            // through LiveViewRecordCursorFactory, which unions the un-flushed tier - so it could
            // materialise rows no LV WAL txn covers yet and record a lastRefreshBaseTxn behind them.
            throw SqlException.$(baseTableNamePosition,
                    "live views are not allowed as base tables [name=").put(baseTableName).put(']');
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
                final ExpressionNode intervalNode = SqlUtil.getTimestampFloorInterval(ast);
                intervalExpr = intervalNode.token;
                intervalPos = intervalNode.position;
                if (timestamp == null) {
                    // Clean name: the persisted designated-timestamp name is resolved verbatim against
                    // factory metadata downstream, and the model map is keyed clean (see above). Compute
                    // it once - toColumnName re-scans the alias and allocates a String on each call.
                    final String tsName = SqlUtil.toColumnName(queryColumn.getName());
                    createTableOperation.setTimestampColumnName(tsName);
                    createTableOperation.setTimestampColumnNamePosition(ast.position);
                    final CreateTableColumnModel timestampModel = createColumnModelMap.get(tsName);
                    if (timestampModel == null) {
                        throw SqlException.position(selectTextPosition)
                                .put("TIMESTAMP column does not exist or not present in select list [name=")
                                .put(tsName).put(']');
                    }
                }
            }
        }

        // No SAMPLE BY and no timestamp_floor(): treat as a non-aggregating "passthrough" view
        // (e.g. SELECT * FROM base) when the query has no aggregates/joins. Such a view copies base
        // rows 1:1; samplingInterval becomes the granularity of refresh ranges (set later in
        // validateAndUpdateMetadataFromSelect), not an aggregation bucket.
        if (intervalExpr == null) {
            if (isPassthrough(functionFactoryCache, queryModel, tmpColumnIndexes, tmpLiterals, sqlNodeStack)) {
                passthrough = true;
            } else if (timestamp != null) {
                // The designated timestamp column was already confirmed present in the select
                // list above, but the query is not a passthrough view and has neither a SAMPLE BY
                // nor a GROUP BY timestamp_floor(...), so no sampling interval could be inferred.
                // Point the user at the two supported forms instead of claiming the column is missing.
                throw SqlException.position(selectTextPosition)
                        .put("materialized view query requires a sampling interval, use SAMPLE BY or GROUP BY timestamp_floor() [name=")
                        .put(timestamp).put(']');
            } else {
                throw SqlException.$(selectTextPosition, "TIMESTAMP column is not present in select list");
            }
        }

        if (passthrough) {
            if (periodLength != 0) {
                throw SqlException.$(selectTextPosition, "PERIOD is not supported for non-aggregating (passthrough) materialized views");
            }
        } else {
            // Parse sampling interval expression.
            final CharSequence interval = GenericLexer.unquote(intervalExpr);
            final int samplingIntervalEnd = CommonUtils.findPositiveIntervalEndIndex(interval, intervalPos, "sample");
            assert samplingIntervalEnd < interval.length();
            samplingInterval = CommonUtils.parsePositiveInterval(interval, samplingIntervalEnd, intervalPos, "sample", Numbers.INT_NULL, ' ');
            assert samplingInterval > 0;
            samplingIntervalUnit = interval.charAt(samplingIntervalEnd);
        }

        CairoEngine engine = sqlExecutionContext.getCairoEngine();
        try (TableMetadata baseTableMetadata = engine.getTableMetadata(baseTableToken)) {
            final ObjList<String> passthroughColumnNames = new ObjList<>(baseTableMetadata.getColumnCount());
            if (passthrough) {
                collectPassthroughColumnNames(columns, queryModel, baseTableName, baseTableMetadata, passthroughColumnNames);
                for (int i = 0, n = columns.size(); i < n; i++) {
                    final QueryColumn column = columns.getQuick(i);
                    if (isPassthroughTimestampColumn(queryModel, column, baseTableName, baseTableMetadata)) {
                        baseTimestampColumnNames.add(SqlUtil.toColumnName(column.getName()));
                    }
                }
            } else {
                for (int i = 0, n = columns.size(); i < n; i++) {
                    final QueryColumn column = columns.getQuick(i);
                    if (isBucketedBaseTimestampColumn(queryModel, column, baseTableName, baseTableMetadata)) {
                        baseTimestampColumnNames.add(SqlUtil.toColumnName(column.getName()));
                    }
                }
            }
            // A column list defines its columns' SYMBOL capacity and index, with CREATE TABLE's defaults.
            for (int i = 0, n = columns.size(); i < n; i++) {
                final QueryColumn column = columns.getQuick(i);
                if (columnListMetadata == null && hasNoAggregates(functionFactoryCache, queryModel, i, tmpColumnIndexes, tmpLiterals, sqlNodeStack)) {
                    final String columnName = SqlUtil.toColumnName(column.getName());
                    final CreateTableColumnModel columnModel = createColumnModelMap.get(columnName);
                    if (columnModel == null) {
                        throw SqlException.$(0, "missing column [name=").put(columnName).put(']');
                    }
                    copyBaseTableSymbolColumnCapacity(column.getAst(), queryModel, columnModel, baseTableName, baseTableMetadata);
                    if (passthrough) {
                        // A passthrough view (e.g. SELECT * FROM base, as used by EXPIRE ROWS retention
                        // views) copies base rows 1:1, so a pass-through symbol column inherits the base
                        // table's index - otherwise the view silently drops it, forcing full scans for
                        // indexed/LATEST ON reads that could use an index seek. Passthrough-only: an
                        // aggregating view's rows are not base rows, so it never inherits.
                        inheritBaseSymbolColumnIndex(
                                column.getAst(),
                                queryModel.getNestedModel(),
                                columnModel,
                                columnName,
                                baseTableName,
                                baseTableMetadata,
                                passthroughColumnNames
                        );
                    }
                }
            }
        }

        // Don't forget to reset augmented columns in create table op with what we have scraped. The create table op
        // keeps the definitions of a column list, which it received when the statement built it.
        if (columnListMetadata == null) {
            createTableOperation.initColumnMetadata(createColumnModelMap);
        }

        if (periodLength == -1) {
            // It's PERIOD (SAMPLE BY INTERVAL) which means that we need to align the period to the SAMPLE BY bucket.
            periodLength = (int) Math.min(samplingInterval, Integer.MAX_VALUE);
            periodLengthUnit = samplingIntervalUnit;
            CreateMatViewOperation.validateMatViewPeriodLength(periodLength, periodLengthUnit, intervalPos);
            final TimestampSampler periodSamplerMicros = TimestampSamplerFactory.getInstance(
                    MicrosTimestampDriver.INSTANCE,
                    periodLength,
                    periodLengthUnit,
                    intervalPos
            );
            assert timerTimeZone == null;
            timerTimeZone = timeZone;
            TimeZoneRules tzRulesMicros = null;
            if (timerTimeZone != null) {
                try {
                    tzRulesMicros = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timerTimeZone);
                } catch (CairoException e) {
                    throw SqlException.position(intervalPos).put(e.getFlyweightMessage());
                }
            }
            if (timeZoneOffset != null) {
                final long val = Dates.parseOffset(timeZoneOffset);
                if (val == Numbers.LONG_NULL) {
                    throw SqlException.position(intervalPos).put("invalid offset: ").put(timeZoneOffset);
                }
                if (Numbers.decodeLowInt(val) != 0) {
                    throw SqlException.position(intervalPos).put("PERIOD (SAMPLE BY INTERVAL) can't be used with WITH OFFSET");
                }
            }
            final long nowMicros = configuration.getMicrosecondClock().getTicks();
            final long nowLocalMicros = tzRulesMicros != null ? nowMicros + tzRulesMicros.getOffset(nowMicros) : nowMicros;
            timerStartUs = periodSamplerMicros.round(nowLocalMicros);
        }
    }

    @Override
    public void validateAndUpdateMetadataFromSelect(
            @NotNull RecordMetadata selectMetadata,
            @NotNull TableReaderMetadata baseTableMetadata,
            int scanDirection
    ) throws SqlException {
        final int selectTextPosition = createTableOperation.getSelectTextPosition();
        // SELECT validation
        if (createTableOperation.getTimestampColumnName() == null) {
            if (selectMetadata.getTimestampIndex() == -1) {
                throw SqlException.position(selectTextPosition)
                        .put("materialized view query is required to have designated timestamp");
            }
        }
        if (columnListMetadata == null) {
            createTableOperation.validateAndUpdateMetadataFromSelect(selectMetadata, scanDirection);
        }
        // Refresh recomputes the base table's intervals of its designated timestamp and replaces them in the view, so
        // the view's designated timestamp must be that timestamp, unchanged for a passthrough view and bucketed for an
        // aggregating view, through aliases and nested selects.
        final String timestampName = createTableOperation.getTimestampColumnName() != null
                ? createTableOperation.getTimestampColumnName()
                : selectMetadata.getColumnName(selectMetadata.getTimestampIndex());
        if (!baseTimestampColumnNames.contains(timestampName)) {
            throw SqlException.$(getTimestampPosition(timestampName), passthrough ? PASSTHROUGH_TIMESTAMP_ERROR : AGGREGATING_TIMESTAMP_ERROR);
        }
        if (columnListMetadata != null) {
            validateColumnList(selectMetadata, timestampName);
            createTableOperation.validateAndUpdateMetadataFromSelect(columnListMetadata, scanDirection);
        }
        if (passthrough) {
            setPassthroughChunkInterval();
        }
        updateMatViewTablePartitionBy(createTableOperation.getTimestampType(), baseTableMetadata.getPartitionBy());
        this.baseTableTimestampType = baseTableMetadata.getTimestampType();
    }

    // Puts into passthroughColumnNames, at the index of each base table column, the name of the first column of the
    // passthrough query that passes the base column through, or null when none does.
    private static void collectPassthroughColumnNames(
            ObjList<QueryColumn> columns,
            IQueryModel queryModel,
            CharSequence baseTableName,
            TableMetadata baseTableMetadata,
            ObjList<String> passthroughColumnNames
    ) {
        passthroughColumnNames.setPos(baseTableMetadata.getColumnCount());
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final int baseColumnIndex = resolveBaseColumnIndex(
                    column.getAst(),
                    queryModel.getNestedModel(),
                    baseTableName,
                    baseTableMetadata,
                    false
            );
            if (baseColumnIndex > -1 && passthroughColumnNames.getQuick(baseColumnIndex) == null) {
                passthroughColumnNames.setQuick(baseColumnIndex, SqlUtil.toColumnName(column.getName()));
            }
        }
    }

    private static void copyBaseTableSymbolColumnCapacity(
            @Nullable ExpressionNode columnNode,
            @Nullable IQueryModel queryModel,
            @NotNull CreateTableColumnModel columnModel,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata
    ) {
        if (columnNode != null && queryModel != null) {
            if (columnNode.type == ExpressionNode.LITERAL) {
                if (queryModel.getTableName() != null) {
                    if (Chars.equalsIgnoreCase(queryModel.getTableName(), baseTableName)) {
                        final CharSequence columnName = resolveColumnName(columnNode.token, queryModel);
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

    private static ExpressionNode findSampleByNode(IQueryModel model) {
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

    private static QueryColumn findTimestampFloorColumn(IQueryModel model) {
        while (model != null) {
            if (SqlUtil.isNotPlainSelectModel(model)) {
                break;
            }

            final ObjList<QueryColumn> queryColumns = model.getBottomUpColumns();
            for (int i = 0, n = queryColumns.size(); i < n; i++) {
                final QueryColumn queryColumn = queryColumns.getQuick(i);
                final ExpressionNode ast = queryColumn.getAst();
                if (SqlUtil.isTimestampFloorFunction(ast)) {
                    return queryColumn;
                }
            }
            model = model.getNestedModel();
        }
        return null;
    }

    private static boolean hasNoAggregates(
            FunctionFactoryCache functionFactoryCache,
            IQueryModel queryModel,
            int columnIndex,
            IntList tmpColumnIndexes,
            LowerCaseCharSequenceHashSet tmpLiterals,
            ArrayDeque<ExpressionNode> sqlNodeStack
    ) {
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

    /**
     * Inherits the base table's SYMBOL index (type, value block capacity, and available covering columns)
     * into {@code columnModel}
     * when {@code columnNode} is a bare column reference that resolves - through the nested-model chain,
     * exactly like {@link #copyBaseTableSymbolColumnCapacity} - to an indexed SYMBOL column of the base
     * table. A reference that lands on an expression at any level (e.g. {@code upper(k) sym} in an inner
     * projection shadowing an indexed base column of the same name) resolves to no base column and
     * inherits nothing. An index the CREATE statement declared explicitly is kept as-is. The caller
     * invokes this for passthrough views only, which have no join/union models, so only the nested chain
     * is walked.
     */
    private static void inheritBaseSymbolColumnIndex(
            @Nullable ExpressionNode columnNode,
            @Nullable IQueryModel queryModel,
            @NotNull CreateTableColumnModel columnModel,
            @NotNull CharSequence outputColumnName,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata,
            @NotNull ObjList<String> passthroughColumnNames
    ) {
        if (columnNode == null || queryModel == null || columnNode.type != ExpressionNode.LITERAL || columnModel.isIndexed()) {
            return;
        }
        final int columnIndex = resolveBaseColumnIndex(columnNode, queryModel, baseTableName, baseTableMetadata, false);
        if (columnIndex < 0
                || !ColumnType.isSymbol(baseTableMetadata.getColumnType(columnIndex))
                || !baseTableMetadata.isColumnIndexed(columnIndex)) {
            return;
        }
        // One index per indexed base column, on the output column that passthroughColumnNames holds for it.
        // A view may project the same base column more than once (SELECT k, k AS k2 ...), and the index
        // belongs to the first of those projections. A covering list resolves its column names through the
        // same map, so both halves of an inherited index describe one projection.
        if (!Chars.equalsNc(outputColumnName, passthroughColumnNames.getQuick(columnIndex))) {
            return;
        }

        final TableColumnMetadata baseColumnMetadata = baseTableMetadata.getColumnMetadata(columnIndex);
        columnModel.setIndexType(
                baseColumnMetadata.getIndexType(),
                columnNode.position,
                baseTableMetadata.getIndexValueBlockCapacity(columnIndex)
        );
        final IntList coveringColumnWriterIndices = baseColumnMetadata.getCoveringColumnIndices();
        if (coveringColumnWriterIndices != null) {
            for (int i = 0, n = coveringColumnWriterIndices.size(); i < n; i++) {
                final int coveringColumnIndex = resolveDenseColumnIndex(
                        baseTableMetadata,
                        coveringColumnWriterIndices.getQuick(i)
                );
                if (coveringColumnIndex > -1) {
                    final String coveringOutputColumnName = passthroughColumnNames.getQuick(coveringColumnIndex);
                    if (coveringOutputColumnName != null) {
                        columnModel.addCoveringColumnName(coveringOutputColumnName, columnNode.position);
                    }
                }
            }
        }
    }

    // The scratch collections are hasNoAggregates()'s.
    private static boolean isPassthrough(
            FunctionFactoryCache functionFactoryCache,
            IQueryModel queryModel,
            IntList tmpColumnIndexes,
            LowerCaseCharSequenceHashSet tmpLiterals,
            ArrayDeque<ExpressionNode> sqlNodeStack
    ) {
        // A non-aggregating projection over a single table (e.g. SELECT * FROM base [WHERE ...]).
        // The caller has already established there is no SAMPLE BY / timestamp_floor(). Reject anything
        // whose rows are not 1:1 with the base: a top-level table name / GROUP BY / top-level JOIN / LATEST
        // ON / UNION here (isNotPlainSelectModel), then the same features on nested models in the walk
        // below, and any aggregate in a column. (isNotPlainSelectModel does NOT catch DISTINCT or window
        // functions - the nested-chain walk is what rejects DISTINCT.)
        if (SqlUtil.isNotPlainSelectModel(queryModel)) {
            return false;
        }
        if (queryModel.getJoinModels().size() > 1) {
            return false;
        }
        // LATEST ON / UNION / GROUP BY / DISTINCT lower onto a NESTED model, which the top-level
        // isNotPlainSelectModel() check above does not see. Walk the whole nested chain so a DERIVED view
        // (e.g. SELECT * FROM base LATEST ON ts PARTITION BY k) is NOT misclassified as passthrough: its
        // rows are not 1:1 with the base, so EXPIRE ROWS physical cleanup must never run on it. DISTINCT
        // has three shapes to catch: a SELECT_MODEL_DISTINCT model, the isDistinct() flag, and - once
        // isSqlDistinctGroupByRewriteEnabled() has rewritten DISTINCT to an implicit group-by - a
        // SELECT_MODEL_GROUP_BY model whose explicit getGroupBy() list is empty. Rejecting every
        // SELECT_MODEL_GROUP_BY covers that last shape and every other aggregating model; a genuine
        // aggregating view never reaches here (intervalExpr would be set).
        for (IQueryModel m = queryModel; m != null; m = m.getNestedModel()) {
            if (m.getLatestByType() != IQueryModel.LATEST_BY_NONE
                    || m.getUnionModel() != null
                    || m.getGroupBy().size() > 0
                    || m.getJoinModels().size() > 1
                    || m.isDistinct()
                    || m.getSelectModelType() == IQueryModel.SELECT_MODEL_DISTINCT
                    || m.getSelectModelType() == IQueryModel.SELECT_MODEL_GROUP_BY) {
                return false;
            }
        }
        final ObjList<QueryColumn> cols = queryModel.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (!hasNoAggregates(functionFactoryCache, queryModel, i, tmpColumnIndexes, tmpLiterals, sqlNodeStack)) {
                return false;
            }
        }
        return true;
    }

    // Returns the index of the base table column whose values an expression over the columns of queryModel takes
    // unchanged, or -1: the expression must be a column reference, which the method follows through the aliases of
    // nested models down to the base table. With isFloorFollowed, it also follows the floored argument of
    // timestamp_floor(), whose buckets keep the order and the intervals of the column it floors.
    private static int resolveBaseColumnIndex(
            @Nullable ExpressionNode columnNode,
            @Nullable IQueryModel queryModel,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata,
            boolean isFloorFollowed
    ) {
        if (columnNode == null) {
            return -1;
        }
        if (isFloorFollowed && SqlUtil.isTimestampFloorFunction(columnNode)) {
            return resolveBaseColumnIndex(
                    SqlUtil.getTimestampFloorTimestamp(columnNode),
                    queryModel,
                    baseTableName,
                    baseTableMetadata,
                    true
            );
        }
        if (columnNode.type != ExpressionNode.LITERAL) {
            return -1;
        }
        return resolveBaseColumnIndex(columnNode.token, queryModel, baseTableName, baseTableMetadata, isFloorFollowed);
    }

    // As above, for a reference to a column of queryModel. The reference may carry the name of the model as a prefix,
    // as the columns of the master model of a join do, or the name of a model that queryModel joins.
    private static int resolveBaseColumnIndex(
            @NotNull CharSequence columnRef,
            @Nullable IQueryModel queryModel,
            @NotNull CharSequence baseTableName,
            @NotNull TableMetadata baseTableMetadata,
            boolean isFloorFollowed
    ) {
        if (queryModel == null) {
            return -1;
        }
        // null under the prefix of another model
        final CharSequence columnName = resolveColumnName(columnRef, queryModel);
        if (columnName == null) {
            final ObjList<IQueryModel> joinModels = queryModel.getJoinModels();
            for (int i = 1, n = joinModels.size(); i < n; i++) {
                final IQueryModel joinModel = joinModels.getQuick(i);
                if (resolveColumnName(columnRef, joinModel) != null) {
                    return resolveBaseColumnIndex(columnRef, joinModel, baseTableName, baseTableMetadata, isFloorFollowed);
                }
            }
        }
        if (queryModel.getTableName() != null) {
            if (columnName != null && Chars.equalsIgnoreCase(queryModel.getTableName(), baseTableName)) {
                return baseTableMetadata.getColumnIndexQuiet(columnName);
            }
            return -1;
        }

        QueryColumn column = queryModel.getAliasToColumnMap().get(columnRef);
        if (column == null && columnName != null) {
            column = queryModel.getAliasToColumnMap().get(columnName);
        }
        if (column != null) {
            return resolveBaseColumnIndex(column.getAst(), queryModel.getNestedModel(), baseTableName, baseTableMetadata, isFloorFollowed);
        }
        // a model without a projection of its own, such as the master model of a join, passes its nested model's
        // columns through
        return resolveBaseColumnIndex(
                columnName != null ? columnName : columnRef,
                queryModel.getNestedModel(),
                baseTableName,
                baseTableMetadata,
                isFloorFollowed
        );
    }

    // Returns the name of the column a reference to a column of queryModel names, without the prefix of the model's
    // name, or null when the reference carries the prefix of another model.
    private static @Nullable CharSequence resolveColumnName(CharSequence columnRef, IQueryModel queryModel) {
        final int dotIndex = Chars.indexOfLastUnquoted(columnRef, '.');
        if (dotIndex > -1) {
            final CharSequence modelName = queryModel.getName();
            if (modelName != null && Chars.equalsIgnoreCase(modelName, columnRef, 0, dotIndex)) {
                return columnRef.subSequence(dotIndex + 1, columnRef.length());
            }
        } else {
            return columnRef;
        }
        return null;
    }

    private static int resolveDenseColumnIndex(TableMetadata metadata, int writerIndex) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getWriterIndex(i) == writerIndex) {
                return i;
            }
        }
        return -1;
    }

    // Where the view's designated timestamp is named in the statement: at TIMESTAMP(...) after the query, or at the
    // query's column as the parser saw it. A column that only a wildcard of the outermost SELECT carries falls back to
    // its column in the optimised query, which may be the column a nested SELECT names.
    private int getTimestampPosition(CharSequence timestampName) {
        if (isTimestampDeclared) {
            return createTableOperation.getTimestampColumnNamePosition();
        }
        final int index = columnPositions.keyIndex(timestampName);
        if (index < 0) {
            return columnPositions.valueAt(index);
        }
        return createTableOperation.getSelectTextPosition() + createColumnModelMap.get(timestampName).getColumnNamePos();
    }

    // The checks refresh makes when it fills the listed columns from the query's columns by name, see
    // MatViewColumnMapping: the view's designated timestamp is listed, with the query's type, and each listed column
    // the query produces takes the query column's type through the record copier. A listed column the query does not
    // produce stays NULL, and a query column the list lacks is ignored. Each error points at the listed column's type.
    private void validateColumnList(RecordMetadata selectMetadata, CharSequence timestampName) throws SqlException {
        assert columnListMetadata != null && columnListTypePositions != null;
        final int listedTimestampIndex = columnListMetadata.getColumnIndexQuiet(timestampName);
        if (listedTimestampIndex < 0) {
            // nothing in the list is wrong by itself, so the error points at the list
            throw SqlException.position(columnListPosition)
                    .put("column list must include the designated timestamp column [column=")
                    .put(timestampName).put(']');
        }
        final int queryTimestampIndex = selectMetadata.getColumnIndexQuiet(timestampName);
        assert queryTimestampIndex > -1;
        final int listedTimestampType = columnListMetadata.getColumnType(listedTimestampIndex);
        final int queryTimestampType = selectMetadata.getColumnType(queryTimestampIndex);
        if (listedTimestampType != queryTimestampType) {
            // the error refresh gives
            throw SqlException.position(columnListTypePositions.getQuick(listedTimestampIndex))
                    .put("timestamp type mismatch between materialized view and query [view=")
                    .put(ColumnType.nameOf(listedTimestampType))
                    .put(", query=")
                    .put(ColumnType.nameOf(queryTimestampType))
                    .put(']');
        }
        for (int i = 0, n = columnListMetadata.getColumnCount(); i < n; i++) {
            final int queryColumnIndex = selectMetadata.getColumnIndexQuiet(columnListMetadata.getColumnName(i));
            if (queryColumnIndex > -1) {
                final int queryColumnType = selectMetadata.getColumnType(queryColumnIndex);
                final int columnType = columnListMetadata.getColumnType(i);
                if (!RecordToRowCopierUtils.isCopierConvertible(queryColumnType, columnType)) {
                    // the error refresh gives the same pair, see MatViewColumnMapping
                    throw SqlException.position(columnListTypePositions.getQuick(i))
                            .put("inconvertible types: ")
                            .put(ColumnType.nameOf(queryColumnType)).put(" -> ").put(ColumnType.nameOf(columnType))
                            .put(" [from=").put(selectMetadata.getColumnName(queryColumnIndex))
                            .put(", to=").put(columnListMetadata.getColumnName(i)).put(']');
                }
            }
        }
        columnListMetadata.setTimestampIndex(listedTimestampIndex);
    }

    private void setPassthroughChunkInterval() {
        // Refresh range granularity for a passthrough view. It is not an aggregation bucket: incremental
        // refresh rounds the timestamp ranges the new base transactions touched out to this granularity
        // and replaces each rounded range in the view. At one microsecond the rounded range matches the
        // touched range to the microsecond, so a refresh rewrites only the view rows inside it. The
        // rows-per-query estimate sizes each query step, independently of this granularity.
        samplingInterval = 1;
        samplingIntervalUnit = 'U';
    }

    private void updateMatViewTablePartitionBy(int timestampType, int basePartitionBy) throws SqlException {
        // Check if PARTITION BY wasn't specified in SQL, so that we need
        // to assign it based on the sampling interval.
        if (createTableOperation.getPartitionBy() == PartitionBy.NONE) {
            if (passthrough) {
                // Mirror the base table's partitioning so refresh REPLACE_RANGE and expiry DROP/REPLACE
                // align to base partitions and per-partition row counts match the base.
                final int viewPartitionBy = basePartitionBy != PartitionBy.NONE ? basePartitionBy : PartitionBy.DAY;
                createTableOperation.setPartitionBy(viewPartitionBy);
                final int ttlHoursOrMonths = createTableOperation.getTtlHoursOrMonths();
                if (ttlHoursOrMonths != 0) {
                    PartitionBy.validateTtlGranularity(viewPartitionBy, ttlHoursOrMonths, createTableOperation.getTtlPosition());
                }
                return;
            }
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
            if (ttlHoursOrMonths != 0) {
                // Don't forget to validate TTL against PARTITION BY. Negative values are
                // months-based TTL; validateTtlGranularity handles both signs.
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, createTableOperation.getTtlPosition());
            }
        }
    }
}
