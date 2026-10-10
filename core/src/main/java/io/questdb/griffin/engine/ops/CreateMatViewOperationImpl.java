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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetUtcFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
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
 * and validated. Sampling interval
 * and unit are also parsed at this stage as we want to support GROUP BY timestamp_floor(ts)
 * queries.
 */
public class CreateMatViewOperationImpl implements CreateMatViewOperation {
    private final String baseTableName;
    private final int baseTableNamePosition;
    private final CairoConfiguration configuration;
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    private final boolean deferred;
    private final int periodDelay;
    private final char periodDelayUnit;
    private final StringSink intervalSink = new StringSink();
    private final int refreshType;
    private final String sqlText;
    private final String timeZone;
    private final String timeZoneOffset;
    private final int timerInterval;
    private final char timerUnit;
    private final MatViewDefinition viewDefinition = new MatViewDefinition();
    private int baseTableTimestampType;
    private CreateTableOperationImpl createTableOperation;
    private int periodLength;
    private char periodLengthUnit;
    private long samplingInterval;
    private char samplingIntervalUnit;
    private int scanColumnId;
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
            char periodDelayUnit
    ) {
        this.configuration = configuration;
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
    public byte getIndexType(int index) {
        return createTableOperation.getIndexType(index);
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
    public void validateAndUpdateMetadataFromPlan(
            @NotNull SqlExecutionContext sqlExecutionContext,
            @NotNull @Transient LogicalPlan root,
            @NotNull @Transient IntList positions
    ) throws SqlException {
        final OutputSchema columns = root.getOutput();
        assert columns.getColumnCount() > 0;
        // We do not know types of columns at this stage.
        // Compiler must put table together using query metadata.
        createTableOperation.initColumnModels(createColumnModelMap, columns, positions);
        final String timestamp = createTableOperation.getTimestampColumnName();
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
        final GroupingPlan sampleBy = findSampleBy(root);
        // Vanilla SAMPLE BY.
        if (sampleBy instanceof SampleByPlan cursor) {
            intervalExpr = cursor.getPeriodToken();
            intervalPos = cursor.getPeriodPosition();
        } else if (sampleBy != null) {
            final ConstantExpression period = (ConstantExpression) sampleByBucket((AggregatePlan) sampleBy).argumentAt(0);
            intervalExpr = intervalText(period);
            intervalPos = period.getPosition();
        }
        if (sampleBy != null && timestamp == null && isDirectTableSampleBy(sampleBy)) {
            // SAMPLE BY buckets the base designated timestamp, which the view must select.
            final String tsName = sampleByTimestampName(sqlExecutionContext, baseTableToken);
            if (tsName != null && createColumnModelMap.get(tsName) == null && !isTimestampSelected(root, tsName)) {
                throw SqlException.position(selectTextPosition)
                        .put("TIMESTAMP column does not exist or not present in select list [name=")
                        .put(tsName).put(']');
            }
        }

        // GROUP BY timestamp_floor(ts) (optimized SAMPLE BY).
        if (intervalExpr == null) {
            FunctionExpression floor = null;
            for (LogicalPlan level = root; level != null && floor == null; level = nextLevel(level)) {
                if (!(level instanceof ProjectPlan project)) {
                    continue;
                }
                for (int i = 0, n = project.getExpressions().size(); i < n && floor == null; i++) {
                    floor = timestampFloorCall(project, i);
                    if (floor != null && timestamp == null) {
                        // The persisted designated-timestamp name is resolved verbatim against factory metadata
                        // downstream, and the model map is keyed by the same output names.
                        final String tsName = Chars.toString(project.getOutput().getColumnName(i));
                        createTableOperation.setTimestampColumnName(tsName);
                        createTableOperation.setTimestampColumnNamePosition(floor.getPosition());
                        if (createColumnModelMap.get(tsName) == null) {
                            throw SqlException.position(selectTextPosition)
                                    .put("TIMESTAMP column does not exist or not present in select list [name=")
                                    .put(tsName).put(']');
                        }
                    }
                }
            }
            if (floor != null) {
                final ConstantExpression interval = (ConstantExpression) floor.argumentAt(0);
                intervalExpr = intervalText(interval);
                intervalPos = interval.getPosition();
            }
        }

        // We haven't found timestamp_floor() in SELECT.
        if (intervalExpr == null) {
            if (timestamp != null) {
                // The designated timestamp column was already confirmed present in the select
                // list above, but the query has neither a SAMPLE BY nor a GROUP BY
                // timestamp_floor(...), so no sampling interval could be inferred. Point the
                // user at the two supported forms instead of claiming the column is missing.
                throw SqlException.position(selectTextPosition)
                        .put("materialized view query requires a sampling interval, use SAMPLE BY or GROUP BY timestamp_floor() [name=")
                        .put(timestamp).put(']');
            }
            throw SqlException.$(selectTextPosition, "TIMESTAMP column is not present in select list");
        }

        // Parse sampling interval expression.
        final CharSequence interval = GenericLexer.unquote(intervalExpr);
        final int samplingIntervalEnd = TimestampSamplerFactory.findPositiveIntervalEndIndex(interval, intervalPos, "sample");
        assert samplingIntervalEnd < interval.length();
        samplingInterval = TimestampSamplerFactory.parsePositiveInterval(interval, samplingIntervalEnd, intervalPos, "sample", Numbers.INT_NULL, ' ');
        assert samplingInterval > 0;
        samplingIntervalUnit = interval.charAt(samplingIntervalEnd);

        CairoEngine engine = sqlExecutionContext.getCairoEngine();
        try (TableMetadata baseTableMetadata = engine.getTableMetadata(baseTableToken)) {
            for (int i = 0, n = columns.getColumnCount(); i < n; i++) {
                if (!readsAggregate(root, columns.getColumnId(i))) {
                    final CharSequence columnName = columns.getColumnName(i);
                    final CreateTableColumnModel columnModel = createColumnModelMap.get(columnName);
                    if (columnModel == null) {
                        throw SqlException.$(0, "missing column [name=").put(columnName).put(']');
                    }
                    copyBaseTableSymbolColumnCapacity(root, columns.getColumnId(i), columnModel, baseTableToken, baseTableMetadata);
                }
            }
        }

        // Don't forget to reset augmented columns in create table op with what we have scraped.
        createTableOperation.initColumnMetadata(createColumnModelMap);

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
        createTableOperation.validateAndUpdateMetadataFromSelect(selectMetadata, scanDirection);
        updateMatViewTablePartitionBy(createTableOperation.getTimestampType());
        this.baseTableTimestampType = baseTableMetadata.getTimestampType();
    }

    /**
     * The first SAMPLE BY, plain select levels down from the root, whose period is spelled as a constant: its
     * cursor, or the aggregate grouped by its bucket; null when there is none. A level that groups explicitly,
     * joins, reads a table with LATEST ON or is a set operation ends the search, as a non-plain select model does.
     */
    private static GroupingPlan findSampleBy(LogicalPlan root) {
        LogicalPlan plan = root;
        while (true) {
            switch (plan) {
                case SampleByPlan sampleBy -> {
                    return sampleBy.getPeriodToken() != null ? sampleBy : null;
                }
                case AggregatePlan aggregate -> {
                    if (aggregate.hasSampleByBucket()) {
                        return aggregate;
                    }
                    if (aggregate.hasExplicitGrouping()) {
                        return null;
                    }
                    plan = aggregate.getInput();
                }
                case ProjectPlan _, FilterPlan _, SortPlan _, LimitPlan _, DistinctPlan _, WindowPlan _, FillPlan _ ->
                        plan = plan.inputAt(0);
                default -> {
                    return null;
                }
            }
        }
    }

    /**
     * Whether the SAMPLE BY buckets a table directly, rather than a joined source or a sub-query.
     */
    private static boolean isDirectTableSampleBy(GroupingPlan sampleBy) {
        LogicalPlan input = sampleBy.getInput();
        while (input instanceof FilterPlan || input instanceof LatestByPlan) {
            input = input.inputAt(0);
        }
        return input instanceof ScanPlan;
    }

    private static boolean isTimestampFloor(BoundExpression expression) {
        return expression instanceof FunctionExpression call
                && (TimestampFloorFunctionFactory.NAME.equals(call.getName()) || TimestampFloorFromOffsetUtcFunctionFactory.NAME.equals(call.getName()));
    }

    /**
     * The plan the next plain select level down from {@code plan} starts at; null below a level that is not plain:
     * a table, a join, a LATEST ON, an explicit GROUP BY, a SAMPLE BY or a set operation.
     */
    private static LogicalPlan nextLevel(LogicalPlan plan) {
        return switch (plan) {
            case ProjectPlan _, FilterPlan _, SortPlan _, LimitPlan _, DistinctPlan _, WindowPlan _, FillPlan _ ->
                    plan.inputAt(0);
            case AggregatePlan aggregate ->
                    aggregate.hasExplicitGrouping() || aggregate.hasSampleByBucket() ? null : aggregate.getInput();
            default -> null;
        };
    }

    /**
     * Whether the expression, over {@code input}, reads an aggregate: directly, or through the plain select levels
     * beneath it.
     */
    private static boolean readsAggregate(LogicalPlan input, BoundExpression expression) {
        if (expression instanceof ColumnExpression column) {
            return input.getOutput().getColumnIndexById(column.getColumnId()) >= 0 && readsAggregate(input, column.getColumnId());
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (readsAggregate(input, call.argumentAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether column {@code id} of {@code plan} is an aggregate, or computes over one through the plain select
     * levels beneath; a key of an explicit GROUP BY counts as none, as it does in its select model.
     */
    private static boolean readsAggregate(LogicalPlan plan, int id) {
        while (true) {
            switch (plan) {
                case ProjectPlan project -> {
                    final BoundExpression expression = project.getExpressions().getQuick(project.getOutput().getColumnIndexById(id));
                    if (!(expression instanceof ColumnExpression column)) {
                        return readsAggregate(project.getInput(), expression);
                    }
                    id = column.getColumnId();
                    plan = project.getInput();
                }
                case GroupingPlan grouping -> {
                    final int index = grouping.getOutput().getColumnIndexById(id);
                    if (index >= grouping.getGroupingExpressions().size()) {
                        return true;
                    }
                    if (grouping.hasExplicitGrouping()) {
                        return false;
                    }
                    final BoundExpression key = grouping.getGroupingExpressions().getQuick(index);
                    if (!(key instanceof ColumnExpression column)) {
                        return readsAggregate(grouping.getInput(), key);
                    }
                    id = column.getColumnId();
                    plan = grouping.getInput();
                }
                case SetOperationPlan operation -> {
                    id = operation.getLeft().getOutput().getColumnId(operation.getOutput().getColumnIndexById(id));
                    plan = operation.getLeft();
                }
                default -> {
                    LogicalPlan next = null;
                    for (int i = 0, n = plan.inputCount(); i < n && next == null; i++) {
                        final LogicalPlan input = plan.inputAt(i);
                        if (input != null && input.getOutput().getColumnIndexById(id) >= 0) {
                            next = input;
                        }
                    }
                    if (next == null) {
                        return false;
                    }
                    plan = next;
                }
            }
        }
    }

    /**
     * The SAMPLE BY bucket the aggregate groups by: the timestamp_floor_utc key over the input's designated
     * timestamp, with the period as its first argument. No other key reads that timestamp directly, as the
     * select expressions that do compute over the bucket.
     */
    private static FunctionExpression sampleByBucket(AggregatePlan aggregate) {
        final int timestampId = aggregate.getInput().getOutput().getTimestampColumnId();
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i) instanceof FunctionExpression call
                    && TimestampFloorFromOffsetUtcFunctionFactory.NAME.equals(call.getName()) && call.getArgumentCount() == 5
                    && call.argumentAt(0) instanceof ConstantExpression
                    && call.argumentAt(1) instanceof ColumnExpression column && column.getColumnId() == timestampId) {
                return call;
            }
        }
        throw new IllegalStateException("SAMPLE BY bucket is not a grouping key");
    }

    private static String sampleByTimestampName(SqlExecutionContext sqlExecutionContext, TableToken baseTableToken) {
        try (TableMetadata metadata = sqlExecutionContext.getCairoEngine().getTableMetadata(baseTableToken)) {
            final int index = metadata.getTimestampIndex();
            return index < 0 ? null : Chars.toString(metadata.getColumnName(index));
        }
    }

    /**
     * The timestamp_floor call that defines visible column {@code index} of {@code project}: its expression, or the
     * key of the grouping beneath it that the column reads as spelled; null for any other column.
     */
    private static FunctionExpression timestampFloorCall(ProjectPlan project, int index) {
        if (!project.getOutput().isVisible(index)) {
            return null;
        }
        final BoundExpression expression = project.getExpressions().getQuick(index);
        if (isTimestampFloor(expression)) {
            return (FunctionExpression) expression;
        }
        if (!(expression instanceof ColumnExpression column) || column.isCast()) {
            return null;
        }
        LogicalPlan input = project.getInput();
        while (input instanceof SortPlan || input instanceof LimitPlan || input instanceof FillPlan) {
            input = input.inputAt(0);
        }
        if (!(input instanceof GroupingPlan grouping)) {
            return null;
        }
        final int keyIndex = grouping.getOutput().getColumnIndexById(column.getColumnId());
        return keyIndex >= 0 && keyIndex < grouping.getGroupingExpressions().size() && isTimestampFloor(grouping.getGroupingExpressions().getQuick(keyIndex))
                ? (FunctionExpression) grouping.getGroupingExpressions().getQuick(keyIndex) : null;
    }

    /**
     * The scan whose column, left in {@link #scanColumnId}, column {@code id} of {@code plan} reads as a plain
     * column: through projections and grouping keys that copy it, the SAMPLE BY bucket that reads the timestamp a
     * select column spells, joins and set operations (the right branch first); null when a computation defines
     * the column.
     */
    private ScanPlan columnScan(LogicalPlan plan, int id) {
        while (true) {
            switch (plan) {
                case ScanPlan scan -> {
                    scanColumnId = id;
                    return scan;
                }
                case ProjectPlan project -> {
                    if (!(project.getExpressions().getQuick(project.getOutput().getColumnIndexById(id)) instanceof ColumnExpression column) || column.isCast()) {
                        return null;
                    }
                    id = column.getColumnId();
                    plan = project.getInput();
                }
                case GroupingPlan grouping -> {
                    final int index = grouping.getOutput().getColumnIndexById(id);
                    if (index >= grouping.getGroupingExpressions().size()) {
                        return null;
                    }
                    final BoundExpression key = grouping.getGroupingExpressions().getQuick(index);
                    final ColumnExpression column;
                    if (key instanceof ColumnExpression keyColumn) {
                        column = keyColumn.isCast() ? null : keyColumn;
                    } else if (grouping instanceof AggregatePlan aggregate && aggregate.hasSampleByBucket() && key == sampleByBucket(aggregate)) {
                        column = (ColumnExpression) ((FunctionExpression) key).argumentAt(1);
                    } else {
                        column = null;
                    }
                    if (column == null) {
                        return null;
                    }
                    id = column.getColumnId();
                    plan = grouping.getInput();
                }
                case SetOperationPlan operation -> {
                    final int index = operation.getOutput().getColumnIndexById(id);
                    final ScanPlan right = columnScan(operation.getRight(), operation.getRight().getOutput().getColumnId(index));
                    if (right != null) {
                        return right;
                    }
                    id = operation.getLeft().getOutput().getColumnId(index);
                    plan = operation.getLeft();
                }
                default -> {
                    LogicalPlan next = null;
                    for (int i = 0, n = plan.inputCount(); i < n && next == null; i++) {
                        final LogicalPlan input = plan.inputAt(i);
                        if (input != null && input.getOutput().getColumnIndexById(id) >= 0) {
                            next = input;
                        }
                    }
                    if (next == null) {
                        return null;
                    }
                    plan = next;
                }
            }
        }
    }

    private void copyBaseTableSymbolColumnCapacity(
            @NotNull LogicalPlan root,
            int columnId,
            @NotNull CreateTableColumnModel columnModel,
            @NotNull TableToken baseTableToken,
            @NotNull TableMetadata baseTableMetadata
    ) {
        final ScanPlan scan = columnScan(root, columnId);
        if (scan == null || !scan.getTableToken().equals(baseTableToken)) {
            return;
        }
        final int columnIndex = baseTableMetadata.getColumnIndexQuiet(scan.getOutput().getColumnName(scan.getOutput().getColumnIndexById(scanColumnId)));
        if (columnIndex > -1) {
            final TableColumnMetadata baseTableColumnMetadata = baseTableMetadata.getColumnMetadata(columnIndex);
            if (baseTableColumnMetadata.getColumnType() == ColumnType.SYMBOL) {
                columnModel.setSymbolCapacity(baseTableColumnMetadata.getSymbolCapacity());
            }
        }
    }

    private CharSequence intervalText(ConstantExpression interval) {
        return switch (ColumnType.tagOf(interval.getDataType())) {
            case ColumnType.VARCHAR -> interval.getVarcharValue().asAsciiCharSequence();
            case ColumnType.CHAR -> {
                intervalSink.clear();
                intervalSink.put((char) interval.getLongValue());
                yield intervalSink;
            }
            default -> interval.getStrValue();
        };
    }

    /**
     * Whether a column of the root reads, as a plain column, a table column named {@code timestampName}.
     */
    private boolean isTimestampSelected(LogicalPlan root, CharSequence timestampName) {
        final OutputSchema columns = root.getOutput();
        for (int i = 0, n = columns.getColumnCount(); i < n; i++) {
            final ScanPlan scan = columnScan(root, columns.getColumnId(i));
            if (scan != null && Chars.equalsIgnoreCase(scan.getOutput().getColumnName(scan.getOutput().getColumnIndexById(scanColumnId)), timestampName)) {
                return true;
            }
        }
        return false;
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
            if (ttlHoursOrMonths != 0) {
                // Don't forget to validate TTL against PARTITION BY. Negative values are
                // months-based TTL; validateTtlGranularity handles both signs.
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, createTableOperation.getTtlPosition());
            }
        }
    }
}
