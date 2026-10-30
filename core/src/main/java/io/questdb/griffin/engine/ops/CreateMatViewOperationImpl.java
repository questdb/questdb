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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
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
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
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
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.FixedTimeZoneRule;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
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
    private static final TimeZoneRules UTC_RULES = new FixedTimeZoneRule(0);
    private static final String ZERO_OFFSET = "00:00";
    private final String baseTableName;
    private final int baseTableNamePosition;
    private final CairoConfiguration configuration;
    private final LowerCaseCharSequenceObjHashMap<CreateTableColumnModel> createColumnModelMap = new LowerCaseCharSequenceObjHashMap<>();
    private final boolean deferred;
    private final ObjList<GroupingPlan> nestedSampleBys = new ObjList<>();
    private final int periodDelay;
    private final char periodDelayUnit;
    private final int refreshType;
    private final String sqlText;
    private final int timerInterval;
    private final char timerUnit;
    private final MatViewDefinition viewDefinition = new MatViewDefinition();
    private int baseTableTimestampType;
    private CreateTableOperationImpl createTableOperation;
    private int nonConstantAlignmentPosition = -1;
    private int periodLength;
    private char periodLengthUnit;
    private long samplingInterval;
    private char samplingIntervalUnit;
    private int scanColumnId;
    private String timeZone;
    private String timeZoneOffset = ZERO_OFFSET;
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

        // Find sampling interval: a SAMPLE BY whose bucket the view selects, or else the timestamp_floor() that
        // spells the view's timestamp.
        int intervalPos = 0;
        GroupingPlan sampleBy = findSampleBy(root);
        final int bucketIndex = sampleBy != null ? copyingColumnIndex(root, sampleBy, bucketColumnId(sampleBy)) : -1;
        FunctionExpression floor = null;
        ProjectPlan floorLevel = null;
        int floorIndex = 0;
        if (bucketIndex < 0) {
            for (LogicalPlan level = root; level != null && floor == null; level = nextLevel(level)) {
                if (level instanceof ProjectPlan project) {
                    for (int i = 0, n = project.getExpressions().size(); i < n && floor == null; i++) {
                        floor = timestampFloorCall(project, i);
                        floorLevel = project;
                        floorIndex = i;
                    }
                }
            }
            if (sampleBy != null && floor == bucketKey(sampleBy)) {
                floor = null;
            } else if (floor != null) {
                sampleBy = null;
            }
        }
        // Vanilla SAMPLE BY.
        if (sampleBy != null) {
            samplingInterval = sampleBy.getStride();
            samplingIntervalUnit = sampleBy.getPeriodUnit();
            intervalPos = sampleBy.getPeriodPosition();
            readSampleByAlignment(sampleBy.getTimezone(), sampleBy.getOffset(), selectTextPosition);
            if (sampleBy instanceof AggregatePlan && timestamp == null && bucketIndex >= 0) {
                createTableOperation.setTimestampColumnName(Chars.toString(columns.getColumnName(bucketIndex)));
                createTableOperation.setTimestampColumnNamePosition(positions.getQuick(bucketIndex));
            }
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
        CharSequence intervalExpr = null;
        LogicalPlan floorInput = null;
        if (floor != null) {
            floorInput = floor == floorLevel.getExpressions().getQuick(floorIndex) ? floorLevel.getChild() : groupingBelow(floorLevel).getChild();
            if (timestamp == null) {
                // The persisted designated-timestamp name is resolved verbatim against factory metadata
                // downstream, and the model map is keyed by the same output names.
                final String tsName = Chars.toString(floorLevel.getOutput().getColumnName(floorIndex));
                createTableOperation.setTimestampColumnName(tsName);
                createTableOperation.setTimestampColumnNamePosition(floor.getPosition());
                if (createColumnModelMap.get(tsName) == null) {
                    throw SqlException.position(selectTextPosition)
                            .put("TIMESTAMP column does not exist or not present in select list [name=")
                            .put(tsName).put(']');
                }
            }
            final ConstantExpression interval = (ConstantExpression) floor.argumentAt(0);
            intervalExpr = constantText(interval);
            intervalPos = interval.getPosition();
        }

        // We haven't found timestamp_floor() in SELECT.
        if (sampleBy == null && intervalExpr == null) {
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

        if (floor != null) {
            final CharSequence interval = GenericLexer.unquote(intervalExpr);
            samplingInterval = TimestampSamplerFactory.parsePeriodStride(interval, intervalPos);
            samplingIntervalUnit = interval.charAt(TimestampSamplerFactory.periodUnitIndex(interval));
            final int flooredColumnId = readTimestampFloorAlignment(sqlExecutionContext, floor, floorInput, baseTableToken, selectTextPosition);
            intervalPos = fitNestedSampleBys(floorInput, flooredColumnId, intervalPos, selectTextPosition);
        } else {
            intervalPos = fitNestedSampleBys(sampleBy.getChild(), sampleBy.getTimestampColumnId(), intervalPos, selectTextPosition);
        }

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
            assert timerTimeZone == null;
            timerTimeZone = timeZone;
            final TimeZoneRules rules = CreateMatViewOperation.periodTimeZoneRules(timerTimeZone, intervalPos);
            CreateMatViewOperation.validatePeriodSampleByOffset(timeZoneOffset, intervalPos);
            timerStartUs = CreateMatViewOperation.periodTimerStartUs(configuration.getMicrosecondClock(), periodLength, periodLengthUnit, rules, intervalPos);
        }
    }

    @Override
    public void validateAndUpdateMetadataFromSelect(
            @NotNull RecordMetadata selectMetadata,
            @NotNull TableReaderMetadata baseTableMetadata,
            int scanDirection
    ) throws SqlException {
        if (nonConstantAlignmentPosition > -1) {
            throw SqlException.position(nonConstantAlignmentPosition)
                    .put("SAMPLE BY in a materialized view requires a constant offset and time zone");
        }
        final int selectTextPosition = createTableOperation.getSelectTextPosition();
        // SELECT validation
        if (createTableOperation.getTimestampColumnName() == null
                && (selectMetadata.getTimestampIndex() == -1 || scanDirection != RecordCursorFactory.SCAN_DIRECTION_FORWARD)) {
            throw SqlException.position(selectTextPosition)
                    .put("materialized view query is required to have designated timestamp");
        }
        createTableOperation.validateAndUpdateMetadataFromSelect(selectMetadata, scanDirection);
        updateMatViewTablePartitionBy(createTableOperation.getTimestampType());
        this.baseTableTimestampType = baseTableMetadata.getTimestampType();
    }

    /**
     * The output column id of the bucket key of the SAMPLE BY {@code sampleBy}; -1 when it has none.
     */
    private static int bucketColumnId(GroupingPlan sampleBy) {
        final int index = sampleBy.getBucketKeyIndex();
        return index < 0 ? -1 : sampleBy.getOutput().getColumnId(index);
    }

    /**
     * The bucket key of the SAMPLE BY {@code sampleBy}; null when it has none.
     */
    private static BoundExpression bucketKey(GroupingPlan sampleBy) {
        final int index = sampleBy.getBucketKeyIndex();
        return index < 0 ? null : sampleBy.getGroupingExpressions().getQuick(index);
    }

    /**
     * The text of a STRING, VARCHAR or CHAR constant; null for a NULL value.
     */
    private static String constantText(ConstantExpression constant) {
        return ColumnType.tagOf(constant.getDataType()) == ColumnType.CHAR
                ? String.valueOf((char) constant.getLongValue()) : Chars.toString(constant.getText());
    }

    /**
     * Whether column {@code id} of {@code plan} copies column {@code sourceId} of {@code source} as a plain column,
     * through the projections and the row-preserving levels between them, whatever order they sort the rows in.
     */
    private static boolean copiesColumn(LogicalPlan plan, int id, LogicalPlan source, int sourceId) {
        while (plan != source) {
            switch (plan) {
                case ProjectPlan project -> {
                    if (!(project.getExpressions().getQuick(project.getOutput().getColumnIndexById(id)) instanceof ColumnExpression column)
                            || column.isCast()) {
                        return false;
                    }
                    id = column.getColumnId();
                }
                case ForwardingPlan _, WindowPlan _ -> {
                }
                default -> {
                    return false;
                }
            }
            plan = plan.inputAt(0);
        }
        return id == sourceId;
    }

    /**
     * The index of the last root column that copies column {@code sourceId} of {@code source}, or -1 when none does;
     * the last, as the binder names a SAMPLE BY bucket after the last alias of the timestamp.
     */
    private static int copyingColumnIndex(LogicalPlan root, LogicalPlan source, int sourceId) {
        final OutputSchema columns = root.getOutput();
        for (int i = columns.getColumnCount() - 1; i >= 0; i--) {
            if (copiesColumn(root, columns.getColumnId(i), source, sourceId)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The first SAMPLE BY, plain select levels down from the root, whose period is spelled as a constant: its
     * cursor, or the aggregate grouped by its bucket; null when there is none. A level that aggregates, joins, reads
     * a table with LATEST ON or is a set operation ends the search, as the outermost aggregate's bucket is the view's
     * grid.
     */
    private static GroupingPlan findSampleBy(LogicalPlan root) {
        LogicalPlan plan = root;
        while (true) {
            switch (plan) {
                case GroupingPlan grouping -> {
                    return grouping.getPeriodToken() != null ? grouping : null;
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
     * Whether each bucket of the inner bucketing lies within one bucket of the outer one, given their strides, units,
     * time zones and offsets in minutes, from the epoch on, as no designated timestamp precedes it. Sub-day buckets
     * floor on the zone's standard offset, see {@link CommonUtils#floorUtcToLocal}, so they start a whole number of
     * strides from an origin, as do the days of a zone that converts their local start by one offset. A coarser
     * bucketing fits them when its stride is a multiple of theirs and each of its starts lies a whole number of their
     * strides from that origin: a sub-day start by its own origin, and a start of a day or longer by each offset by
     * which its zone converts its local time, see {@link TimeZoneRules#getLocalTimeOffsetGcd}. Days whose offsets vary,
     * weeks, months and years fit only the local starts of the same time zone: days under a multiple of days, weeks,
     * months or years at an offset a whole number of strides apart, weeks under weeks, and months under months and
     * years, at the same offset.
     */
    private static boolean fits(
            long innerStride,
            char innerUnit,
            @Nullable CharSequence innerZone,
            int innerOffset,
            long outerStride,
            char outerUnit,
            @Nullable CharSequence outerZone,
            int outerOffset,
            TimestampDriver driver
    ) {
        final TimeZoneRules innerRules = timeZoneRules(innerZone, driver);
        final TimeZoneRules outerRules = timeZoneRules(outerZone, driver);
        if (innerRules == null || outerRules == null) {
            return false;
        }
        final long innerShift = driver.fromMinutes(innerOffset);
        final long outerShift = driver.fromMinutes(outerOffset);
        final boolean isOuterSubDay = CommonUtils.isSubDayUnit(outerUnit);
        if (CommonUtils.isFixedAlignedUnit(innerUnit)) {
            final long length = driver.from(innerStride, innerUnit);
            if (length <= 0) {
                return false;
            }
            final long outerPeriod = isOuterSubDay || outerUnit == 'd' ? driver.from(outerStride, outerUnit) : driver.fromDays(1);
            final boolean isInnerSubDay = CommonUtils.isSubDayUnit(innerUnit);
            if (isInnerSubDay || innerRules.getLocalTimeOffsetGcd(0, innerShift) == 0) {
                final long origin = innerShift - (isInnerSubDay ? innerRules.getStandardOffset() : innerRules.getOffset(0));
                if (isOuterSubDay) {
                    return outerPeriod % length == 0 && Math.floorMod(outerShift - outerRules.getStandardOffset() - origin, length) == 0;
                }
                return outerPeriod % length == 0
                        && outerRules.getLocalTimeOffsetGcd(0, outerShift) % length == 0
                        && Math.floorMod(outerShift - outerRules.getOffset(0) - origin, length) == 0;
            }
            return !isOuterSubDay
                    && isSameTimeZone(innerZone, innerRules, outerZone, outerRules)
                    && outerPeriod % length == 0
                    && Math.floorMod(outerShift - innerShift, length) == 0;
        }
        if (outerShift != innerShift || !isSameTimeZone(innerZone, innerRules, outerZone, outerRules)) {
            return false;
        }
        if (innerUnit == 'w') {
            return outerUnit == 'w' && outerStride % innerStride == 0;
        }
        return (outerUnit == 'M' || outerUnit == 'y') && (outerUnit == 'y' ? 12 * outerStride : outerStride) % (innerUnit == 'y' ? 12 * innerStride : innerStride) == 0;
    }

    /**
     * The grouping beneath {@code project}, under the sorts, limits and fills between them; null when there is none.
     */
    private static GroupingPlan groupingBelow(ProjectPlan project) {
        LogicalPlan input = project.getChild();
        while (input instanceof SortPlan || input instanceof LimitPlan || input instanceof FillPlan) {
            input = input.inputAt(0);
        }
        return input instanceof GroupingPlan grouping ? grouping : null;
    }

    /**
     * Whether the SAMPLE BY buckets a table directly, rather than a joined source or a sub-query.
     */
    private static boolean isDirectTableSampleBy(GroupingPlan sampleBy) {
        LogicalPlan input = sampleBy.getChild();
        while (input instanceof FilterPlan || input instanceof LatestByPlan) {
            input = input.inputAt(0);
        }
        return input instanceof ScanPlan;
    }

    /**
     * Whether a timestamp_floor from argument is NULL or the epoch, which floor from the epoch as the two-argument
     * form does.
     */
    private static boolean isEpochOrNull(BoundExpression from) {
        return from instanceof ConstantExpression origin
                && (ColumnType.isNull(origin.getDataType())
                || ColumnType.isTimestamp(origin.getDataType()) && (origin.getLongValue() == 0 || origin.getLongValue() == Numbers.LONG_NULL));
    }

    /**
     * Whether two time zones convert local times alike from the epoch on: the same name, or one offset that each of
     * them keeps, which UTC, standing for a null zone, does.
     */
    private static boolean isSameTimeZone(@Nullable CharSequence zone, TimeZoneRules rules, @Nullable CharSequence other, TimeZoneRules otherRules) {
        return zone != null && other != null && Chars.equalsIgnoreCase(zone, other)
                || rules.getLocalTimeOffsetGcd(0, 0) == 0 && otherRules.getLocalTimeOffsetGcd(0, 0) == 0 && rules.getOffset(0) == otherRules.getOffset(0);
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
                    aggregate.hasExplicitGrouping() || aggregate.getBucketKeyIndex() >= 0 ? null : aggregate.getChild();
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
                        return readsAggregate(project.getChild(), expression);
                    }
                    id = column.getColumnId();
                    plan = project.getChild();
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
                        return readsAggregate(grouping.getChild(), key);
                    }
                    id = column.getColumnId();
                    plan = grouping.getChild();
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

    private static String sampleByTimestampName(SqlExecutionContext sqlExecutionContext, TableToken baseTableToken) {
        try (TableMetadata metadata = sqlExecutionContext.getCairoEngine().getTableMetadata(baseTableToken)) {
            final int index = metadata.getTimestampIndex();
            return index < 0 ? null : Chars.toString(metadata.getColumnName(index));
        }
    }

    /**
     * The text of the constant offset or time zone argument {@code index} of a timestamp_floor call; null for NULL.
     */
    private static String timestampFloorArgument(FunctionExpression floor, int index, int selectTextPosition) throws SqlException {
        if (!(floor.argumentAt(index) instanceof ConstantExpression constant)) {
            throw SqlException.position(selectTextPosition + floor.argumentAt(index).getPosition()).put(floor.getName())
                    .put("() in a materialized view requires a constant offset and time zone");
        }
        return constantText(constant);
    }

    /**
     * The timestamp_floor call that defines visible column {@code index} of {@code project}: its expression, whose
     * arguments read the projection's input, or the key of the {@link #groupingBelow grouping beneath it} that the
     * column reads as spelled, whose arguments read the grouping's input; null for any other column.
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
        final GroupingPlan grouping = groupingBelow(project);
        if (grouping == null) {
            return null;
        }
        final int keyIndex = grouping.getOutput().getColumnIndexById(column.getColumnId());
        return keyIndex >= 0 && keyIndex < grouping.getGroupingExpressions().size() && isTimestampFloor(grouping.getGroupingExpressions().getQuick(keyIndex))
                ? (FunctionExpression) grouping.getGroupingExpressions().getQuick(keyIndex) : null;
    }

    /**
     * The rules of the time zone, or of UTC, which null stands for; null for a name that is not a time zone.
     */
    private static TimeZoneRules timeZoneRules(@Nullable CharSequence zone, TimestampDriver driver) {
        if (zone == null) {
            return UTC_RULES;
        }
        try {
            return driver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, zone);
        } catch (CairoException e) {
            return null;
        }
    }

    /**
     * The scan whose column, left in {@link #scanColumnId}, column {@code id} of {@code plan} reads as a plain
     * column: through projections and grouping keys that copy it, the SAMPLE BY bucket key that floors the timestamp,
     * joins and set operations (the right branch first); null when a computation defines the column. Each SAMPLE BY
     * whose bucket the column reads through goes to {@code sampleBys}, unless it is null.
     */
    private ScanPlan columnScan(LogicalPlan plan, int id, @Nullable ObjList<GroupingPlan> sampleBys) {
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
                    plan = project.getChild();
                }
                case GroupingPlan grouping -> {
                    final int index = grouping.getOutput().getColumnIndexById(id);
                    if (index >= grouping.getGroupingExpressions().size()) {
                        return null;
                    }
                    if (id == bucketColumnId(grouping)) {
                        if (sampleBys != null) {
                            sampleBys.add(grouping);
                        }
                        id = grouping.getTimestampColumnId();
                    } else if (grouping.getGroupingExpressions().getQuick(index) instanceof ColumnExpression column && !column.isCast()) {
                        id = column.getColumnId();
                    } else {
                        return null;
                    }
                    plan = grouping.getChild();
                }
                case SetOperationPlan operation -> {
                    final int index = operation.getOutput().getColumnIndexById(id);
                    final ScanPlan right = columnScan(operation.getRight(), operation.getRight().getOutput().getColumnId(index), sampleBys);
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
        final ScanPlan scan = columnScan(root, columnId, null);
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

    /**
     * Fits the view's grid to the SAMPLE BYs whose bucket column {@code timestampId} of {@code input}, the view's
     * timestamp, reads through, see {@link #columnScan}, and returns the position of the grid's period, given
     * {@code intervalPos}. A refresh recomputes whole grid buckets, so each nested bucket must lie within one of them,
     * see {@link #fits}. A nested SAMPLE BY whose buckets each hold whole grid buckets becomes the grid instead, as the
     * bucketing above it then keeps its bucket starts. Raises for a nested SAMPLE BY whose buckets do neither. Fits
     * nothing once a time zone or offset of the grid or of a nested SAMPLE BY is not a constant, see
     * {@link #sampleByAlignmentText}.
     */
    private int fitNestedSampleBys(LogicalPlan input, int timestampId, int intervalPos, int selectTextPosition) throws SqlException {
        if (nonConstantAlignmentPosition > -1) {
            return intervalPos;
        }
        try {
            columnScan(input, timestampId, nestedSampleBys);
            for (int i = 0, n = nestedSampleBys.size(); i < n; i++) {
                final GroupingPlan sampleBy = nestedSampleBys.getQuick(i);
                final String offsetText = sampleByAlignmentText(sampleBy.getOffset(), selectTextPosition);
                final String zone = sampleByAlignmentText(sampleBy.getTimezone(), selectTextPosition);
                if (nonConstantAlignmentPosition > -1) {
                    return intervalPos;
                }
                final long nestedOffset = offsetText != null ? Dates.parseOffset(offsetText) : 0;
                final long gridOffset = Dates.parseOffset(timeZoneOffset);
                if (nestedOffset != Numbers.LONG_NULL && gridOffset != Numbers.LONG_NULL) {
                    final long stride = sampleBy.getStride();
                    final char unit = sampleBy.getPeriodUnit();
                    final TimestampDriver driver = ColumnType.getTimestampDriver(sampleBy.getOutput().getColumnType(sampleBy.getBucketKeyIndex()));
                    final int nestedMinutes = Numbers.decodeLowInt(nestedOffset);
                    final int gridMinutes = Numbers.decodeLowInt(gridOffset);
                    if (fits(stride, unit, zone, nestedMinutes, samplingInterval, samplingIntervalUnit, timeZone, gridMinutes, driver)) {
                        continue;
                    }
                    if (fits(samplingInterval, samplingIntervalUnit, timeZone, gridMinutes, stride, unit, zone, nestedMinutes, driver)) {
                        samplingInterval = stride;
                        samplingIntervalUnit = unit;
                        timeZone = zone;
                        timeZoneOffset = offsetText != null ? offsetText : ZERO_OFFSET;
                        intervalPos = sampleBy.getPeriodPosition();
                        continue;
                    }
                }
                throw SqlException.position(selectTextPosition + sampleBy.getPeriodPosition())
                        .put("the materialized view's sampling interval and a nested SAMPLE BY interval must have aligned buckets, one evenly dividing the other");
            }
            return intervalPos;
        } finally {
            nestedSampleBys.clear();
        }
    }

    /**
     * Whether a column of the root reads, as a plain column, a table column named {@code timestampName}.
     */
    private boolean isTimestampSelected(LogicalPlan root, CharSequence timestampName) {
        final OutputSchema columns = root.getOutput();
        for (int i = 0, n = columns.getColumnCount(); i < n; i++) {
            final ScanPlan scan = columnScan(root, columns.getColumnId(i), null);
            if (scan != null && Chars.equalsIgnoreCase(scan.getOutput().getColumnName(scan.getOutput().getColumnIndexById(scanColumnId)), timestampName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads the TIME ZONE and WITH OFFSET of the SAMPLE BY the view samples by into the view's time zone and offset.
     */
    private void readSampleByAlignment(BoundExpression timezone, BoundExpression offset, int selectTextPosition) {
        final String offsetText = sampleByAlignmentText(offset, selectTextPosition);
        if (offsetText != null) {
            timeZoneOffset = offsetText;
        }
        timeZone = sampleByAlignmentText(timezone, selectTextPosition);
    }

    /**
     * Reads the bucket grid of the timestamp_floor call, whose arguments read {@code floorInput}, into the view's time
     * zone and offset, the way SAMPLE BY carries TIME ZONE and WITH OFFSET, and returns the id of the column of
     * {@code floorInput} it floors. Raises for a grid the definition cannot carry: a floored expression other than the
     * base table's designated timestamp read as itself or, at a grid no finer than a millisecond, as a DATE; a from
     * origin other than the epoch; an offset or time zone that is not a constant; or a time zone other than UTC on
     * timestamp_floor, which floors to local time.
     */
    private int readTimestampFloorAlignment(
            SqlExecutionContext sqlExecutionContext,
            FunctionExpression floor,
            LogicalPlan floorInput,
            TableToken baseTableToken,
            int selectTextPosition
    ) throws SqlException {
        final BoundExpression floored = floor.argumentAt(1);
        final BoundExpression read = floored instanceof FunctionExpression cast && SqlKeywords.isCastKeyword(cast.getName())
                && floored.getDataType() == ColumnType.DATE ? cast.argumentAt(0) : floored;
        final int readId = read instanceof ColumnExpression column ? column.getColumnId() : -1;
        final ScanPlan scan = readId > -1 ? columnScan(floorInput, readId, null) : null;
        final int scanIndex = scan != null ? scan.getOutput().getColumnIndexById(scanColumnId) : -1;
        if (scan == null || !scan.getTableToken().equals(baseTableToken)
                || !Chars.equalsIgnoreCase(scan.getOutput().getColumnName(scanIndex), sampleByTimestampName(sqlExecutionContext, baseTableToken))
                || floor.getDataType() != scan.getOutput().getColumnType(scanIndex)
                || floored.getDataType() == ColumnType.DATE && (samplingIntervalUnit == 'U' || samplingIntervalUnit == 'n')) {
            throw SqlException.position(selectTextPosition + floored.getPosition()).put(floor.getName())
                    .put("() in a materialized view must floor the designated timestamp of the base table");
        }
        final int argumentCount = floor.getArgumentCount();
        if (argumentCount > 2 && !isEpochOrNull(floor.argumentAt(2))) {
            throw SqlException.position(selectTextPosition + floor.argumentAt(2).getPosition()).put(floor.getName())
                    .put("() in a materialized view does not support a from origin, use the offset argument");
        }
        if (argumentCount == 5) {
            final String offset = timestampFloorArgument(floor, 3, selectTextPosition);
            if (offset != null) {
                timeZoneOffset = offset;
            }
            timeZone = timestampFloorArgument(floor, 4, selectTextPosition);
            if (timeZone != null && TimestampFloorFunctionFactory.NAME.equals(floor.getName())) {
                final TimeZoneRules rules = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timeZone);
                if (!rules.hasFixedOffset() || rules.getOffset(0) != 0) {
                    throw SqlException.$(selectTextPosition + floor.argumentAt(4).getPosition(),
                            "timestamp_floor() with a time zone floors to local time, use timestamp_floor_utc() in a materialized view");
                }
            }
        }
        return readId;
    }

    /**
     * The text of the constant TIME ZONE or WITH OFFSET {@code value} of a SAMPLE BY, the view's or a nested one; null
     * for none or NULL. The first value that is not a constant leaves its position in
     * {@link #nonConstantAlignmentPosition}, which {@link #validateAndUpdateMetadataFromSelect} rejects once the
     * compiler has rejected a non-deterministic one.
     */
    private String sampleByAlignmentText(BoundExpression value, int selectTextPosition) {
        if (value instanceof ConstantExpression constant) {
            return constantText(constant);
        }
        if (value != null && nonConstantAlignmentPosition < 0) {
            nonConstantAlignmentPosition = selectTextPosition + value.getPosition();
        }
        return null;
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
