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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MatViewDefinition implements Mutable {
    public static final String MAT_VIEW_DEFINITION_FILE_NAME = "_mv";
    public static final int MAT_VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE = 1;
    public static final int MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;
    // Immediate refresh means that the mat view is refreshed on data transactions in the base table.
    public static final int REFRESH_TYPE_IMMEDIATE = 0;
    // Manual refresh means that the mat view is refreshed only when the user runs REFRESH SQL, e.g.
    // `REFRESH MATERIALIZED VIEW my_manual_view INCREMENTAL;`
    public static final int REFRESH_TYPE_MANUAL = 2;
    // Timer refresh means that the mat view is refreshed on time intervals.
    public static final int REFRESH_TYPE_TIMER = 1;
    private static final Log LOG = LogFactory.getLog(MatViewDefinition.class);
    private String baseTableName;
    private TimestampDriver baseTableTimestampDriver;
    private int baseTableTimestampType = ColumnType.UNDEFINED;
    private boolean deferred;
    // Not persisted, parsed from timeZoneOffset.
    private long fixedOffset;
    private String matViewSql;
    private volatile TableToken matViewToken;
    private int periodDelay;
    private char periodDelayUnit;
    private int periodLength;
    private char periodLengthUnit;
    // Not persisted, parsed from periodLength and periodLengthUnit.
    // Access must be synchronized as this object is not thread-safe.
    private TimestampSampler periodSampler;
    private int refreshLimitHoursOrMonths;
    private int refreshType = -1;
    // Not persisted, parsed from timeZone.
    private @Nullable TimeZoneRules rules;
    private long samplingInterval;
    private char samplingIntervalUnit;
    private @Nullable String timeZone;
    private @Nullable String timeZoneOffset;
    private int timerInterval;
    // Not persisted, parsed from timerTimeZone.
    private @Nullable TimeZoneRules timerRulesUs;
    private long timerStartUs = Numbers.LONG_NULL;
    private @Nullable String timerTimeZone;
    private char timerUnit;
    // Not persisted, parsed from samplingInterval and samplingIntervalUnit.
    // Access must be synchronized as this object is not thread-safe.
    private TimestampSampler timestampSampler;

    public static void append(@NotNull MatViewDefinition matViewDefinition, @NotNull AppendableBlock block) {
        final int refreshTypeRaw = encodeRefreshTypeAndDeferred(matViewDefinition.refreshType, matViewDefinition.deferred);
        block.putInt(refreshTypeRaw);
        block.putStr(matViewDefinition.baseTableName);
        block.putLong(matViewDefinition.samplingInterval);
        block.putChar(matViewDefinition.samplingIntervalUnit);
        block.putStr(matViewDefinition.timeZone);
        block.putStr(matViewDefinition.timeZoneOffset);
        block.putStr(matViewDefinition.matViewSql);
    }

    public static void append(@NotNull MatViewDefinition matViewDefinition, @NotNull BlockFileWriter writer) {
        AppendableBlock block = writer.append();
        append(matViewDefinition, block);
        block.commit(MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE);
        block = writer.append();
        appendExtra(matViewDefinition, block);
        block.commit(MAT_VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE);
        writer.commit();
    }

    public static void appendExtra(@NotNull MatViewDefinition matViewDefinition, @NotNull AppendableBlock block) {
        block.putInt(matViewDefinition.refreshLimitHoursOrMonths);
        block.putInt(matViewDefinition.timerInterval);
        block.putChar(matViewDefinition.timerUnit);
        block.putLong(matViewDefinition.timerStartUs);
        block.putStr(matViewDefinition.timerTimeZone);
        block.putInt(matViewDefinition.periodLength);
        block.putChar(matViewDefinition.periodLengthUnit);
        block.putInt(matViewDefinition.periodDelay);
        block.putChar(matViewDefinition.periodDelayUnit);
    }

    public static void readFrom(
            @NotNull CairoEngine engine,
            @NotNull MatViewDefinition destDefinition,
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken viewToken
    ) {
        path.trimTo(rootLen).concat(viewToken.getDirName()).concat(MAT_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());

        boolean definitionBlockFound = false;
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE) {
                definitionBlockFound = true;
                readDefinitionBlock(engine, destDefinition, block, viewToken);
                // keep going, because V2 block might follow
                continue;
            }
            if (block.type() == MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE) {
                readExtraBlock(destDefinition, block);
                return;
            }
        }

        if (!definitionBlockFound) {
            throw CairoException.critical(0)
                    .put("cannot read materialized view definition, block not found [path=").put(path)
                    .put(']');
        }

        // Timer settings used to be stored in table meta, but later on we moved them to view definition.
        // So, there may be older mat views with timer refresh type and no interval present in the definition.
        // As a fallback, treat them as manual refresh views.
        if (destDefinition.refreshType == REFRESH_TYPE_TIMER && destDefinition.timerInterval == 0) {
            LOG.error().$("cannot find timer interval value, falling back to manual refresh [view=").$(viewToken).I$();
            destDefinition.refreshType = REFRESH_TYPE_MANUAL;
        }
    }

    @Override
    public void clear() {
        matViewToken = null;
        baseTableName = null;
        matViewSql = null;
        rules = null;
        timeZone = null;
        timeZoneOffset = null;
        timestampSampler = null;
        fixedOffset = 0;
        refreshType = -1;
        deferred = false;
        samplingInterval = 0;
        samplingIntervalUnit = 0;
        refreshLimitHoursOrMonths = 0;
        timerInterval = 0;
        timerUnit = 0;
        timerStartUs = Numbers.LONG_NULL;
        timerTimeZone = null;
        timerRulesUs = null;
        periodLength = 0;
        periodLengthUnit = 0;
        periodDelay = 0;
        periodDelayUnit = 0;
        baseTableTimestampDriver = null;
        baseTableTimestampType = ColumnType.UNDEFINED;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public TimestampDriver getBaseTableTimestampDriver() {
        return baseTableTimestampDriver;
    }

    public long getFixedOffset() {
        return fixedOffset;
    }

    public String getMatViewSql() {
        return matViewSql;
    }

    public TableToken getMatViewToken() {
        return matViewToken;
    }

    public int getPeriodDelay() {
        return periodDelay;
    }

    public char getPeriodDelayUnit() {
        return periodDelayUnit;
    }

    public int getPeriodLength() {
        return periodLength;
    }

    public char getPeriodLengthUnit() {
        return periodLengthUnit;
    }

    public TimestampSampler getPeriodSampler() {
        return periodSampler;
    }

    /**
     * Returns incremental refresh limit for the materialized view:
     * if positive, it's in hours;
     * if negative, it's in months (and the actual value is positive);
     * zero means "no refresh limit".
     */
    public int getRefreshLimitHoursOrMonths() {
        return refreshLimitHoursOrMonths;
    }

    public int getRefreshType() {
        return refreshType;
    }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public char getSamplingIntervalUnit() {
        return samplingIntervalUnit;
    }

    public @Nullable String getTimeZone() {
        return timeZone;
    }

    public @Nullable String getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public int getTimerInterval() {
        return timerInterval;
    }

    public long getTimerStartUs() {
        return timerStartUs;
    }

    public @Nullable String getTimerTimeZone() {
        return timerTimeZone;
    }

    public @Nullable TimeZoneRules getTimerTzRulesUs() {
        return timerRulesUs;
    }

    public char getTimerUnit() {
        return timerUnit;
    }

    public TimestampSampler getTimestampSampler() {
        return timestampSampler;
    }

    public @Nullable TimeZoneRules getTzRules() {
        return rules;
    }

    public void init(
            int refreshType,
            boolean deferred,
            int timestampType,
            @NotNull TableToken matViewToken,
            @NotNull String matViewSql,
            @NotNull String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            @Nullable String timeZone,
            @Nullable String timeZoneOffset,
            int refreshLimitHoursOrMonths,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable String timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        initDefinition(
                refreshType,
                deferred,
                timestampType,
                matViewToken,
                matViewSql,
                baseTableName,
                samplingInterval,
                samplingIntervalUnit,
                timeZone,
                timeZoneOffset
        );
        initDefinitionExtra(
                refreshLimitHoursOrMonths,
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

    public boolean isDeferred() {
        return deferred;
    }

    public void setPeriodSampler(TimestampSampler periodSampler) {
        this.periodSampler = periodSampler;
    }

    public MatViewDefinition updateRefreshLimit(int refreshLimitHoursOrMonths) {
        final MatViewDefinition newDefinition = new MatViewDefinition();
        newDefinition.init(
                refreshType,
                deferred,
                baseTableTimestampType,
                matViewToken,
                matViewSql,
                baseTableName,
                samplingInterval,
                samplingIntervalUnit,
                timeZone,
                timeZoneOffset,
                refreshLimitHoursOrMonths,
                timerInterval,
                timerUnit,
                timerStartUs,
                timerTimeZone,
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit
        );
        return newDefinition;
    }

    public MatViewDefinition updateRefreshParams(
            int refreshType,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable CharSequence timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        final MatViewDefinition newDefinition = new MatViewDefinition();
        newDefinition.init(
                refreshType,
                deferred,
                baseTableTimestampType,
                matViewToken,
                matViewSql,
                baseTableName,
                samplingInterval,
                samplingIntervalUnit,
                timeZone,
                timeZoneOffset,
                refreshLimitHoursOrMonths,
                timerInterval,
                timerUnit,
                timerStartUs,
                Chars.toString(timerTimeZone),
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit
        );
        return newDefinition;
    }

    public MatViewDefinition updateTimer(int timerInterval, char timerUnit, long timerStartUs) {
        final MatViewDefinition newDefinition = new MatViewDefinition();
        newDefinition.init(
                refreshType,
                deferred,
                baseTableTimestampType,
                matViewToken,
                matViewSql,
                baseTableName,
                samplingInterval,
                samplingIntervalUnit,
                timeZone,
                timeZoneOffset,
                refreshLimitHoursOrMonths,
                timerInterval,
                timerUnit,
                timerStartUs,
                timerTimeZone,
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit
        );
        return newDefinition;
    }

    public void updateToken(TableToken updatedToken) {
        this.matViewToken = updatedToken;
    }

    private static boolean decodeDeferred(int refreshTypeRaw) {
        return refreshTypeRaw < 0;
    }

    private static int decodeRefreshType(int refreshTypeRaw) {
        return refreshTypeRaw < 0 ? Math.abs(refreshTypeRaw + 1) : refreshTypeRaw;
    }

    private static int encodeRefreshTypeAndDeferred(int refreshType, boolean deferred) {
        // Keep pre-deferred definitions compatible with the new refresh type format.
        return deferred ? -(refreshType + 1) : refreshType;
    }

    private static void readDefinitionBlock(
            CairoEngine engine,
            MatViewDefinition destDefinition,
            ReadableBlock block,
            TableToken matViewToken
    ) {
        assert block.type() == MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE;

        long offset = 0;
        final int refreshTypeRaw = block.getInt(offset);
        final boolean deferred = decodeDeferred(refreshTypeRaw);
        final int refreshType = decodeRefreshType(refreshTypeRaw);
        if (refreshType != REFRESH_TYPE_IMMEDIATE && refreshType != REFRESH_TYPE_TIMER && refreshType != REFRESH_TYPE_MANUAL) {
            throw CairoException.critical(0)
                    .put("unsupported refresh type [view=")
                    .put(matViewToken.getTableName())
                    .put(", type=")
                    .put(refreshType)
                    .put(']');
        }
        offset += Integer.BYTES;

        final CharSequence baseTableName = block.getStr(offset);
        if (baseTableName == null || baseTableName.length() == 0) {
            throw CairoException.critical(0)
                    .put("base table name for materialized view is empty [view=")
                    .put(matViewToken.getTableName())
                    .put(']');
        }
        offset += Vm.getStorageLength(baseTableName);
        final String baseTableNameStr = Chars.toString(baseTableName);

        final long samplingInterval = block.getLong(offset);
        offset += Long.BYTES;

        final char samplingIntervalUnit = block.getChar(offset);
        offset += Character.BYTES;

        final CharSequence timeZone = block.getStr(offset);
        offset += Vm.getStorageLength(timeZone);
        final String timeZoneStr = Chars.toString(timeZone);

        final CharSequence timeZoneOffset = block.getStr(offset);
        offset += Vm.getStorageLength(timeZoneOffset);
        final String timeZoneOffsetStr = Chars.toString(timeZoneOffset);

        final CharSequence matViewSql = block.getStr(offset);
        if (matViewSql == null || matViewSql.length() == 0) {
            throw CairoException.critical(0)
                    .put("materialized view SQL is empty [view=")
                    .put(matViewToken.getTableName())
                    .put(']');
        }

        // Mat view's and base table's timestamp types must always match.
        final int timestampType;
        try (TableMetadata metadata = engine.getTableMetadata(matViewToken)) {
            timestampType = metadata.getTimestampType();
        }

        destDefinition.initDefinition(
                refreshType,
                deferred,
                timestampType,
                matViewToken,
                Chars.toString(matViewSql),
                baseTableNameStr,
                samplingInterval,
                samplingIntervalUnit,
                timeZoneStr,
                timeZoneOffsetStr
        );
    }

    private static void readExtraBlock(
            MatViewDefinition destDefinition,
            ReadableBlock block
    ) {
        assert block.type() == MAT_VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE;

        long offset = 0;
        final int refreshLimitHoursOrMonths = block.getInt(offset);
        offset += Integer.BYTES;

        final int timerInterval = block.getInt(offset);
        offset += Integer.BYTES;

        final char timerUnit = block.getChar(offset);
        offset += Character.BYTES;

        final long timerStartUs = block.getLong(offset);
        offset += Long.BYTES;

        final CharSequence timerTimeZone = block.getStr(offset);
        offset += Vm.getStorageLength(timerTimeZone);

        final int periodLength = block.getInt(offset);
        offset += Integer.BYTES;

        final char periodLengthUnit = block.getChar(offset);
        offset += Character.BYTES;

        final int periodDelay = block.getInt(offset);
        offset += Integer.BYTES;

        final char periodDelayUnit = block.getChar(offset);

        destDefinition.initDefinitionExtra(
                refreshLimitHoursOrMonths,
                timerInterval,
                timerUnit,
                timerStartUs,
                Chars.toString(timerTimeZone),
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit
        );
    }

    private void initDefinition(
            int refreshType,
            boolean deferred,
            int timestampType,
            @NotNull TableToken matViewToken,
            @NotNull String matViewSql,
            @NotNull String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            @Nullable String timeZone,
            @Nullable String timeZoneOffset
    ) {
        this.refreshType = refreshType;
        this.deferred = deferred;
        this.matViewToken = matViewToken;
        this.matViewSql = matViewSql;
        this.baseTableName = baseTableName;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
        this.baseTableTimestampType = timestampType;
        this.baseTableTimestampDriver = ColumnType.getTimestampDriver(baseTableTimestampType);

        try {
            this.timestampSampler = TimestampSamplerFactory.getInstance(
                    baseTableTimestampDriver,
                    samplingInterval,
                    samplingIntervalUnit,
                    0
            );
        } catch (SqlException e) {
            throw CairoException.critical(0).put("invalid sampling interval and/or unit: ").put(samplingInterval)
                    .put(", ").put(samplingIntervalUnit);
        }

        if (timeZone != null) {
            this.rules = baseTableTimestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timeZone);
        } else {
            this.rules = null;
        }

        if (timeZoneOffset != null) {
            final long val = Dates.parseOffset(timeZoneOffset);
            if (val == Numbers.LONG_NULL) {
                throw CairoException.critical(0).put("invalid offset: ").put(timeZoneOffset);
            }
            this.fixedOffset = baseTableTimestampDriver.fromMinutes(Numbers.decodeLowInt(val));
        } else {
            this.fixedOffset = 0;
        }
    }

    private void initDefinitionExtra(
            int refreshLimitHoursOrMonths,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable String timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        this.refreshLimitHoursOrMonths = refreshLimitHoursOrMonths;
        this.timerInterval = timerInterval;
        this.timerUnit = timerUnit;
        this.timerStartUs = timerStartUs;
        this.timerTimeZone = timerTimeZone;
        this.periodLength = periodLength;
        this.periodLengthUnit = periodLengthUnit;
        this.periodDelay = periodDelay;
        this.periodDelayUnit = periodDelayUnit;
        if (timerTimeZone != null) {
            this.timerRulesUs = MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timerTimeZone);
        } else {
            this.timerRulesUs = null;
        }
    }
}
