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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.Nullable;

public interface CreateMatViewOperation extends TableStructure, Operation {

    /**
     * The start of the timer of a PERIOD (LENGTH) refresh, see {@link #periodTimerStartUs}: validates the length
     * {@code length} {@code lengthUnit}, failing at {@code lengthPos}, the time zone {@code timeZone}, null for none,
     * which the period starts in, failing at {@code timeZonePos}, and the delay {@code delay} {@code delayUnit}, a
     * zero unit for none, failing at {@code delayPos}.
     */
    static long lengthPeriodTimerStartUs(
            MicrosecondClock clock,
            int length,
            char lengthUnit,
            int lengthPos,
            @Nullable CharSequence timeZone,
            int timeZonePos,
            int delay,
            char delayUnit,
            int delayPos
    ) throws SqlException {
        validateMatViewPeriodLength(length, lengthUnit, lengthPos);
        final TimeZoneRules rules = periodTimeZoneRules(timeZone, timeZonePos);
        if (delayUnit != 0) {
            validateMatViewPeriodDelay(length, lengthUnit, delay, delayUnit, delayPos);
        }
        return periodTimerStartUs(clock, length, lengthUnit, rules, lengthPos);
    }

    /**
     * The length of a PERIOD (SAMPLE BY INTERVAL) refresh, which aligns the period to the SAMPLE BY bucket: the
     * sampling interval {@code samplingInterval}, clamped to INT.
     */
    static int samplePeriodLength(long samplingInterval) {
        return (int) Math.min(samplingInterval, Integer.MAX_VALUE);
    }

    /**
     * The start of the timer of a PERIOD (SAMPLE BY INTERVAL) refresh, see {@link #periodTimerStartUs}: validates the
     * {@link #samplePeriodLength length} {@code length} {@code lengthUnit}, the SAMPLE BY time zone {@code timeZone},
     * which the period starts in, and the SAMPLE BY offset {@code timeZoneOffset}, failing at {@code pos}.
     */
    static long samplePeriodTimerStartUs(
            MicrosecondClock clock,
            int length,
            char lengthUnit,
            @Nullable CharSequence timeZone,
            @Nullable CharSequence timeZoneOffset,
            int pos
    ) throws SqlException {
        validateMatViewPeriodLength(length, lengthUnit, pos);
        final TimeZoneRules rules = periodTimeZoneRules(timeZone, pos);
        validatePeriodSampleByOffset(timeZoneOffset, pos);
        return periodTimerStartUs(clock, length, lengthUnit, rules, pos);
    }

    CharSequence getBaseTableName();

    CreateTableOperation getCreateTableOperation();

    int getRefreshType();

    CharSequence getSqlText();

    int getTableNamePosition();

    CharSequence getVolumeAlias();

    int getVolumePosition();

    boolean ignoreIfExists();

    boolean isDeferred();

    /**
     * True for a non-aggregating "passthrough" view (e.g. {@code SELECT * FROM base}, no SAMPLE BY).
     */
    boolean isPassthrough();

    /**
     * Replaces the SQL text the view definition stores with {@code matViewSql}. CREATE calls it on every
     * compile pass with the query it rewrites: a passthrough view's top-level wildcard expanded into the
     * columns the view has, and bare zero-argument function names written as calls. Null stores the text
     * the user wrote.
     */
    void setMatViewSql(@Nullable String matViewSql);

    void updateOperationFutureTableToken(TableToken tableToken);

    /**
     * Validates the bound query of the view and records its columns, named at {@code positions}, the designated
     * timestamp its SAMPLE BY bucket or timestamp_floor() key defines, its sampling interval and the symbol
     * capacities it takes from the base table, without retaining either input.
     */
    void validateAndUpdateMetadataFromPlan(SqlExecutionContext sqlExecutionContext, @Transient LogicalPlan root, @Transient IntList positions) throws SqlException;

    void validateAndUpdateMetadataFromSelect(RecordMetadata selectMetadata, TableReaderMetadata baseTableMetadata, int scanDirection) throws SqlException;

    /**
     * The seconds in {@code value} {@code unit}; a unit other than 's', 'm', 'h' and 'd' fails at {@code pos}, naming
     * the {@code kind} of the interval.
     */
    private static long periodSeconds(int value, char unit, String kind, int pos) throws SqlException {
        return switch (unit) {
            case 's' -> value;
            case 'm' -> value * 60L;
            case 'h' -> value * 60L * 60;
            case 'd' -> value * 24L * 60 * 60;
            default ->
                    throw SqlException.position(pos).put("unsupported ").put(kind).put(" unit: ").put(value).put(unit)
                            .put(", supported units are 's', 'm', 'h', 'd'");
        };
    }

    /**
     * The rules of the PERIOD time zone {@code timeZone}, null for none; an unknown time zone fails at {@code pos}.
     */
    private static TimeZoneRules periodTimeZoneRules(@Nullable CharSequence timeZone, int pos) throws SqlException {
        if (timeZone == null) {
            return null;
        }
        try {
            return MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timeZone);
        } catch (CairoException e) {
            throw SqlException.position(pos).put(e.getFlyweightMessage());
        }
    }

    /**
     * The start of a PERIOD refresh timer: the start of the current period of {@code length} {@code lengthUnit}, which
     * {@link #validateMatViewPeriodLength} validated, in the time zone {@code rules}, or in UTC for null, in
     * microseconds.
     */
    private static long periodTimerStartUs(MicrosecondClock clock, int length, char lengthUnit, @Nullable TimeZoneRules rules, int pos) throws SqlException {
        final long nowMicros = clock.getTicks();
        return TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, length, lengthUnit, pos)
                .round(rules != null ? nowMicros + rules.getOffset(nowMicros) : nowMicros);
    }

    private static void validateMatViewPeriodDelay(int length, char lengthUnit, int delay, char delayUnit, int pos) throws SqlException {
        if (delay < 0) {
            throw SqlException.position(pos).put("delay cannot be negative");
        }
        final long lengthSeconds = periodSeconds(length, lengthUnit, "length", pos);
        if (periodSeconds(delay, delayUnit, "delay", pos) >= lengthSeconds) {
            throw SqlException.position(pos).put("delay cannot be equal to or greater than length");
        }
    }

    private static void validateMatViewPeriodLength(int length, char lengthUnit, int pos) throws SqlException {
        if (periodSeconds(length, lengthUnit, "length", pos) > 24 * 60 * 60) {
            throw SqlException.position(pos).put("maximum supported length interval is 24 hours: ").put(length).put(lengthUnit);
        }
    }

    /**
     * Fails at {@code pos} unless the offset of the SAMPLE BY a PERIOD (SAMPLE BY INTERVAL) aligns to, {@code offset},
     * is none or a valid offset of zero.
     */
    private static void validatePeriodSampleByOffset(@Nullable CharSequence offset, int pos) throws SqlException {
        if (offset == null) {
            return;
        }
        final long value = Dates.parseOffset(offset);
        if (value == Numbers.LONG_NULL) {
            throw SqlException.position(pos).put("invalid offset: ").put(offset);
        }
        if (Numbers.decodeLowInt(value) != 0) {
            throw SqlException.position(pos).put("PERIOD (SAMPLE BY INTERVAL) can't be used with WITH OFFSET");
        }
    }
}
