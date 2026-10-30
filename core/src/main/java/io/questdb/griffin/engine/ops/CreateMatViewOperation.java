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

    static int matViewPeriodDelaySeconds(int delay, char delayUnit, int pos) throws SqlException {
        return switch (delayUnit) {
            case 's' -> delay;
            case 'm' -> delay * 60;
            case 'h' -> delay * 60 * 60;
            case 'd' -> delay * 24 * 60 * 60;
            default -> throw SqlException.position(pos).put("unsupported delay unit: ").put(delay).put(delayUnit)
                    .put(", supported units are 's', 'm', 'h', 'd'");
        };
    }

    static int matViewPeriodLengthSeconds(int length, char lengthUnit, int pos) throws SqlException {
        return switch (lengthUnit) {
            case 's' -> length;
            case 'm' -> length * 60;
            case 'h' -> length * 60 * 60;
            case 'd' -> length * 24 * 60 * 60;
            default -> throw SqlException.position(pos).put("unsupported length unit: ").put(length).put(lengthUnit)
                    .put(", supported units are 's', 'm', 'h', 'd'");
        };
    }

    /**
     * The start of a PERIOD refresh timer: the start of the current period of {@code length} {@code lengthUnit}, which
     * {@link #validateMatViewPeriodLength} validated, in the time zone {@code rules}, or in UTC for null, in
     * microseconds.
     */
    static long periodTimerStartUs(MicrosecondClock clock, int length, char lengthUnit, @Nullable TimeZoneRules rules, int pos) throws SqlException {
        final long nowMicros = clock.getTicks();
        return TimestampSamplerFactory.getInstance(MicrosTimestampDriver.INSTANCE, length, lengthUnit, pos)
                .round(rules != null ? nowMicros + rules.getOffset(nowMicros) : nowMicros);
    }

    /**
     * The rules of the PERIOD time zone {@code timeZone}, null for none; an unknown time zone fails at {@code pos}.
     */
    static TimeZoneRules periodTimeZoneRules(@Nullable CharSequence timeZone, int pos) throws SqlException {
        if (timeZone == null) {
            return null;
        }
        try {
            return MicrosTimestampDriver.INSTANCE.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timeZone);
        } catch (CairoException e) {
            throw SqlException.position(pos).put(e.getFlyweightMessage());
        }
    }

    static void validateMatViewPeriodLength(int length, char lengthUnit, int pos) throws SqlException {
        final int lengthSeconds = matViewPeriodLengthSeconds(length, lengthUnit, pos);
        if (lengthSeconds > 24 * 60 * 60) {
            throw SqlException.position(pos).put("maximum supported length interval is 24 hours: ").put(length).put(lengthUnit);
        }
    }

    /**
     * Fails at {@code pos} unless the offset of the SAMPLE BY a PERIOD (SAMPLE BY INTERVAL) aligns to, {@code offset},
     * is none or a valid offset of zero.
     */
    static void validatePeriodSampleByOffset(@Nullable CharSequence offset, int pos) throws SqlException {
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

    CharSequence getBaseTableName();

    CreateTableOperation getCreateTableOperation();

    int getRefreshType();

    CharSequence getSqlText();

    int getTableNamePosition();

    CharSequence getVolumeAlias();

    int getVolumePosition();

    boolean ignoreIfExists();

    boolean isDeferred();

    void updateOperationFutureTableToken(TableToken tableToken);

    /**
     * Validates the bound query of the view and records its columns, named at {@code positions}, the designated
     * timestamp its SAMPLE BY bucket or timestamp_floor() key defines, its sampling interval and the symbol
     * capacities it takes from the base table, without retaining either input.
     */
    void validateAndUpdateMetadataFromPlan(SqlExecutionContext sqlExecutionContext, @Transient LogicalPlan root, @Transient IntList positions) throws SqlException;

    void validateAndUpdateMetadataFromSelect(RecordMetadata selectMetadata, TableReaderMetadata baseTableMetadata, int scanDirection) throws SqlException;
}
