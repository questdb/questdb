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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;
import static io.questdb.std.datetime.microtime.Timestamps.MINUTE_MICROS;

public class MatViewDefinition {
    private final String baseTableName;
    private final long fromMicros;
    private final String matViewSql;
    private final TableToken matViewToken;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final String timeZone;
    private final String timeZoneOffset;
    private final long toMicros;
    private long fixedOffset;
    private @Nullable TimeZoneRules rules;
    // non persistent fields
    private long tzOffset;

    public MatViewDefinition(
            TableToken matViewToken,
            String matViewSql,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            long fromMicros,
            long toMicros,
            String timeZone,
            String timeZoneOffset
    ) {
        this.matViewToken = matViewToken;
        this.matViewSql = matViewSql;
        this.baseTableName = baseTableName;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.fromMicros = fromMicros;
        this.toMicros = toMicros;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;

        if (timeZone != null) {
            try {
                long opt = Timestamps.parseOffset(timeZone);
                if (opt == Long.MIN_VALUE) {
                    this.rules = TimestampFormatUtils.EN_LOCALE.getZoneRules(
                            Numbers.decodeLowInt(TimestampFormatUtils.EN_LOCALE.matchZone(timeZone, 0, timeZone.length())),
                            RESOLUTION_MICROS
                    );
                } else {
                    // here timezone is in numeric offset format
                    this.tzOffset = Numbers.decodeLowInt(opt) * MINUTE_MICROS;
                }
            } catch (NumericException e) {
                throw CairoException.critical(0).put("invalid timezone: ").put(timeZone);
            }
        }

        if (timeZoneOffset != null) {
            final long val = Timestamps.parseOffset(timeZoneOffset);
            if (val == Numbers.LONG_NULL) {
                throw CairoException.critical(0).put("invalid offset: ").put(timeZoneOffset);
            }
            this.fixedOffset = Numbers.decodeLowInt(val) * MINUTE_MICROS;
        }
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public long getFromMicros() {
        return fromMicros;
    }

    public String getMatViewSql() {
        return matViewSql;
    }

    public TableToken getMatViewToken() {
        return matViewToken;
    }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public char getSamplingIntervalUnit() {
        return samplingIntervalUnit;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public long getToMicros() {
        return toMicros;
    }

    public long getFixedOffset() {
        return fixedOffset;
    }

    public @Nullable TimeZoneRules getTzRules() {
        return rules;
    }
}
