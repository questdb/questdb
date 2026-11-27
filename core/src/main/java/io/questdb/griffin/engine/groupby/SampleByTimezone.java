/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;

/**
 * The TIME ZONE and OFFSET clauses of a SAMPLE BY, resolved per execution: a zone name to its rules, a numeric
 * zone to its fixed offset, and OFFSET to its value or Long.MIN_VALUE when absent.
 */
final class SampleByTimezone {
    private long fixedOffset;
    private TimeZoneRules rules;
    private long tzOffset;

    long getFixedOffset() {
        return fixedOffset;
    }

    TimeZoneRules getRules() {
        return rules;
    }

    long getTzOffset() {
        return tzOffset;
    }

    void of(
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            TimestampDriver timestampDriver,
            SymbolTableSource base,
            SqlExecutionContext executionContext
    ) throws SqlException {
        timezoneNameFunc.init(base, executionContext);
        offsetFunc.init(base, executionContext);
        rules = null;
        tzOffset = 0;

        final CharSequence tz = timezoneNameFunc.getStrA(null);
        if (tz != null) {
            try {
                long opt = Dates.parseOffset(tz);
                if (opt == Long.MIN_VALUE) {
                    // this is timezone name
                    // fixed rules means the timezone does not have historical or daylight time changes
                    rules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                            Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, tz.length())),
                            timestampDriver.getTZRuleResolution()
                    );
                } else {
                    // here timezone is in numeric offset format
                    tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(opt));
                }
            } catch (NumericException e) {
                throw SqlException.$(timezoneNameFuncPos, "invalid timezone: ").put(tz);
            }
        }

        final CharSequence offset = offsetFunc.getStrA(null);
        if (offset != null) {
            final long val = Dates.parseOffset(offset);
            if (val == Numbers.LONG_NULL) {
                // bad value for offset
                throw SqlException.$(offsetFuncPos, "invalid offset: ").put(offset);
            }
            fixedOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
        } else {
            fixedOffset = Long.MIN_VALUE;
        }
    }
}
