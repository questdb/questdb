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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.CommonUtils;
import org.jetbrains.annotations.NotNull;

public final class TimestampSamplerFactory {

    public static TimestampSampler getInstance(TimestampDriver driver, long interval, CharSequence units, int position) throws SqlException {
        if (units.length() == 1) {
            return getInstance(driver, interval, units.charAt(0), position);
        }
        // Just in case SqlParser will allow this in the future
        throw SqlException.$(position, "expected one character interval qualifier");
    }

    @NotNull
    public static TimestampSampler getInstance(TimestampDriver driver, long interval, char timeUnit, int position) throws SqlException {
        return driver.getTimestampSampler(interval, timeUnit, position);
    }

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs       the key
     * @param position position in SQL text to report error against
     * @return instance of appropriate TimestampSampler
     * @throws SqlException when input string is invalid
     */
    public static TimestampSampler getInstance(TimestampDriver driver, CharSequence cs, int position) throws SqlException {
        int k = CommonUtils.findPositiveIntervalEndIndex(cs, position, "sample");
        assert cs.length() > k;

        long n = CommonUtils.parsePositiveInterval(cs, k, position, "sample", Numbers.INT_NULL, '?');
        return getInstance(driver, n, cs.charAt(k), position + k);
    }
}
