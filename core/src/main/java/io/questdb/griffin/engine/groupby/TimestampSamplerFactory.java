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
        final long stride = parsePeriodStride(cs, position);
        final int unitIndex = periodUnitIndex(cs);
        return getSampleByInstance(driver, stride, cs.charAt(unitIndex), position + unitIndex);
    }

    /**
     * The sampler of a SAMPLE BY stride, which must be positive, whether a literal or a constant expression spells it,
     * and must fit the interval the unit's sampler counts in, see {@link TimestampDriver#intSamplerInterval}. A stride
     * that does not fails at {@code position} with the error a literal stride raises: a negative one, zero, or an
     * invalid value for NULL or one too large. A literal stride is an INT by its syntax, while a constant expression
     * may be a LONG.
     *
     * @param stride   the stride; {@link Numbers#LONG_NULL} for NULL
     * @param position position in SQL text to report error against
     */
    public static TimestampSampler getSampleByInstance(TimestampDriver driver, long stride, char timeUnit, int position) throws SqlException {
        if (stride == 0) {
            throw SqlException.$(position, "zero is not a valid sample value");
        }
        if (stride < 0 && stride != Numbers.LONG_NULL) {
            throw SqlException.$(position, "negative interval is not allowed");
        }
        if (stride == Numbers.LONG_NULL) {
            throw SqlException.$(position, "invalid sample value [value=null]");
        }
        return getInstance(driver, stride, timeUnit, position);
    }

    /**
     * The stride of the SAMPLE BY period literal {@code cs}, such as 15 for {@code 15m} or 1 for {@code m}: an optional
     * positive INT followed by the one-letter unit that ends the literal, at {@link #periodUnitIndex}. The stride
     * must also fit the timestamp range in that unit, which {@link #getSampleByInstance} checks.
     *
     * @param position position of the literal in SQL text to report error against
     */
    public static long parsePeriodStride(CharSequence cs, int position) throws SqlException {
        final int unitIndex = CommonUtils.findPositiveIntervalEndIndex(cs, position, "sample");
        return CommonUtils.parsePositiveInterval(cs, unitIndex, position, "sample", Numbers.INT_NULL, '?');
    }

    /**
     * The index of the one-letter unit that ends the SAMPLE BY period literal {@code cs}; -1 for an empty literal.
     */
    public static int periodUnitIndex(CharSequence cs) {
        return cs.length() - 1;
    }

}
