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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * The intervals an interval IN searches, {@code key IN '<interval>'}, parsed once from the constant text at the key's
 * precision: the intervals themselves, or the compiled tick expression of a text with a date variable, which yields
 * them for each "now". The binder parses the text, and the Java filter and the JIT both read the result, so they
 * search the same intervals. Every function built from the binding shares the result and reads it without changing
 * it.
 */
public final class IntervalSearch {
    private final CompiledTickExpression compiledExpr;
    private final LongList intervals;
    private final int timestampType;

    private IntervalSearch(int timestampType, LongList intervals, CompiledTickExpression compiledExpr) {
        this.timestampType = timestampType;
        this.intervals = intervals;
        this.compiledExpr = compiledExpr;
    }

    /**
     * Parses the text of an interval IN over a key of {@code timestampType}: NULL text searches NULL. Text in its
     * quoted spelling, as interval extraction parses a string literal, reads a bare number as an invalid interval;
     * other text reads it as an epoch, as InTimestampTimestampFunctionFactory always did. Returns null when the text
     * holds no instant at that precision, e.g. '2024-01-06#workday', where the day filter drops the only day, a
     * Saturday: no row matches, NULL included.
     *
     * @throws SqlException at {@code position} for text that spells no interval
     */
    public static @Nullable IntervalSearch parse(
            int timestampType,
            CairoConfiguration configuration,
            @Nullable CharSequence text,
            boolean isQuotedSpelling,
            int position
    ) throws SqlException {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        if (text != null && DateExpressionEvaluator.containsDateVariable(text, 0, text.length())) {
            final CompiledTickExpression compiledExpr = IntervalUtils.compileTickExpr(driver, configuration, text, 0, text.length(), position);
            return new IntervalSearch(timestampType, null, compiledExpr);
        }
        final LongList intervals = new LongList();
        final StringSink sink = new StringSink();
        if (text != null && isQuotedSpelling) {
            final StringSink quoted = new StringSink();
            quoted.put('\'').put(text).put('\'');
            IntervalUtils.parseTickExpr(driver, configuration, quoted, 1, quoted.length() - 1, position, intervals, sink);
        } else {
            IntervalUtils.parseTickExprAndIntersect(driver, configuration, text, intervals, position, sink);
        }
        return intervals.size() > 0 ? new IntervalSearch(timestampType, intervals, null) : null;
    }

    /**
     * The compiled tick expression of a text with a date variable, or null for static intervals. Evaluate it with
     * {@link CompiledTickExpression#evaluate(LongList, long)}, which keeps no state.
     */
    public @Nullable CompiledTickExpression getCompiledExpr() {
        return compiledExpr;
    }

    /**
     * The sorted [lo, hi] pairs of a text without date variables, at least one, or null for a text with a date
     * variable. Callers must not modify the list.
     */
    public @Nullable LongList getIntervals() {
        return intervals;
    }

    /**
     * The precision of the intervals: the timestamp type of the key, TIMESTAMP for a DATE key.
     */
    public int getTimestampType() {
        return timestampType;
    }
}
