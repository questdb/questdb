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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

/**
 * Turns the constant or runtime-constant operand of a timestamp comparison into bounds in the type of the values
 * it is compared with, by {@link TimestampDriver#lowerBound} and {@link TimestampDriver#upperBound}. The
 * comparison then tests each value against the bounds without converting it. The exact value of the operand is
 * its text when it is timestamp text, or a literal whose value drops some of its digits, otherwise its timestamp
 * value. Other text, such as epoch digits or the PostgreSQL format, converts through the operand's own value, which
 * fails for text that is not a timestamp. Callers read the operand only after {@link #isNull(Function)} returned
 * false.
 */
public final class TimestampBounds {
    // lo > hi: a range that holds no value, NULL included
    public static final long EMPTY_HI = Long.MIN_VALUE;
    public static final long EMPTY_LO = Long.MAX_VALUE;

    private TimestampBounds() {
    }

    /**
     * Reads the operand once and puts {@link TimestampDriver#lowerBound} and {@link TimestampDriver#upperBound}
     * of it, not strict, into out.
     *
     * @return false, with out empty, when the operand is NULL
     */
    public static boolean bounds(TimestampDriver driver, Function operand, LongList out) {
        out.clear();
        final CharSequence text = textOf(operand);
        if (text != null) {
            try {
                out.add(driver.lowerBound(text, 0, text.length(), false), driver.upperBound(text, 0, text.length(), false));
                return true;
            } catch (NumericException ignore) {
                // not timestamp text: the operand's own conversion reads it
            }
        }
        final long timestamp = operand.getTimestamp(null);
        if (timestamp == Numbers.LONG_NULL) {
            return false;
        }
        final int timestampType = ColumnType.getTimestampType(operand.getType());
        out.add(driver.lowerBound(timestamp, timestampType, false), driver.upperBound(timestamp, timestampType, false));
        return true;
    }

    public static boolean isNull(Function operand) {
        if (operand instanceof TimestampConstant constant && constant.getSourceText() != null) {
            return false;
        }
        return switch (ColumnType.tagOf(operand.getType())) {
            case ColumnType.STRING, ColumnType.SYMBOL -> operand.getStrA(null) == null;
            case ColumnType.VARCHAR -> operand.getVarcharA(null) == null;
            default -> operand.getTimestamp(null) == Numbers.LONG_NULL;
        };
    }

    /**
     * Whether timestamp text that the driver parsed has no digit finer than the driver's type.
     */
    public static boolean isExactText(TimestampDriver driver, @Nullable CharSequence text) {
        return text == null || driver.isExactText(text, 0, text.length());
    }

    public static boolean isExactText(TimestampDriver driver, @Nullable Utf8Sequence text) {
        return text == null || !Utf8s.isAscii(text) || driver.isExactText(text.asAsciiCharSequence(), 0, text.size());
    }

    /**
     * {@link TimestampDriver#lowerBound} for the operand.
     */
    public static long lower(TimestampDriver driver, Function operand, boolean isStrict) {
        final CharSequence text = textOf(operand);
        if (text != null) {
            try {
                return driver.lowerBound(text, 0, text.length(), isStrict);
            } catch (NumericException ignore) {
                // not timestamp text: the operand's own conversion reads it
            }
        }
        return driver.lowerBound(operand.getTimestamp(null), ColumnType.getTimestampType(operand.getType()), isStrict);
    }

    /**
     * The one value of the driver's type that equals the operand, or {@link Numbers#LONG_NULL} when none does:
     * the operand is NULL, lies past the range of the type, or lies between two of its values. It reads the
     * operand once.
     */
    public static long point(TimestampDriver driver, Function operand) {
        final CharSequence text = textOf(operand);
        if (text != null) {
            try {
                final long lo = driver.lowerBound(text, 0, text.length(), false);
                return lo != Numbers.LONG_NULL && lo == driver.upperBound(text, 0, text.length(), false) ? lo : Numbers.LONG_NULL;
            } catch (NumericException ignore) {
                // not timestamp text: the operand's own conversion reads it
            }
        }
        final long timestamp = operand.getTimestamp(null);
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        final int timestampType = ColumnType.getTimestampType(operand.getType());
        final long lo = driver.lowerBound(timestamp, timestampType, false);
        return lo != Numbers.LONG_NULL && lo == driver.upperBound(timestamp, timestampType, false) ? lo : Numbers.LONG_NULL;
    }

    /**
     * The operand's text, or null when its timestamp value is exact.
     */
    public static @Nullable CharSequence textOf(Function operand) {
        if (operand instanceof TimestampConstant constant) {
            return constant.getSourceText();
        }
        switch (ColumnType.tagOf(operand.getType())) {
            case ColumnType.STRING, ColumnType.SYMBOL -> {
                return operand.getStrA(null);
            }
            case ColumnType.VARCHAR -> {
                // non-ASCII text, such as a localized time zone name, converts through the operand's value
                final Utf8Sequence text = operand.getVarcharA(null);
                return text != null && Utf8s.isAscii(text) ? text.asAsciiCharSequence() : null;
            }
            default -> {
                return null;
            }
        }
    }

    /**
     * {@link TimestampDriver#upperBound} for the operand.
     */
    public static long upper(TimestampDriver driver, Function operand, boolean isStrict) {
        final CharSequence text = textOf(operand);
        if (text != null) {
            try {
                return driver.upperBound(text, 0, text.length(), isStrict);
            } catch (NumericException ignore) {
                // not timestamp text: the operand's own conversion reads it
            }
        }
        return driver.upperBound(operand.getTimestamp(null), ColumnType.getTimestampType(operand.getType()), isStrict);
    }

}
