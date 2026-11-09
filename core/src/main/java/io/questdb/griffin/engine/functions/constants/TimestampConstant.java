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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

public class TimestampConstant extends TimestampFunction implements ConstantFunction {
    public static final TimestampConstant TIMESTAMP_MICRO_NULL = new TimestampConstant(Numbers.LONG_NULL, ColumnType.TIMESTAMP_MICRO);
    public static final TimestampConstant TIMESTAMP_NANO_NULL = new TimestampConstant(Numbers.LONG_NULL, ColumnType.TIMESTAMP_NANO);
    // the text of a literal whose value drops some of its digits, so that comparisons can read it exactly
    private final @Nullable String sourceText;
    private final long value;

    public TimestampConstant(long value, int columnType) {
        this(value, columnType, null);
    }

    private TimestampConstant(long value, int columnType, @Nullable String sourceText) {
        super(columnType);
        this.value = value;
        this.sourceText = sourceText;
    }

    public static ConstantFunction newInstance(long value, int timestampType) {
        return value != Numbers.LONG_NULL ? new TimestampConstant(value, timestampType) : ColumnType.getTimestampDriver(timestampType).getTimestampConstantNull();
    }

    /**
     * A constant for a timestamp literal. The text is kept only when the value, of the given type, drops some of
     * its digits: past the end of TIMESTAMP_NS a literal with nanosecond digits becomes a TIMESTAMP value.
     */
    public static ConstantFunction newInstance(long value, int timestampType, CharSequence text) {
        if (value == Numbers.LONG_NULL || ColumnType.getTimestampDriver(timestampType).isExactText(text, 0, text.length())) {
            return newInstance(value, timestampType);
        }
        return new TimestampConstant(value, timestampType, Chars.toString(text));
    }

    /**
     * The literal's text when the value drops some of its digits, otherwise null and the value is exact.
     */
    public @Nullable CharSequence getSourceText() {
        return sourceText;
    }

    @Override
    public long getTimestamp(Record rec) {
        return value;
    }

    @Override
    public boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof TimestampConstant that) {
            return this.value == that.value && Chars.equalsNullable(this.sourceText, that.sourceText);
        }
        return false;
    }

    @Override
    public boolean isNullConstant() {
        return value == Numbers.LONG_NULL;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.valISODate(timestampDriver, value);
    }
}
