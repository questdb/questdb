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

import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

public class DateConstant extends DateFunction implements ConstantFunction {
    public static final DateConstant NULL = new DateConstant(Numbers.LONG_NULL);
    // the text of a literal whose value drops some of its digits, so that comparisons can read it exactly
    private final @Nullable String sourceText;
    private final long value;

    public DateConstant(long value) {
        this(value, null);
    }

    private DateConstant(long value, @Nullable String sourceText) {
        this.value = value;
        this.sourceText = sourceText;
    }

    public static DateConstant newInstance(long value) {
        return value != Numbers.LONG_NULL ? new DateConstant(value) : NULL;
    }

    /**
     * A constant for a date literal. The text is kept only when the value drops some of its digits, those finer
     * than a millisecond.
     */
    public static DateConstant newInstance(long value, CharSequence text) {
        if (value == Numbers.LONG_NULL || MillisTimestampDriver.INSTANCE.isExactText(text, 0, text.length())) {
            return newInstance(value);
        }
        return new DateConstant(value, Chars.toString(text));
    }

    @Override
    public long getDate(Record rec) {
        return value;
    }

    /**
     * The literal's text when the value drops some of its digits, otherwise null and the value is exact.
     */
    public @Nullable CharSequence getSourceText() {
        return sourceText;
    }

    @Override
    public boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof DateConstant that) {
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
        sink.val(value);
    }
}
