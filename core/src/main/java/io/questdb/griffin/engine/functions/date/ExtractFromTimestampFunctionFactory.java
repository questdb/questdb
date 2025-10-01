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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class ExtractFromTimestampFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "extract(sN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final CharSequence part = args.getQuick(0).getStrA(null);
        final Function arg = args.getQuick(1);
        final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));

        if (SqlKeywords.isCenturyKeyword(part)) {
            return new CenturyFunction(arg, driver);
        }

        if (SqlKeywords.isDayKeyword(part)) {
            return new DayOfMonthFunctionFactory.DayOfMonthFunction(arg, driver);
        }

        if (SqlKeywords.isDecadeKeyword(part)) {
            return new DecadeFunction(arg, driver);
        }

        if (SqlKeywords.isDowKeyword(part)) {
            return new DowFunction(arg, driver);
        }

        if (SqlKeywords.isDoyKeyword(part)) {
            return new DoyFunction(arg, driver);
        }

        if (SqlKeywords.isEpochKeyword(part)) {
            return new EpochFunction(arg, driver);
        }

        if (SqlKeywords.isHourKeyword(part)) {
            return new HourOfDayFunctionFactory.HourOfDayFunction(arg, driver);
        }

        if (SqlKeywords.isIsoDowKeyword(part)) {
            return new IsoDowFunction(arg, driver);
        }

        if (SqlKeywords.isIsoYearKeyword(part)) {
            return new IsoYearFunction(arg, driver);
        }

        if (SqlKeywords.isMicrosecondsKeyword(part)) {
            return new MicrosecondsFunction(arg, driver);
        }

        if (SqlKeywords.isMillenniumKeyword(part)) {
            return new MillenniumFunction(arg, driver);
        }

        if (SqlKeywords.isMillisecondsKeyword(part)) {
            return new MillisecondsFunction(arg, driver);
        }

        if (SqlKeywords.isMinuteKeyword(part)) {
            return new MinuteOfHourFunctionFactory.MinuteFunction(arg, driver);
        }

        if (SqlKeywords.isMonthKeyword(part)) {
            return new MonthOfYearFunctionFactory.MonthOfYearFunction(arg, driver);
        }

        if (SqlKeywords.isQuarterKeyword(part)) {
            return new QuarterFunction(arg, driver);
        }

        if (SqlKeywords.isSecondKeyword(part)) {
            return new SecondOfMinuteFunctionFactory.SecondOfMinuteFunc(arg, driver);
        }

        if (SqlKeywords.isWeekKeyword(part)) {
            return new WeekFunction(arg, driver);
        }

        if (SqlKeywords.isYearKeyword(part)) {
            return new YearFunctionFactory.YearFunction(arg, driver);
        }

        throw SqlException.position(argPositions.getQuick(0)).put("unsupported part '").put(part).put('\'');
    }

    static final class CenturyFunction extends IntExtractFunction {
        public CenturyFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getCentury(value);
        }
    }

    static final class DecadeFunction extends IntExtractFunction {
        public DecadeFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getDecade(value);
        }
    }

    static final class DowFunction extends IntExtractFunction {
        public DowFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getDow(value);
        }
    }

    static final class DoyFunction extends IntExtractFunction {
        public DoyFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getDoy(value);
        }
    }

    static final class EpochFunction extends LongExtractFunction {
        public EpochFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public long getLong(Record rec) {
            return driver.toSeconds(arg.getTimestamp(rec));
        }
    }

    static abstract class IntExtractFunction extends IntFunction implements UnaryFunction {
        protected final Function arg;
        protected final TimestampDriver driver;

        IntExtractFunction(Function arg, TimestampDriver driver) {
            this.arg = arg;
            this.driver = driver;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "extract";
        }
    }

    static final class IsoDowFunction extends IntExtractFunction {
        public IsoDowFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getDayOfWeek(value);
        }
    }

    static final class IsoYearFunction extends IntExtractFunction {
        public IsoYearFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getIsoYear(value);
        }
    }

    static abstract class LongExtractFunction extends LongFunction implements UnaryFunction {
        protected final Function arg;
        protected final TimestampDriver driver;

        LongExtractFunction(Function arg, TimestampDriver driver) {
            this.arg = arg;
            this.driver = driver;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "extract";
        }
    }

    static final class MicrosecondsFunction extends IntExtractFunction {
        public MicrosecondsFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getMicrosOfSecond(value);
        }
    }

    static final class MillenniumFunction extends IntExtractFunction {
        public MillenniumFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getMillennium(value);
        }
    }

    static final class MillisecondsFunction extends IntExtractFunction {
        public MillisecondsFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getMillisOfSecond(value);
        }
    }

    static final class QuarterFunction extends IntExtractFunction {
        public QuarterFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getQuarter(value);
        }
    }

    static final class WeekFunction extends IntExtractFunction {
        public WeekFunction(Function arg, TimestampDriver driver) {
            super(arg, driver);
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getWeek(value);
        }
    }
}
