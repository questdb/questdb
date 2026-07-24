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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.date.VarcharToDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.VarcharToNanoTimestampVCFunctionFactory;
import io.questdb.griffin.engine.functions.date.VarcharToTimestampVCFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRuleFactory;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.datetime.nanotime.NanosFormatCompiler;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormatSymbols;
import java.util.Locale;

public class VarcharDateFunctionUtilsTest extends AbstractFunctionFactoryTest {
    private static final String NON_ASCII_TEXT = "ø";

    @Test
    public void testIsAsciiOnlyPattern() throws SqlException {
        assertAsciiOnly("G", false);
        assertAsciiOnly("M", true);
        assertAsciiOnly("MM", true);
        assertAsciiOnly("MMM", false);
        assertAsciiOnly("MMMM", false);
        assertAsciiOnly("E", false);
        assertAsciiOnly("EE", false);
        assertAsciiOnly("a", false);
        assertAsciiOnly("z", false);
        assertAsciiOnly("zz", false);
        assertAsciiOnly("zzz", false);
        assertAsciiOnly("Z", false);
        assertAsciiOnly("x", false);
        assertAsciiOnly("xx", false);
        assertAsciiOnly("xxx", false);
        assertAsciiOnly("yyyy年MM月dd日", false);
        assertAsciiOnly("yyyy-MM-dd HH:mm:ss.SSS", true);
        assertAsciiOnly("MM-dd-MM", true);
    }

    @Test
    public void testLocaleBackedCompilerOpsUseUtf8Parser() throws SqlException {
        final DateLocale locale = createNonAsciiLocale();
        assertLocaleBackedOps(
                new DateFormatCompiler()::compile,
                DateFormatCompiler::getOpName,
                DateFormatCompiler.getOpCount(),
                new VarcharToDateFunctionFactory(),
                locale,
                "date"
        );
        assertLocaleBackedOps(
                new MicrosFormatCompiler()::compile,
                MicrosFormatCompiler::getOpName,
                MicrosFormatCompiler.getOpCount(),
                new VarcharToTimestampVCFunctionFactory(),
                locale,
                "microsecond timestamp"
        );
        assertLocaleBackedOps(
                new NanosFormatCompiler()::compile,
                NanosFormatCompiler::getOpName,
                NanosFormatCompiler.getOpCount(),
                new VarcharToNanoTimestampVCFunctionFactory(),
                locale,
                "nanosecond timestamp"
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new VarcharToDateFunctionFactory();
    }

    private void assertAsciiOnly(String pattern, boolean expected) throws SqlException {
        assertAsciiOnly(getFunctionFactory(), pattern, expected);
    }

    private void assertAsciiOnly(FunctionFactory functionFactory, String pattern, boolean expected) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(new VarcharColumn(0));
        args.add(new StrConstant(pattern));

        final IntList argPositions = new IntList();
        argPositions.add(0);
        argPositions.add(0);

        try (Function function = functionFactory.newInstance(
                0,
                args,
                argPositions,
                configuration,
                sqlExecutionContext
        )) {
            final String className = function.getClass().getName();
            final boolean isAsciiOnly = className.endsWith("$ToAsciiDateFunction")
                    || className.endsWith("$ToAsciiTimestampFunc");
            Assert.assertEquals(pattern, expected, isAsciiOnly);
        }
    }

    private void assertLocaleBackedOps(
            FormatCompiler compiler,
            OpNameProvider opNameProvider,
            int opCount,
            FunctionFactory functionFactory,
            DateLocale locale,
            String compilerName
    ) throws SqlException {
        int localeBackedOpCount = 0;
        final StringSink sink = new StringSink();
        assertAsciiOnly(functionFactory, "yyyy-MM-dd", true);
        for (int i = 0; i < opCount; i++) {
            final String pattern = opNameProvider.getOpName(i);
            sink.clear();
            compiler.compile(pattern).format(0, locale, NON_ASCII_TEXT, sink);
            if (!Chars.isAscii(sink)) {
                localeBackedOpCount++;
                assertAsciiOnly(functionFactory, pattern, false);
            }
        }
        Assert.assertTrue("no locale-backed operations found for " + compilerName, localeBackedOpCount > 0);
    }

    private static DateLocale createNonAsciiLocale() {
        final DateFormatSymbols symbols = new DateFormatSymbols(Locale.ENGLISH);
        symbols.setAmPmStrings(newNonAsciiStrings(2));
        symbols.setEras(newNonAsciiStrings(2));
        symbols.setMonths(newNonAsciiStrings(13));
        symbols.setShortMonths(newNonAsciiStrings(13));
        symbols.setShortWeekdays(newNonAsciiStrings(8));
        symbols.setWeekdays(newNonAsciiStrings(8));
        symbols.setZoneStrings(new String[][]{
                {"UTC", NON_ASCII_TEXT, NON_ASCII_TEXT, NON_ASCII_TEXT, NON_ASCII_TEXT}
        });
        return new DateLocale("non-ascii", symbols, TimeZoneRuleFactory.INSTANCE);
    }

    private static String[] newNonAsciiStrings(int count) {
        final String[] values = new String[count];
        for (int i = 0; i < count; i++) {
            values[i] = NON_ASCII_TEXT;
        }
        return values;
    }

    @FunctionalInterface
    private interface FormatCompiler {
        DateFormat compile(CharSequence pattern);
    }

    @FunctionalInterface
    private interface OpNameProvider {
        String getOpName(int index);
    }
}
