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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;


public class ToStrDateAndTimestampTest extends AbstractCairoTest {
    @Test
    public void testToStrBehavior() {
        String[] inputs = {
                "2020-01-01T17:16:30.192Z",
                "2019-03-10T07:16:30.192Z",
                "2020-03-10T07:16:30.192Z",
                "1893-03-19T17:16:30.192Z",
                "2020-12-31T12:00:00.000Z",
                "2021-12-31T12:00:00.000Z",
                "-2021-12-31T12:00:00.000Z"
        };

        String[] opList = {
                "G",
                "y",
                "yy",
                "yyy",
                "yyyy",
                // "YYYY", only used in PartitionBy as it relates to ISO Weeks
                "M",
                "MM",
                "MMM",
                "MMMM",
                "d",
                "dd",
                "E",
                "EE",
                "u",
                "D",
                "w",
                // "ww", only used in PartitionBy as it relates to ISO Weeks
                "a",
                "H",
                "HH",
                "k",
                "kk",
                "K",
                "KK",
                "h",
                "hh",
                "m",
                "mm",
                "s",
                "ss",
                "S",
                "SSS",
                // "N", these don't make sense for millis
                // "NNN",
                "z",
                "zz",
                "zzz",
                "Z",
                "x",
                "xx",
                "xxx",
                // "U", these also don't make sense for millis
                // "UUU"
        };
        MicrosFormatCompiler tsc = new MicrosFormatCompiler();
        DateFormatCompiler dsc = new DateFormatCompiler();
        DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getLocale("en-GB");
        for (String op : opList) {
            DateFormat fmt = tsc.compile(op, true);
            DateFormat fmt2 = dsc.compile(op, true);
            for (String s : inputs) {
                long micros = MicrosFormatUtils.parseTimestamp(s);
                long millis = DateFormatUtils.parseUTCDate(s);
                assert micros == 1e3 * millis;

                StringSink ss = new StringSink();
                fmt.format(micros, defaultLocale, "GMT", ss);
                StringSink ss2 = new StringSink();
                fmt2.format(millis, defaultLocale, "GMT", ss2);
                TestUtils.assertEqualsIgnoreCase(ss, ss2);
            }
        }
    }
}
