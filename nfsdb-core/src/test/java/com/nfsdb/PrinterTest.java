/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.printer.JournalPrinter;
import com.nfsdb.printer.appender.Appender;
import com.nfsdb.printer.converter.DateConverter;
import com.nfsdb.printer.converter.ScaledDoubleConverter;
import com.nfsdb.printer.converter.StripCRLFStringConverter;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PrinterTest {

    private final TestAppender testAppender = new TestAppender();

    @Test
    public void testCRLFStripping() throws Exception {
        try (JournalPrinter printer = new JournalPrinter()) {
            printer.setAppender(testAppender).setNullString("")
                    .types(String.class, TestEntity.class)
                    .v(0).c(new StripCRLFStringConverter(printer)).h("Test String")
                    .f("bStr").c(new StripCRLFStringConverter(printer)).h("destination");

            printer.out("test string", new TestEntity().setBStr("ok\nbunny"));
            printer.out("test\nstring2\r_good", new TestEntity().setBStr("ok foxy"));

            testAppender.assertLine("test string\tok bunny", 0);
            testAppender.assertLine("test string2_good\tok foxy", 1);
        }
    }

    @Test
    public void testDateConversion() throws Exception {

        long millis = Dates.parseDateTime("2013-10-11T10:00:00.000Z");

        Quote position1 = new Quote().setBidSize(10).setTimestamp(millis);
        Quote position2 = new Quote().setTimestamp(millis);

        try (JournalPrinter printer = new JournalPrinter()) {
            printer.types(Quote.class, int.class);
            printer.setAppender(testAppender);
            printer
                    .f("timestamp").i(0).h("TimeStamp").c(new DateConverter(printer))
                    .f("bidSize")
                    .f("askSize")
                    .v(1).h("test");

            printer.header();
            printer.out(position1, 0);
            printer.out(position2, 1);
            testAppender.assertLine("TimeStamp\tbidSize\taskSize\ttest", 0);
            testAppender.assertLine("2013-10-11T10:00:00.000Z\t10\t0\t0", 1);
            testAppender.assertLine("2013-10-11T10:00:00.000Z\t0\t0\t1", 2);
        }
    }

    @Test
    public void testDoublePrinting() {
        for (String s : "-0.000001,0.000009,-0.000010,0.100000,1.100000,10.100000".split(",")) {
            double d = Double.parseDouble(s);
            StringBuilder sb = new StringBuilder();
            ScaledDoubleConverter.appendTo(sb, d, 6);
            Assert.assertEquals(s, sb.toString());
        }
    }

    private static class TestAppender implements Appender {

        private final List<String> output = new ArrayList<>();

        @Override
        public void append(StringBuilder stringBuilder) {
            output.add(stringBuilder.toString());
        }

        @Override
        public void close() {
            output.clear();
        }

        public void assertLine(String expected, int line) {
            Assert.assertTrue("Appender receiver too few lines: " + line + " >= " + output.size(), line < output.size());
            Assert.assertEquals(expected, output.get(line));
        }
    }
}
