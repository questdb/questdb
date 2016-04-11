/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.printer;

import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.printer.appender.Appender;
import com.nfsdb.printer.converter.DateConverter;
import com.nfsdb.printer.converter.ScaledDoubleConverter;
import com.nfsdb.printer.converter.StripCRLFStringConverter;
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
