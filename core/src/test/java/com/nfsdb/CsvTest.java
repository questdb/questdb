/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.io.ImportManager;
import com.nfsdb.io.parser.CsvParser;
import com.nfsdb.io.parser.TabParser;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CsvTest {

    @Test
    public void testHeaders() throws Exception {
        final List<String> names = new ArrayList<>();
        final List<String> expected = new ArrayList<String>() {{
            add("type");
            add("value");
            add("active");
            add("desc");
            add("grp");
        }};


        ImportManager.parse(new File(this.getClass().getResource("/csv/test-headers.csv").getFile())
                , new CsvParser()
                , 1024 * 1024
                , true
                , new Listener() {
                    @Override
                    public void onError(int line) {

                    }

                    @Override
                    public void onFieldCount(int count) {

                    }

                    @Override
                    public void onFields(int line, CharSequence[] values, int hi) {

                    }

                    @Override
                    public void onHeader(CharSequence[] values, int hi) {
                        for (int i = 0; i < hi; i++) {
                            names.add(values[i].toString());
                        }
                    }

                    @Override
                    public void onLineCount(int count) {

                    }
                }
        );

        TestUtils.assertEquals(expected.iterator(), names.iterator());

    }

    @Test
    public void testParseDosCsvSmallBuf() throws Exception {
        assertFile("/csv/test-dos.csv", 10, new CsvParser());
    }

    @Test
    public void testParseTab() throws Exception {
        assertFile("/csv/test.txt", 1024 * 1024, new TabParser());
    }

    @Test
    public void testParseTabSmallBuf() throws Exception {
        assertFile("/csv/test.txt", 10, new TabParser());
    }

    @Test
    public void testParseUnixCsvSmallBuf() throws Exception {
        assertFile("/csv/test-unix.csv", 10, new CsvParser());
    }

/*
    @Test
    public void testTypeDetection() throws Exception {
        final List<ColumnMetadata> meta = new ArrayList<>();
        final List<ColumnMetadata> expected = new ArrayList<ColumnMetadata>() {{
            ColumnMetadata m;

            m = new ColumnMetadata();
            m.name = "type";
            m.type = ColumnType.STRING;
            add(m);

            m = new ColumnMetadata();
            m.name = "value";
            m.type = ColumnType.DOUBLE;
            add(m);


            m = new ColumnMetadata();
            m.name = "active";
            m.type = ColumnType.BOOLEAN;
            add(m);


            m = new ColumnMetadata();
            m.name = "desc";
            m.type = ColumnType.STRING;
            add(m);

            m = new ColumnMetadata();
            m.name = "grp";
            m.type = ColumnType.STRING;
            add(m);

        }};

        final AtomicInteger fieldCount = new AtomicInteger();
        final AtomicInteger lineCount = new AtomicInteger();


        Csv csv = new Csv();
        csv.analyzeAndParse(
                new File(this.getClass().getResource("/csv/test-headers.csv").getFile())
                , 1024 * 1024
                , new Csv.Listener() {
                    @Override
                    public void onError(int line) {

                    }

                    @Override
                    public void onFields(int index, CharSequence value, int line, boolean eol) {
                    }

                    @Override
                    public void onFieldCount(int count) {
                        fieldCount.set(count);
                    }

                    @Override
                    public void onLineCount(int count) {
                        lineCount.set(count);
                    }

                    @Override
                    public void onNames(List<ColumnMetadata> m) {
                        meta.addAll(m);
                    }
                }

        );

        Assert.assertEquals(expected.size(), meta.size());
        TestUtils.assertEquals(expected.iterator(), meta.iterator());

    }
*/

    private void assertFile(String file, long bufSize, TextParser parser) throws Exception {
        String expected = "123,abc,2015-01-20T21:00:00.000Z,3.1415,TRUE,Lorem ipsum dolor sit amet.,122\n" +
                "124,abc,2015-01-20T21:00:00.000Z,7.342,FALSE,Lorem ipsum \n" +
                "\n" +
                "dolor \"\"sit\"\" amet.,546756\n" +
                "125,abc,2015-01-20T21:00:00.000Z,9.334,,Lorem ipsum \"\"dolor\"\" sit amet.,23\n" +
                "126,abc,2015-01-20T21:00:00.000Z,1.345,TRUE,Lorem, ipsum, dolor sit amet.,434\n" +
                "127,abc,2015-01-20T21:00:00.000Z,1.53321,TRUE,Lorem ipsum dolor sit amet.,112\n" +
                "128,abc,2015-01-20T21:00:00.000Z,2.456,TRUE,Lorem ipsum dolor sit amet.,122\n";

        TestCsvListener listener = new TestCsvListener();
        ImportManager.parse(
                new File(this.getClass().getResource(file).getFile())
                , parser
                , bufSize
                , false
                , listener

        );

        Assert.assertEquals(expected, listener.toString());
        Assert.assertEquals(1, listener.getErrorCounter());
        Assert.assertEquals(4, listener.getErrorLine());
        Assert.assertEquals(7, parser.getLineCount());
    }

    private static class TestCsvListener implements Listener {
        private final StringSink sink = new StringSink();
        private int errorCounter = 0;
        private int errorLine = -1;

        public int getErrorCounter() {
            return errorCounter;
        }

        public int getErrorLine() {
            return errorLine;
        }

        @Override
        public String toString() {
            return sink.toString();
        }

        @Override
        public void onError(int line) {
            errorCounter++;
            errorLine = line;
        }

        @Override
        public void onFieldCount(int count) {

        }

        @Override
        public void onFields(int line, CharSequence[] values, int hi) {
            for (int i = 0; i < hi; i++) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put(values[i]);
            }
            sink.put('\n');
        }

        @Override
        public void onHeader(CharSequence[] values, int hi) {

        }

        @Override
        public void onLineCount(int count) {
        }


    }
}
