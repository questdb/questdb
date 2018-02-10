/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
 ******************************************************************************/

package com.questdb;

import com.questdb.parser.plaintext.PlainTextLexer;
import com.questdb.parser.plaintext.PlainTextParser;
import com.questdb.parser.typeprobe.TypeProbeCollection;
import com.questdb.std.ObjList;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.StringSink;
import com.questdb.store.util.ImportManager;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CsvTest {

    private final BootstrapEnv env = new BootstrapEnv();

    public CsvTest() {
        env.typeProbeCollection = new TypeProbeCollection();
        env.configuration = new ServerConfiguration();
    }

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
                , new PlainTextLexer(env).of(',')
                , 1024 * 1024
                , true
                , new PlainTextParser() {
                    @Override
                    public void onError(int line) {

                    }

                    @Override
                    public void onFieldCount(int count) {

                    }

                    @Override
                    public void onFields(int line, ObjList<DirectByteCharSequence> values, int hi) {

                    }

                    @Override
                    public void onHeader(ObjList<DirectByteCharSequence> values, int hi) {
                        for (int i = 0; i < hi; i++) {
                            names.add(values.getQuick(i).toString());
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
        assertFile("/csv/test-dos.csv", 10, new PlainTextLexer(env).of(','));
    }

    @Test
    public void testParseTab() throws Exception {
        assertFile("/csv/test.txt", 1024 * 1024, new PlainTextLexer(env).of('\t'));
    }

    @Test
    public void testParseTabSmallBuf() throws Exception {
        assertFile("/csv/test.txt", 10, new PlainTextLexer(env).of('\t'));
    }

    @Test
    public void testParseUnixCsvSmallBuf() throws Exception {
        assertFile("/csv/test-unix.csv", 10, new PlainTextLexer(env).of(','));
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
                    public void onEvent(int line) {

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
        GriffinParserTestUtils.assertEquals(expected.iterator(), meta.iterator());

    }
*/

    private void assertFile(String file, long bufSize, PlainTextLexer parser) throws Exception {
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

    private static class TestCsvListener implements PlainTextParser {
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
        public void onFields(int line, ObjList<DirectByteCharSequence> values, int hi) {
            for (int i = 0; i < hi; i++) {
                if (i > 0) {
                    sink.put(',');
                }
                sink.put(values.getQuick(i));
            }
            sink.put('\n');
        }

        @Override
        public void onHeader(ObjList<DirectByteCharSequence> values, int hi) {

        }

        @Override
        public void onLineCount(int count) {
        }


    }
}
