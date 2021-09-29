/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoLexer;
import io.questdb.cutlass.line.LineProtoNanoTimestampAdapter;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestMicroClock;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CairoLineProtoParserTest extends AbstractCairoTest {

    @Test
    public void testAddColumn() throws Exception {
        final String expected = "tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp\tf5\n" +
                "abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\tNaN\n" +
                "woopsie\tdaisy\t2000\t3.0889100000000003\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\tNaN\n" +
                "444\td555\t510\t1.4000000000000001\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\t55\n" +
                "666\t777\t410\t1.1\tcomment X\tfalse\t1970-01-01T00:01:40.000000Z\tNaN\n";

        final String lines = "tab,tag=abc,tag2=xyz field=10000i,f4=9.034,field2=\"str\",fx=true 100000000000\n" +
                "tab,tag=woopsie,tag2=daisy field=2000i,f4=3.08891,field2=\"comment\",fx=true 100000000000\n" +
                "tab,tag=444,tag2=d555 field=510i,f4=1.4,f5=55i,field2=\"comment\",fx=true 100000000000\n" +
                "tab,tag=666,tag2=777 field=410i,f4=1.1,field2=\"comment\\ X\",fx=false 100000000000\n";


        assertThat(expected, lines, "tab");
    }

    @Test
    public void testStr() throws Exception {
        String expected = "host\tuptime_format\ttimestamp\n" +
                "linux-questdb\t 1:18\t2019-12-10T15:06:00.000000Z\n";
        String lines = "system,host=linux-questdb uptime_format=\" 1:18\" 1575990360000000000";
        assertThat(expected, lines, "system");
    }

    @Test
    public void testAppendExistingTable() throws Exception {
        final String expected = "double\tint\tbool\tsym1\tsym2\tstr\ttimestamp\n" +
                "1.6\t15\ttrue\t\txyz\tstring1\t2017-10-03T10:00:00.000000Z\n" +
                "1.3\t11\tfalse\tabc\t\tstring2\t2017-10-03T10:00:00.010000Z\n";

        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("double", ColumnType.DOUBLE)
                .col("int", ColumnType.LONG)
                .col("bool", ColumnType.BOOLEAN)
                .col("sym1", ColumnType.SYMBOL)
                .col("sym2", ColumnType.SYMBOL)
                .col("str", ColumnType.STRING)
                .timestamp()) {
            CairoTestUtils.create(model);
        }

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n";

        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                try {
                    return new TestMicroClock(TimestampFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10);
                } catch (NumericException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        assertThat(expected, lines, "x", configuration);
    }

    @Test
    public void testCharSingleChar() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        assertThat("char\ttimestamp\n" +
                        "c\t1970-01-01T00:00:00.000000Z\n",
                "x char=\"c\"\n",
                "x",
                new DefaultCairoConfiguration(root) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharStringIsProvided() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        assertThat("char\ttimestamp\n" +
                        "c\t1970-01-01T00:00:00.000000Z\n",
                "x char=\"coconut\"\n",
                "x",
                new DefaultCairoConfiguration(root) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharNull() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        assertThat("char\ttimestamp\n" +
                        "\t1970-01-01T00:00:00.000000Z\n",
                "x char=\"\"\n",
                "x",
                new DefaultCairoConfiguration(root) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharMissing() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        assertThat("char\ttimestamp\n",
                "x char=\n",
                "x",
                new DefaultCairoConfiguration(root) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharSingleQuote() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        assertThat("char\ttimestamp\n",
                "x char=''\n",
                "x",
                new DefaultCairoConfiguration(root) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testBinary() throws Exception {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("bin", ColumnType.BINARY)
                .timestamp()) {
            CairoTestUtils.create(model);
        }
        assertThat("bin\ttimestamp\n",
                "x bin=b10101010101\n",
                "x",
                new DefaultCairoConfiguration(root) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testBadDouble1() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t\n" +
                "\tNaN\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\" 1500000000000\n" +
                "x,sym1=abc double=1x.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" + // <-- error here
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBadDouble2() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\" 1500000000000\n" +
                "x,sym2=xyz double=1.6x,int=11i,bool=false,str=\"string0\" 1500000000000\n" +  // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBadDouble3() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6x\t11\tfalse\tstring0\t1970-01-01T00:25:00.000000Z\t\n" +
                "\ta\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\txxx\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6x,int=11i,bool=false,str=\"string0\" 1500000000000\n" +  // <-- error here, taken as symbol
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=a,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=xxx,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBadInt1() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t\n" +
                "\t1.3\tNaN\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\" 1500000000000\n" +
                "x,sym1=abc double=1.3,int=1s1i,bool=false,str=\"string2\" 1500000000000\n" + // <-- error here
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBadInt2() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\tNaN\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=1i5i,bool=true,str=\"string1\" 1500000000000\n" +  // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testLong() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\tNaN\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=1i5i,bool=true,str=\"string1\" 1500000000000\n" +  // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBadTimestamp1() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "\t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\" 1234ab\n" + // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBadTimestamp2() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:00:01.234000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t1970-01-01T00:28:20.000000Z\n" +
                "66\tbox\t7.9\t1970-01-01T00:28:20.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\" 1234000000\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500x000\n" + // <-- error here
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBusyTable() throws Exception {
        final String expected = "double\tint\tbool\tsym1\tsym2\tstr\ttimestamp\n";


        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("double", ColumnType.DOUBLE)
                .col("int", ColumnType.INT)
                .col("bool", ColumnType.BOOLEAN)
                .col("sym1", ColumnType.SYMBOL)
                .col("sym2", ColumnType.SYMBOL)
                .col("str", ColumnType.STRING)
                .timestamp()) {
            CairoTestUtils.create(model);
        }

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n";

        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                try {
                    return new TestMicroClock(TimestampFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10);
                } catch (NumericException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        // open writer so that pool cannot have it
        try (TableWriter ignored = new TableWriter(configuration, "x")) {
            assertThat(expected, lines, "x", configuration);
        }
    }

    @Test
    public void testCannotCreateTable() throws Exception {
        TestFilesFacade ff = new TestFilesFacade() {
            boolean called = false;

            @Override
            public int mkdirs(LPSZ path, int mode) {
                if (Chars.endsWith(path, "x" + Files.SEPARATOR)) {
                    called = true;
                    return -1;
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public boolean wasCalled() {
                return called;
            }
        };

        final String expected = "sym\tdouble\tint\tbool\tstr\ttimestamp\n" +
                "zzz\t1.3\t11\tfalse\tnice\t2017-10-03T10:00:00.000000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,sym=zzz double=1.3,int=11i,bool=false,str=\"nice\"\n";

        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public MicrosecondClock getMicrosecondClock() {
                try {
                    return new TestMicroClock(TimestampFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10);
                } catch (NumericException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        assertThat(expected, lines, "y", configuration);

        Assert.assertTrue(ff.wasCalled());

        try (Path path = new Path()) {
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, TableUtils.exists(ff, path, root, "all"));
        }
    }

    @Test
    public void testCreateAndAppend() throws Exception {
        final String expected = "tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp\n" +
                "abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\n" +
                "woopsie\tdaisy\t2000\t3.0889100000000003\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\n";

        final String lines = "tab,tag=abc,tag2=xyz field=10000i,f4=9.034,field2=\"str\",fx=true 100000000000\n" +
                "tab,tag=woopsie,tag2=daisy field=2000i,f4=3.08891,field2=\"comment\",fx=true 100000000000\n";


        assertThat(expected, lines, "tab");
    }

    @Test
    public void testCreateAndAppendTwoTables() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc\n" +
                "\t0.9\t6\tfalse\tstring3\t2017-10-03T10:00:00.030000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t2017-10-03T10:00:00.050000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t2017-10-03T10:00:00.020000Z\n" +
                "66\tbox\t7.9\t2017-10-03T10:00:00.040000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=t,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=f,str=\"string2\"\n" +
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=.9,int=6i,bool=F,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=T,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testCreateTable() throws Exception {
        final String expected = "tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp\n" +
                "abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\n";
        final String lines = "measurement,tag=abc,tag2=xyz field=10000i,f4=9.034,field2=\"str\",fx=true 100000000000\n";
        assertThat(expected, lines, "measurement");
    }

    @Test
    public void testReservedName() throws Exception {
        final String expected = "sym\tdouble\tint\tbool\tstr\ttimestamp\n" +
                "ok\t2.1\t11\tfalse\tdone\t2017-10-03T10:00:00.000000Z\n";


        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,sym=ok double=2.1,int=11i,bool=false,str=\"done\"\n";

        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                try {
                    return new TestMicroClock(TimestampFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10);
                } catch (NumericException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        try (Path path = new Path()) {
            Files.mkdirs(path.of(root).concat("x").slash$(), configuration.getMkDirMode());
            assertThat(expected, lines, "y", configuration);
            Assert.assertEquals(TableUtils.TABLE_RESERVED, TableUtils.exists(configuration.getFilesFacade(), path, root, "x"));
        }
    }

    @Test
    public void testSyntaxError() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t2017-10-03T10:00:00.020000Z\n" +
                "66\tbox\t7.9\t2017-10-03T10:00:00.030000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=,int=6i,bool=false,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testTypeMismatch1() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t2017-10-03T10:00:00.020000Z\n" +
                "66\tbox\t7.9\t2017-10-03T10:00:00.030000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=\"z\",int=6i,bool=false,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testTypeMismatch2() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t\n" +
                "\t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t2017-10-03T10:00:00.020000Z\n" +
                "66\tbox\t7.9\t2017-10-03T10:00:00.030000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=9.4,int=6.3,bool=false,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testUnquotedString1() throws Exception {
        final String expected1 = "sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1\n" +
                "xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t\n" +
                "\t9.4\t6\tfalse\tstring3\t2017-10-03T10:00:00.020000Z\trow3\n" +
                "\t0.30000000000000004\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t2017-10-03T10:00:00.010000Z\n" +
                "66\tbox\t7.9\t2017-10-03T10:00:00.030000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\"\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=string2\"\n" + // <-- error here
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testUnquotedString2() throws Exception {
        final String expected1 = "sym1\tdouble\tint\tbool\tstr\ttimestamp\n" +
                "abc\t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.000000Z\n" +
                "row3\t9.4\t6\tfalse\tstring3\t2017-10-03T10:00:00.020000Z\n" +
                "row4\t0.30000000000000004\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\n";

        final String expected2 = "asym1\tasym2\tadouble\ttimestamp\n" +
                "55\tbox\t5.9\t2017-10-03T10:00:00.010000Z\n" +
                "66\tbox\t7.9\t2017-10-03T10:00:00.030000Z\n";

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=string1\"\n" + // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testAddTag() throws Exception {
        final String expected = "tag\ttag3\tfield\tf4\tfield2\tfx\ttimestamp\ttag2\n" +
                "abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\t\n" +
                "woopsie\t\t2000\t3.0889100000000003\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\tdaisy\n";
        final String lines = "tab,tag=abc,tag3=xyz field=10000i,f4=9.034,field2=\"str\",fx=true 100000000000\n" +
                "tab,tag=woopsie,tag2=daisy field=2000i,f4=3.08891,field2=\"comment\",fx=true 100000000000\n";
        assertThat(expected, lines, "tab");
    }

    @Test
    public void testNoTag() throws Exception {
        String expected = "uptime_format\ttimestamp\n" +
                " 1:18\t2019-12-10T15:06:00.000000Z\n";
        String lines = "system uptime_format=\" 1:18\" 1575990360000000000";
        assertThat(expected, lines, "system");
    }

    @Test
    public void testColumnConversion1() throws Exception {
        try (
                @SuppressWarnings("resource")
                TableModel model = new TableModel(configuration, "t_ilp21",
                        PartitionBy.NONE).col("event", ColumnType.SHORT).col("id", ColumnType.LONG256).col("ts", ColumnType.TIMESTAMP).col("float1", ColumnType.FLOAT)
                        .col("int1", ColumnType.INT).col("date1", ColumnType.DATE).col("byte1", ColumnType.BYTE).timestamp()) {
            CairoTestUtils.create(model);
        }
        String lines = "t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000\n" +
                "t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000\n";
        String expected = "event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp\n" +
                "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2000\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z\n" +
                "12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0000\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z\n";
        assertThat(expected, lines, "t_ilp21");
    }

    private void assertMultiTable(String expected1, String expected2, String lines) throws Exception {
        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                try {
                    return new TestMicroClock(TimestampFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10);
                } catch (NumericException e) {
                    throw CairoException.instance(0).put("numeric");
                }
            }
        };

        assertThat(expected1, lines, "x", configuration);
        assertTable(expected2, "y");
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    private void assertThat(String expected, String lines, CharSequence tableName, CairoConfiguration configuration) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (CairoLineProtoParser parser = new CairoLineProtoParser(engine, AllowAllCairoSecurityContext.INSTANCE, LineProtoNanoTimestampAdapter.INSTANCE)) {
                    byte[] bytes = lines.getBytes(StandardCharsets.UTF_8);
                    int len = bytes.length;
                    long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (int i = 0; i < len; i++) {
                            Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
                        }
                        try (LineProtoLexer lexer = new LineProtoLexer(4096)) {
                            lexer.withParser(parser);
                            lexer.parse(mem, mem + len);
                            lexer.parseLast();
                            parser.commitAll(CommitMode.NOSYNC);
                        }
                    } finally {
                        Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
            assertTable(expected, tableName);
        });
    }

    private void assertThat(String expected, String lines, CharSequence tableName) throws Exception {
        assertThat(expected, lines, tableName, configuration);
    }
}