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

package io.questdb.test.cutlass.line.udp;

import com.sun.management.ThreadMXBean;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.line.udp.CachedCharSequence;
import io.questdb.cutlass.line.udp.CharSequenceCache;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParser;
import io.questdb.cutlass.line.udp.LineUdpParserImpl;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestFilesFacade;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestMicroClock;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class LineUdpParserImplTest extends AbstractCairoTest {

    @Test
    public void testAddColumnDefaultByte() throws Exception {
        testAddColumnInteger(ColumnType.BYTE, "0");
    }

    @Test
    public void testAddColumnDefaultDouble() throws Exception {
        testAddColumnFloat(
                ColumnType.DOUBLE,
                """
                        tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp\tf5
                        abc\txyz\t100\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\tnull
                        woopsie\tdaisy\t127\t3.08891\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\tnull
                        444\td555\t110\t1.4\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\t55.0
                        666\t777\t40\t1.1\tcomment\\ X\tfalse\t1970-01-01T00:01:40.000000Z\tnull
                        """
        );
    }

    @Test
    public void testAddColumnDefaultFloat() throws Exception {
        testAddColumnFloat(
                ColumnType.FLOAT,
                """
                        tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp\tf5
                        abc\txyz\t100\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\tnull
                        woopsie\tdaisy\t127\t3.08891\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\tnull
                        444\td555\t110\t1.4\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\t55.0
                        666\t777\t40\t1.1\tcomment\\ X\tfalse\t1970-01-01T00:01:40.000000Z\tnull
                        """
        );
    }

    @Test
    public void testAddColumnDefaultInteger() throws Exception {
        testAddColumnInteger(ColumnType.INT, "null");
    }

    @Test
    public void testAddColumnDefaultLong() throws Exception {
        testAddColumnInteger(ColumnType.LONG, "null");
    }

    @Test
    public void testAddColumnDefaultShort() throws Exception {
        testAddColumnInteger(ColumnType.SHORT, "0");
    }

    @Test
    public void testAddTag() throws Exception {
        final String expected = """
                tag\ttag3\tfield\tf4\tfield2\tfx\ttimestamp\ttag2
                abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\t
                woopsie\t\t2000\t3.08891\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\tdaisy
                """;
        final String lines = """
                tab,tag=abc,tag3=xyz field=10000i,f4=9.034,field2="str",fx=true 100000000000
                tab,tag=woopsie,tag2=daisy field=2000i,f4=3.08891,field2="comment",fx=true 100000000000
                """;
        assertThat(expected, lines, "tab");
    }

    @Test
    public void testAppendExistingTable() throws Exception {
        final String expected = """
                double\tint\tbool\tsym1\tsym2\tstr\ttimestamp
                1.6\t15\ttrue\t\txyz\tstring1\t2017-10-03T10:00:00.000000Z
                1.3\t11\tfalse\tabc\t\tstring2\t2017-10-03T10:00:00.010000Z
                """;

        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("double", ColumnType.DOUBLE)
                .col("int", ColumnType.LONG)
                .col("bool", ColumnType.BOOLEAN)
                .col("sym1", ColumnType.SYMBOL)
                .col("sym2", ColumnType.SYMBOL)
                .col("str", ColumnType.STRING)
                .timestamp();
        AbstractCairoTest.create(model);

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                """;

        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                return TestUtils.unchecked(() -> new TestMicroClock(MicrosFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10));
            }
        };

        assertThat(expected, lines, "x", configuration);
    }

    @Test
    public void testBadDouble1() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t
                \tnull\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

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
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

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
        // 1.6x makes column "double" a SYMBOL; the 1.3 sent to it later is stored as its text, as over TCP
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6x\t11\tfalse\tstring0\t1970-01-01T00:25:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \ta\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \txxx\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

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
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t
                \t1.3\tnull\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

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
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\tnull\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

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
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                \t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

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
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t1970-01-01T00:00:01.234000Z\t
                \t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=\"string1\" 1234000000\n" +
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500x000\n" + // <-- error here
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testBinary() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("bin", ColumnType.BINARY)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat("bin\ttimestamp\n",
                "x bin=b10101010101\n",
                "x",
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testBusyTable() throws Exception {
        final String expected = "double\tint\tbool\tsym1\tsym2\tstr\ttimestamp\n";
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("double", ColumnType.DOUBLE)
                .col("int", ColumnType.INT)
                .col("bool", ColumnType.BOOLEAN)
                .col("sym1", ColumnType.SYMBOL)
                .col("sym2", ColumnType.SYMBOL)
                .col("str", ColumnType.STRING)
                .timestamp();
        AbstractCairoTest.create(model);

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                """;

        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                return TestUtils.unchecked(() -> new TestMicroClock(MicrosFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10));
            }
        };

        // open writer so that pool cannot have it
        try (TableWriter ignored = newOffPoolWriter(configuration, "x")) {
            assertThat(expected, lines, "x", configuration);
        }
    }

    @Test
    public void testCannotAutoCreateColumnWhenDisabledInConfiguration() throws Exception {
        try {
            String lines = """
                    t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                    t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                    """;

            assertThat("", lines, "t_ilp21", configuration, new DefaultLineUdpReceiverConfiguration() {
                @Override
                public boolean getAutoCreateNewColumns() {
                    return false;
                }
            });
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "creating new columns is disabled [table=t_ilp21]");
        }
    }

    @Test
    public void testCannotAutoCreateTableWhenDisabledInConfiguration() throws Exception {
        try {
            String lines = """
                    t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                    t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                    """;

            assertThat("", lines, "t_ilp21", configuration, new DefaultLineUdpReceiverConfiguration() {
                @Override
                public boolean getAutoCreateNewTables() {
                    return false;
                }
            });
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "creating new tables is disabled [table=t_ilp21]");
        }
    }

    @Test
    public void testCannotCreateTable() throws Exception {
        CharSequence dirName = "x" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        TestFilesFacade ff = new TestFilesFacade() {
            boolean called = false;

            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.endsWithAscii(path, Chars.toString(dirName) + Files.SEPARATOR)) {
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

        final String expected = """
                sym\tdouble\tint\tbool\tstr\ttimestamp
                zzz\t1.3\t11\tfalse\tnice\t2017-10-03T10:00:00.000000Z
                """;

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                y,sym=zzz double=1.3,int=11i,bool=false,str="nice"
                """;

        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                return TestUtils.unchecked(() -> new TestMicroClock(MicrosFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10));
            }
        };

        assertThat(expected, lines, "y", configuration);

        Assert.assertTrue(ff.wasCalled());

        try (Path path = new Path()) {
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, TableUtils.exists(ff, path, root, "all"));
        }
    }

    @Test
    public void testCharMissing() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat("""
                        char\ttimestamp
                        \t1970-01-01T00:00:00.000000Z
                        """,
                "x char=\n",
                "x",
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharNull() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat("""
                        char\ttimestamp
                        \t1970-01-01T00:00:00.000000Z
                        """,
                "x char=\"\"\n",
                "x",
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharSingleChar() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat("""
                        char\ttimestamp
                        c\t1970-01-01T00:00:00.000000Z
                        """,
                "x char=\"c\"\n",
                "x",
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharSingleQuote() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat("char\ttimestamp\n",
                "x char=''\n",
                "x",
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testCharStringIsProvided() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("char", ColumnType.CHAR)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat("""
                        char\ttimestamp
                        c\t1970-01-01T00:00:00.000000Z
                        """,
                "x char=\"coconut\"\n",
                "x",
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MicrosecondClock getMicrosecondClock() {
                        return new TestMicroClock(0, 0);
                    }
                });
    }

    @Test
    public void testColumnConversion1() throws Exception {
        TableModel model = new TableModel(configuration, "t_ilp21",
                PartitionBy.NONE).col("event", ColumnType.SHORT).col("id", ColumnType.LONG256).col("ts", ColumnType.TIMESTAMP).col("float1", ColumnType.FLOAT)
                .col("int1", ColumnType.INT).col("date1", ColumnType.DATE).col("byte1", ColumnType.BYTE).timestamp();
        AbstractCairoTest.create(model);
        String lines = """
                t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;
        String expected = """
                event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp
                12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z
                12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z
                """;
        assertThat(expected, lines, "t_ilp21");
    }

    @Test
    public void testDecimalExponentAtIntLimits() throws Exception {
        // an exponent near the int limits puts the value out of range, a cast error like any other: the row
        // keeps NULL there and the lines after it are written
        assertForEachStringDefault(
                """
                        d\tn\ttimestamp
                        \t1\t1970-01-01T00:00:01.000000Z
                        \t2\t1970-01-01T00:00:02.000000Z
                        \t3\t1970-01-01T00:00:03.000000Z
                        \t4\t1970-01-01T00:00:04.000000Z
                        \t5\t1970-01-01T00:00:05.000000Z
                        \t6\t1970-01-01T00:00:06.000000Z
                        12.34\t7\t1970-01-01T00:00:07.000000Z
                        """,
                """
                        x d=1e2147483647,n=1i 1000000000
                        x d="1e2147483647",n=2i 2000000000
                        x d=-1e2147483647,n=3i 3000000000
                        x d=1e-2147483648,n=4i 4000000000
                        x d="1e-2147483648",n=5i 5000000000
                        x d="1.5e-2147483647",n=6i 6000000000
                        x d=12.34,n=7i 7000000000
                        """,
                new TableModel(configuration, "x", PartitionBy.NONE)
                        .col("d", ColumnType.getDecimalType(10, 2))
                        .col("n", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testDecimalFromNumber() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("d", ColumnType.getDecimalType(10, 2))
                .col("n", ColumnType.INT)
                .timestamp();
        AbstractCairoTest.create(model);
        // a bare number into a DECIMAL column, as over TCP; one that does not fit the scale
        // is a cast error, so the row keeps NULL there as for any other bad value
        assertThat(
                """
                        d\tn\ttimestamp
                        12.34\t1\t1970-01-01T00:00:01.000000Z
                        1.50\t2\t1970-01-01T00:00:02.000000Z
                        \t3\t1970-01-01T00:00:03.000000Z
                        """,
                """
                        x d=12.34,n=1i 1000000000
                        x d=1.5,n=2i 2000000000
                        x d=1.234,n=3i 3000000000
                        """,
                "x"
        );
    }

    @Test
    public void testDecimalFromString() throws Exception {
        // ILP over TCP stores "12.34" in a DECIMAL column; UDP must not store NULL instead.
        // Text that is not a number is a cast error: the row keeps NULL there.
        assertForEachStringDefault(
                """
                        d\tn\ttimestamp
                        12.34\t1\t1970-01-01T00:00:01.000000Z
                        \t2\t1970-01-01T00:00:02.000000Z
                        -0.01\t3\t1970-01-01T00:00:03.000000Z
                        """,
                """
                        x d="12.34",n=1i 1000000000
                        x d="abc",n=2i 2000000000
                        x d="-0.01",n=3i 3000000000
                        """,
                new TableModel(configuration, "x", PartitionBy.NONE)
                        .col("d", ColumnType.getDecimalType(10, 2))
                        .col("n", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testCreateAndAppend() throws Exception {
        final String expected = """
                tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp
                abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z
                woopsie\tdaisy\t2000\t3.08891\tcomment\ttrue\t1970-01-01T00:01:40.000000Z
                """;

        final String lines = """
                tab,tag=abc,tag2=xyz field=10000i,f4=9.034,field2="str",fx=true 100000000000
                tab,tag=woopsie,tag2=daisy field=2000i,f4=3.08891,field2="comment",fx=true 100000000000
                """;


        assertThat(expected, lines, "tab");
    }

    @Test
    public void testCreateAndAppendTwoTables() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc
                \t0.9\t6\tfalse\tstring3\t2017-10-03T10:00:00.030000Z\trow3
                \t0.3\t91\ttrue\tstring4\t2017-10-03T10:00:00.050000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t2017-10-03T10:00:00.020000Z
                66\tbox\t7.9\t2017-10-03T10:00:00.040000Z
                """;

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=t,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=f,str="string2"
                y,asym1=55,asym2=box adouble=5.9
                x,sym1=row3 double=.9,int=6i,bool=F,str="string3"
                y,asym1=66,asym2=box adouble=7.9
                x,sym1=row4 double=.3,int=91i,bool=T,str="string4"
                """;

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testCreateTable() throws Exception {
        final String expected = """
                tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp
                abc\txyz\t10000\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z
                """;
        final String lines = "measurement,tag=abc,tag2=xyz field=10000i,f4=9.034,field2=\"str\",fx=true 100000000000\n";
        assertThat(expected, lines, "measurement");
    }

    @Test
    public void testExistingTableRemovedColumn() throws Exception {
        TableModel model = new TableModel(configuration, "t_ilp21",
                PartitionBy.NONE)
                .col("event", ColumnType.SHORT)
                .col("id", ColumnType.LONG256)
                .col("ts", ColumnType.TIMESTAMP)
                .col("float1", ColumnType.FLOAT)
                .col("int1", ColumnType.INT)
                .col("date1", ColumnType.DATE)
                .col("byte1", ColumnType.BYTE)
                .timestamp();
        AbstractCairoTest.create(model);
        try (TableWriter writer = getWriter("t_ilp21")) {
            writer.removeColumn("event");
        }
        engine.releaseInactive();

        String lines = """
                t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;
        String expected = """
                id\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp\tevent
                0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z\t12
                0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z\t12
                """;
        assertThat(expected, lines, "t_ilp21");
    }

    @Test
    public void testExistingTableRemovedColumnulldAddedBack() throws Exception {
        TableModel model = new TableModel(configuration, "t_ilp21",
                PartitionBy.NONE)
                .col("event", ColumnType.SHORT)
                .col("id", ColumnType.LONG256)
                .col("ts", ColumnType.TIMESTAMP)
                .col("float1", ColumnType.FLOAT)
                .col("int1", ColumnType.INT)
                .col("date1", ColumnType.DATE)
                .col("byte1", ColumnType.BYTE)
                .timestamp();
        AbstractCairoTest.create(model);
        try (TableWriter writer = getWriter("t_ilp21")) {
            writer.removeColumn("event");
            writer.addColumn("event", ColumnType.SHORT, AllowAllSecurityContext.INSTANCE);
        }
        engine.releaseInactive();

        String lines = """
                t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;
        String expected = """
                id\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp\tevent
                0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z\t12
                0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z\t12
                """;
        assertThat(expected, lines, "t_ilp21");
    }

    @Test
    public void testExistingTableWhenAddingNewTablesAndColumnsDisabled() throws Exception {
        TableModel model = new TableModel(configuration, "t_ilp21",
                PartitionBy.NONE)
                .col("event", ColumnType.SHORT)
                .col("id", ColumnType.LONG256)
                .col("ts", ColumnType.TIMESTAMP)
                .col("float1", ColumnType.FLOAT)
                .col("int1", ColumnType.INT)
                .col("date1", ColumnType.DATE)
                .col("byte1", ColumnType.BYTE)
                .timestamp();
        AbstractCairoTest.create(model);

        String lines = """
                t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;
        String expected = """
                event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp
                12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1.2\t23\t2016-06-13T17:43:50.102Z\t-7\t2016-06-13T17:43:50.102800Z
                12\t0x5a9796963abad00001e5f6bbdb38\t2016-06-13T17:43:50.102400Z\t1000.0\t-500000\t2016-06-13T17:43:50.102Z\t3\t2016-06-13T17:43:50.102800Z
                """;
        assertThat(expected, lines, "t_ilp21", configuration, new DefaultLineUdpReceiverConfiguration() {
            @Override
            public boolean getAutoCreateNewColumns() {
                return false;
            }

            @Override
            public boolean getAutoCreateNewTables() {
                return false;
            }
        });
    }

    @Test
    public void testFailsToAddAColumnWhenAutoColumnAddDisabled() throws Exception {
        TableModel model = new TableModel(configuration, "t_ilp21",
                PartitionBy.NONE)
                .col("event", ColumnType.SHORT)
                .col("id", ColumnType.LONG256)
                .col("ts", ColumnType.TIMESTAMP)
                .col("float1", ColumnType.FLOAT)
                .col("int1", ColumnType.INT)
                .col("date1", ColumnType.DATE)
                .timestamp();
        AbstractCairoTest.create(model);

        String lines = """
                t_ilp21 event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;
        try {
            assertThat("", lines, "t_ilp21", configuration, new DefaultLineUdpReceiverConfiguration() {
                @Override
                public boolean getAutoCreateNewColumns() {
                    return false;
                }
            });
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "column does not exist, creating new columns is disabled [table=t_ilp21, columnName=byte1]");
        }
    }

    @Test
    public void testFailsToAddColumnWithInvalidName() throws Exception {
        String lines = """
                t_ilp21 ..event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;

        assertThat(
                """
                        event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp
                        12\t0x5a9796963abad00001e5f6bbdb38\t1465839830102400\t1000.0\t-500000\t1465839830102\t3\t2016-06-13T17:43:50.102800Z
                        """,
                lines,
                "t_ilp21",
                configuration
        );
    }

    @Test
    public void testIPv4Null() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("ip", ColumnType.IPv4)
                .col("n", ColumnType.INT)
                .timestamp();
        AbstractCairoTest.create(model);
        assertThat(
                """
                        ip\tn\ttimestamp
                        \t1\t1970-01-01T00:00:01.000000Z
                        1.1.1.1\t2\t1970-01-01T00:00:02.000000Z
                        2.2.2.2\t3\t1970-01-01T00:00:03.000000Z
                        """,
                """
                        x ip=,n=1i 1000000000
                        x ip="1.1.1.1",n=2i 2000000000
                        x ip="2.2.2.2",n=3i 3000000000
                        """,
                "x"
        );
    }

    @Test
    public void testInvalidTableName() throws Exception {
        String lines = """
                ..\\/db,a=b event=12i,id=0x05a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1.2,int1=23i,date1=1465839830102i,byte1=-7i 1465839830102800000
                t_ilp21 event=12i,id=0x5a9796963abad00001e5f6bbdb38i,ts=1465839830102400i,float1=1e3,int1=-500000i,date1=1465839830102i,byte1=3i 1465839830102800000
                """;

        assertThat(
                """
                        event\tid\tts\tfloat1\tint1\tdate1\tbyte1\ttimestamp
                        12\t0x5a9796963abad00001e5f6bbdb38\t1465839830102400\t1000.0\t-500000\t1465839830102\t3\t2016-06-13T17:43:50.102800Z
                        """,
                lines,
                "t_ilp21",
                configuration
        );
    }

    @Test
    public void testLong() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\tnull\ttrue\tstring1\t1970-01-01T00:25:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t1970-01-01T00:25:00.000000Z\tabc
                \t9.4\t6\tfalse\tstring3\t1970-01-01T00:25:00.000000Z\trow3
                \t0.3\t91\ttrue\tstring4\t1970-01-01T00:25:00.000000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t1970-01-01T00:28:20.000000Z
                66\tbox\t7.9\t1970-01-01T00:28:20.000000Z
                """;

        String lines = "x,sym2=xyz double=1.6,int=1i5i,bool=true,str=\"string1\" 1500000000000\n" +  // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\" 1500000000000\n" +
                "y,asym1=55,asym2=box adouble=5.9 1700000000000\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\" 1500000000000\n" +
                "y,asym1=66,asym2=box adouble=7.9 1700000000000\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\" 1500000000000\n";

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testNoTag() throws Exception {
        String expected = """
                uptime_format\ttimestamp
                 1:18\t2019-12-10T15:06:00.000000Z
                """;
        String lines = "system uptime_format=\" 1:18\" 1575990360000000000";
        assertThat(expected, lines, "system");
    }

    @Test
    public void testO3() throws Exception {
        String expected = """
                host\tuptime_format\ttimestamp
                linux-questdb\t 1:18\t2019-12-10T15:06:00.000000Z
                linux-questdb\t 4:38\t2019-12-10T15:06:01.000000Z
                """;
        String lines = """
                system,host=linux-questdb uptime_format=" 4:38" 1575990361000000000
                system,host=linux-questdb uptime_format=" 1:18" 1575990360000000000
                """;
        assertThat(expected, lines, "system");
    }

    @Test
    public void testPlusSignedNumber() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("d", ColumnType.DOUBLE)
                .col("f", ColumnType.FLOAT)
                .timestamp();
        AbstractCairoTest.create(model);
        // an explicit plus sign is a number, not a symbol, as over TCP; a new column takes
        // the float type
        assertThat(
                """
                        d\tf\ttimestamp\te
                        1.5\t2.5\t1970-01-01T00:00:01.000000Z\t4.5
                        """,
                "x d=+1.5,f=+2.5,e=+4.5 1000000000\n",
                "x"
        );
    }

    @Test
    public void testRepeatedColumnInLineThatCreatesTable() throws Exception {
        // the line that creates a table keeps the first value of a column it names twice, even an empty
        // repeat, and ignores a column named like the designated timestamp, as for a table that exists
        assertThat(
                """
                        s\tv\ttimestamp
                        a\t1.5\t1970-01-01T00:00:01.000000Z
                        z\t9.5\t1970-01-01T00:00:02.000000Z
                        """,
                """
                        nt,s=a,S=b v=1.5,v=2.5,timestamp=5000000t,Timestamp=6i,V= 1000000000
                        nt,s=z v=9.5 2000000000
                        """,
                "nt"
        );
        // a malformed repeat drops the line as any malformed value does, and so does an empty value for a
        // column that is not a repeat, which cannot give the column a type; the next line creates the table
        assertThat(
                """
                        s\tv\ttimestamp
                        b\t2.5\t1970-01-01T00:00:02.000000Z
                        """,
                """
                        nt2,s=a v=1.5,v=bad" 1000000000
                        nt2,s=a v= 1500000000
                        nt2,s=b v=2.5 2000000000
                        """,
                "nt2"
        );
    }

    @Test
    public void testRepeatedColumnInLineThatCreatesTableComparesOnlyRepeats() throws Exception {
        // the parser reads a kept name back only when its hash matches the new name's, so a line of 1,000
        // different names and 3 repeats reads 3 names, not every kept name for each new one, and allocates
        // nothing once warm; the empty value of a new column drops the line, so each copy takes the same path
        assertMemoryLeak(() -> {
            final StringBuilder sb = new StringBuilder("nt ");
            for (int i = 0; i < 1_000; i++) {
                sb.append('c').append(i).append("=1.5,");
            }
            sb.append("C0=2.5,C500=2.5,C999=2.5,e=\n");
            final byte[] bytes = sb.toString().getBytes(Files.UTF_8);
            final long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
            try (
                    LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
                    LineUdpLexer lexer = new LineUdpLexer(4096);
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.putByte(mem + i, bytes[i]);
                }
                final CacheReadCountingParser countingParser = new CacheReadCountingParser(parser);
                lexer.withParser(countingParser);
                lexer.parse(mem, mem + bytes.length);
                Assert.assertEquals(3, countingParser.reads);

                final ThreadMXBean threadMXBean = scope.getBean();
                final long threadId = Thread.currentThread().threadId();
                final long allocatedBefore = threadMXBean.getThreadAllocatedBytes(threadId);
                for (int i = 0; i < 10_000; i++) {
                    lexer.parse(mem, mem + bytes.length);
                }
                final long allocated = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedBefore;
                Assert.assertEquals(3 + 10_000 * 3, countingParser.reads);
                // a 16-byte object per line would add up to 160 KB
                Assert.assertTrue("10,000 lines allocated " + allocated + " bytes", allocated < 64 * 1024);
                Assert.assertNull(engine.getTableTokenIfExists("nt"));
            } finally {
                Unsafe.free(mem, bytes.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRepeatedColumnInLineThatCreatesTableIgnoresCaseOfAnyLetter() throws Exception {
        // names match ignoring the case of any letter, as the table's column names do; the two Cyrillic
        // names, a+ya and b+a, differ but have the same hash, so both are kept and each is repeated in upper
        // case; the ten names after them make the parser's set of names grow while it holds the pair
        assertThat(
                """
                        a\u044F\tb\u0430\t\u00E9\tc0\tc1\tc2\tc3\tc4\tc5\tc6\tc7\tc8\tc9\ttimestamp
                        1.5\t2.5\t3.5\t0.5\t1.5\t2.5\t3.5\t4.5\t5.5\t6.5\t7.5\t8.5\t9.5\t1970-01-01T00:00:01.000000Z
                        """,
                "nt a\u044F=1.5,b\u0430=2.5,\u00E9=3.5,c0=0.5,c1=1.5,c2=2.5,c3=3.5,c4=4.5,c5=5.5,c6=6.5,c7=7.5,"
                        + "c8=8.5,c9=9.5,B\u0410=-1.5,\u00C9=-1.5,A\u042F=-1.5 1000000000\n",
                "nt"
        );
    }

    @Test
    public void testRepeatedColumnInLineThatCreatesWideTable() throws Exception {
        // 24 names make the parser's set of names grow twice; repeats, in upper case, of the first name, of
        // the names on both sides of each growth and of the last name keep the first value
        final StringBuilder expected = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        final StringBuilder line = new StringBuilder("nt ");
        for (int i = 0; i < 24; i++) {
            expected.append('c').append(i).append('\t');
            values.append(i).append(".5\t");
            line.append('c').append(i).append('=').append(i).append(".5,");
        }
        expected.append("timestamp\n").append(values).append("1970-01-01T00:00:01.000000Z\n");
        line.append("C0=-1.5,C7=-1.5,C8=-1.5,C15=-1.5,C16=-1.5,C23=-1.5 1000000000\n");
        assertThat(expected.toString(), line.toString(), "nt");
    }

    @Test
    public void testRepeatedColumnKeepsFirstValue() throws Exception {
        // a line that names a column twice keeps the first value and ignores the repeat, as ILP over TCP
        // does, for tags and fields, for a column the line adds and for a repeat of the wrong type; a field
        // named like the designated timestamp is ignored too, so the line's own timestamp sets the row's
        // time, where ILP over TCP would take the field as the row timestamp
        AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.NONE)
                .col("s", ColumnType.SYMBOL)
                .col("u", ColumnType.UUID)
                .col("d", ColumnType.getDecimalType(10, 2))
                .col("v", ColumnType.DOUBLE)
                .col("n", ColumnType.INT)
                .timestamp("ts"));
        assertThat(
                """
                        s\tu\td\tv\tn\tts\tc
                        a\t\t\tnull\t1\t1970-01-01T00:00:01.000000Z\tnull
                        a\t\t\tnull\t2\t1970-01-01T00:00:02.000000Z\tnull
                        \t11111111-2222-3333-4444-555555555555\t\tnull\t3\t1970-01-01T00:00:03.000000Z\tnull
                        \t\t1.25\tnull\t4\t1970-01-01T00:00:04.000000Z\tnull
                        \t\t1.25\tnull\t5\t1970-01-01T00:00:05.000000Z\tnull
                        \t\t\t1.5\t6\t1970-01-01T00:00:06.000000Z\tnull
                        \t\t\tnull\t7\t1970-01-01T00:00:07.000000Z\t1.5
                        \t\t\tnull\t8\t1970-01-01T00:00:08.000000Z\t3.5
                        \t\t\tnull\t9\t1970-01-01T00:00:09.000000Z\tnull
                        \t\t\tnull\t10\t1970-01-01T00:00:10.000000Z\tnull
                        \t\t\t1.5\t11\t1970-01-01T00:00:11.000000Z\tnull
                        z\t\t\t9.5\t12\t1970-01-01T00:00:12.000000Z\tnull
                        """,
                """
                        x,s=a,s=b n=1i 1000000000
                        x,s=a S="b",n=2i 2000000000
                        x u="11111111-2222-3333-4444-555555555555",u="66666666-7777-8888-9999-000000000000",n=3i 3000000000
                        x d=1.25,d=2.5,n=4i 4000000000
                        x d="1.25",D="2.50",n=5i 5000000000
                        x v=1.5,V=2.5,n=6i 6000000000
                        x c=1.5,c=2.5,n=7i 7000000000
                        x c=3.5,C=4.5,n=8i 8000000000
                        x ts=5000000t,n=9i 9000000000
                        x TS=5000000i,n=10i 10000000000
                        x v=1.5,v="abc",n=11i 11000000000
                        x,s=z v=9.5,n=12i 12000000000
                        """,
                "x"
        );
    }

    @Test
    public void testReservedName() throws Exception {
        final String expected = """
                sym\tdouble\tint\tbool\tstr\ttimestamp
                ok\t2.1\t11\tfalse\tdone\t2017-10-03T10:00:00.000000Z
                """;


        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                y,sym=ok double=2.1,int=11i,bool=false,str="done"
                """;

        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                return TestUtils.unchecked(() -> new TestMicroClock(MicrosFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10));
            }
        };

        try (Path path = new Path()) {
            CharSequence dirName = "x" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            Files.mkdirs(path.of(root).concat(dirName).slash(), configuration.getMkDirMode());
            assertThat(expected, lines, "y", configuration);
            Assert.assertEquals(TableUtils.TABLE_RESERVED, TableUtils.exists(configuration.getFilesFacade(), path, root, dirName));
        }
    }

    @Test
    public void testStr() throws Exception {
        String expected = """
                host\tuptime_format\ttimestamp
                linux-questdb\t 1:18\t2019-12-10T15:06:00.000000Z
                """;
        String lines = "system,host=linux-questdb uptime_format=\" 1:18\" 1575990360000000000";
        assertThat(expected, lines, "system");
    }

    @Test
    public void testStringIntoArrayColumnIsRejected() throws Exception {
        // a string sent to an array column is a type mismatch whatever the array's dimension count: the line
        // is dropped, it is not stored with a NULL array
        assertMemoryLeak(() -> {
            for (int nDims = 1; nDims <= ColumnType.ARRAY_NDIMS_LIMIT; nDims++) {
                AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.NONE)
                        .col("a", ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims))
                        .col("n", ColumnType.INT)
                        .timestamp());
                parse("""
                        x a="abc",n=1i 1000000000
                        x n=2i 2000000000
                        """, configuration, new DefaultLineUdpReceiverConfiguration());
                refreshTablesInBaseEngine();
                try (
                        TableReader reader = newOffPoolReader(configuration, "x");
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    Assert.assertTrue("dimensions: " + nDims, cursor.hasNext());
                    Assert.assertEquals("dimensions: " + nDims, 2, cursor.getRecord().getInt(1));
                    Assert.assertFalse("dimensions: " + nDims, cursor.hasNext());
                }
                execute("DROP TABLE x");
            }
        });
    }

    @Test
    public void testSymbolFromNumber() throws Exception {
        // a bare number sent to a SYMBOL column is stored as its text, as over TCP, whichever float type new
        // columns take; a bare value that only looks like a number, such as +05:30, keeps its text too, where
        // TCP would refuse the line
        for (short floatType : new short[]{ColumnType.DOUBLE, ColumnType.FLOAT}) {
            AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("s", ColumnType.SYMBOL)
                    .col("n", ColumnType.INT)
                    .timestamp());
            assertThat(
                    """
                            s\tn\ttimestamp
                            +15551234567\t1\t1970-01-01T00:00:01.000000Z
                            +1.5\t2\t1970-01-01T00:00:02.000000Z
                            1.5\t3\t1970-01-01T00:00:03.000000Z
                            -1.5\t4\t1970-01-01T00:00:04.000000Z
                            +0530\t5\t1970-01-01T00:00:05.000000Z
                            +05:30\t6\t1970-01-01T00:00:06.000000Z
                            -05:30\t7\t1970-01-01T00:00:07.000000Z
                            1e5\t8\t1970-01-01T00:00:08.000000Z
                            NaN\t9\t1970-01-01T00:00:09.000000Z
                            .5\t10\t1970-01-01T00:00:10.000000Z
                            """,
                    """
                            x s=+15551234567,n=1i 1000000000
                            x s=+1.5,n=2i 2000000000
                            x s=1.5,n=3i 3000000000
                            x s=-1.5,n=4i 4000000000
                            x s=+0530,n=5i 5000000000
                            x s=+05:30,n=6i 6000000000
                            x s=-05:30,n=7i 7000000000
                            x s=1e5,n=8i 8000000000
                            x s=NaN,n=9i 9000000000
                            x s=.5,n=10i 10000000000
                            """,
                    "x",
                    configuration,
                    new DefaultLineUdpReceiverConfiguration() {
                        @Override
                        public short getDefaultColumnTypeForFloat() {
                            return floatType;
                        }
                    }
            );
            execute("DROP TABLE x");
        }
    }

    @Test
    public void testSymbolFromString() throws Exception {
        // a quoted field value loses its quotes; a tag value keeps every character, as over TCP, and so
        // does a bare field value, which TCP would refuse
        assertForEachStringDefault(
                """
                        s\tt\tn\ttimestamp
                        abc\t\t1\t1970-01-01T00:00:01.000000Z
                        a b\t"q"\t2\t1970-01-01T00:00:02.000000Z
                        bare\tplain\t3\t1970-01-01T00:00:03.000000Z
                        """,
                """
                        x s="abc",n=1i 1000000000
                        x,t="q" s="a b",n=2i 2000000000
                        x,t=plain s=bare,n=3i 3000000000
                        """,
                new TableModel(configuration, "x", PartitionBy.NONE)
                        .col("s", ColumnType.SYMBOL)
                        .col("t", ColumnType.SYMBOL)
                        .col("n", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testSyntaxError() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc
                \tnull\t6\tfalse\tstring3\t2017-10-03T10:00:00.030000Z\trow3
                \t0.3\t91\ttrue\tstring4\t2017-10-03T10:00:00.050000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t2017-10-03T10:00:00.020000Z
                66\tbox\t7.9\t2017-10-03T10:00:00.040000Z
                """;

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                y,asym1=55,asym2=box adouble=5.9
                x,sym1=row3 double=,int=6i,bool=false,str="string3"
                y,asym1=66,asym2=box adouble=7.9
                x,sym1=row4 double=.3,int=91i,bool=true,str="string4"
                """;

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testTypeMismatch1() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc
                \t0.3\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t2017-10-03T10:00:00.020000Z
                66\tbox\t7.9\t2017-10-03T10:00:00.030000Z
                """;

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                y,asym1=55,asym2=box adouble=5.9
                x,sym1=row3 double="z",int=6i,bool=false,str="string3"
                y,asym1=66,asym2=box adouble=7.9
                x,sym1=row4 double=.3,int=91i,bool=true,str="string4"
                """;

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testTypeMismatch2() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t
                \t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.010000Z\tabc
                \t0.3\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t2017-10-03T10:00:00.020000Z
                66\tbox\t7.9\t2017-10-03T10:00:00.030000Z
                """;

        String lines = """
                x,sym2=xyz double=1.6,int=15i,bool=true,str="string1"
                x,sym1=abc double=1.3,int=11i,bool=false,str="string2"
                y,asym1=55,asym2=box adouble=5.9
                x,sym1=row3 double=9.4,int=6.3,bool=false,str="string3"
                y,asym1=66,asym2=box adouble=7.9
                x,sym1=row4 double=.3,int=91i,bool=true,str="string4"
                """;

        assertMultiTable(expected1, expected2, lines);
    }

    @Test
    public void testUuidFromString() throws Exception {
        // a string into a UUID column, as over TCP; text that is not a UUID is a cast error
        assertForEachStringDefault(
                """
                        u\tn\ttimestamp
                        11111111-2222-3333-4444-555555555555\t1\t1970-01-01T00:00:01.000000Z
                        \t2\t1970-01-01T00:00:02.000000Z
                        """,
                """
                        x u="11111111-2222-3333-4444-555555555555",n=1i 1000000000
                        x u="not a uuid",n=2i 2000000000
                        """,
                new TableModel(configuration, "x", PartitionBy.NONE)
                        .col("u", ColumnType.UUID)
                        .col("n", ColumnType.INT)
                        .timestamp()
        );
    }

    @Test
    public void testUnquotedString1() throws Exception {
        final String expected1 = """
                sym2\tdouble\tint\tbool\tstr\ttimestamp\tsym1
                xyz\t1.6\t15\ttrue\tstring1\t2017-10-03T10:00:00.000000Z\t
                \t9.4\t6\tfalse\tstring3\t2017-10-03T10:00:00.020000Z\trow3
                \t0.3\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z\trow4
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t2017-10-03T10:00:00.010000Z
                66\tbox\t7.9\t2017-10-03T10:00:00.030000Z
                """;

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
        final String expected1 = """
                sym1\tdouble\tint\tbool\tstr\ttimestamp
                abc\t1.3\t11\tfalse\tstring2\t2017-10-03T10:00:00.000000Z
                row3\t9.4\t6\tfalse\tstring3\t2017-10-03T10:00:00.020000Z
                row4\t0.3\t91\ttrue\tstring4\t2017-10-03T10:00:00.040000Z
                """;

        final String expected2 = """
                asym1\tasym2\tadouble\ttimestamp
                55\tbox\t5.9\t2017-10-03T10:00:00.010000Z
                66\tbox\t7.9\t2017-10-03T10:00:00.030000Z
                """;

        String lines = "x,sym2=xyz double=1.6,int=15i,bool=true,str=string1\"\n" + // <-- error here
                "x,sym1=abc double=1.3,int=11i,bool=false,str=\"string2\"\n" +
                "y,asym1=55,asym2=box adouble=5.9\n" +
                "x,sym1=row3 double=9.4,int=6i,bool=false,str=\"string3\"\n" +
                "y,asym1=66,asym2=box adouble=7.9\n" +
                "x,sym1=row4 double=.3,int=91i,bool=true,str=\"string4\"\n";

        assertMultiTable(expected1, expected2, lines);
    }

    // A table the parser cannot write (its writer is busy, or creating it fails) leaves the parser without a writer for
    // the table's lines. Those lines, and the columns they add, must not land in the table of the line before, in any
    // mode.
    @Test
    public void testUnresolvedTableLinesNeverLandInAnotherTable() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "false");
        assertMemoryLeak(() -> {
            Assert.assertFalse(engine.getConfiguration().getWalEnabledDefault());
            execute("create table n (s symbol, v long, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("create table b (s symbol, v long, ts timestamp) timestamp(ts) partition by day bypass wal");
            try (Path path = new Path()) {
                // A directory in the way makes creating table x fail.
                Files.mkdirs(path.of(root).concat("x" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX).slash(), configuration.getMkDirMode());
            }
            try (
                    TableWriter ignored = getWriter("b");
                    LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
                    LineUdpLexer lexer = new LineUdpLexer(4096)
            ) {
                lexer.withParser(parser);
                parseDatagram(lexer, """
                        n,s=a v=1i 1704153600000000000
                        b,s=b v=2i 1704157200000000000
                        b,s=c v=3i,q=1i 1704160800000000000
                        n,s=d v=4i 1704164400000000000
                        x,s=e v=5i 1704168000000000000
                        x,s=f v=6i,r=1i 1704171600000000000
                        n,s=g v=7i 1704175200000000000
                        """);
                lexer.parseLast();
                parser.commitAll();
            }
            assertQuery("select * from n")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s\tv\tts
                            a\t1\t2024-01-02T00:00:00.000000Z
                            d\t4\t2024-01-02T03:00:00.000000Z
                            g\t7\t2024-01-02T06:00:00.000000Z
                            """);
            assertQuery("select * from b")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("s\tv\tts\n");
            Assert.assertNull(engine.getTableTokenIfExists("x"));
        });
    }

    // Another session can create a table as WAL while the parser is inside a line that creates it as non-WAL. The
    // table is refused then too, with its later lines, in every commit mode.
    @Test
    public void testWalTableCreatedMidLineRefused() throws Exception {
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
                    setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
                    Assert.assertEquals(mode, CommitMode.fromString(mode), engine.getConfiguration().getCommitMode());
                    final String r = "r_" + mode;
                    try (
                            LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
                            LineUdpLexer lexer = new LineUdpLexer(4096)
                    ) {
                        lexer.withParser(parser);
                        // The parser finds no table at the measurement name and creates it at the line end.
                        parseDatagram(lexer, r + ",s=a v=1i 1704153600000000000");
                        execute("create table " + r + " (s symbol, v long, timestamp timestamp) timestamp(timestamp) partition by day wal");
                        parseDatagram(lexer, "\n" + r + ",s=b v=2i 1704157200000000000\n");
                        lexer.parseLast();
                        parser.commitAll();
                    }
                    Assert.assertTrue(mode, engine.verifyTableName(r).isWal());
                    assertQuery("select * from " + r)
                            .noLeakCheck()
                            .timestamp("timestamp")
                            .expectSize()
                            .returns("s\tv\ttimestamp\n");
                }
                capture.drain();
                for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
                    // A WAL table's token logs as its directory name, the table name with a ~id suffix.
                    capture.assertOnlyOnce("ILP over UDP cannot write to a WAL table.*table=r_" + mode + "~");
                }
            } finally {
                capture.stop();
            }
        });
    }

    // The parser writes through a TableWriter, outside the WAL, the sequencer and replication, so it refuses the
    // lines of a WAL table in every commit mode, with one error per table, and still writes the non-WAL tables of
    // the same datagrams. A table a line creates is non-WAL whatever cairo.wal.enabled.default says, so its lines
    // are written. The second datagram meets the refused table again between lines of two written tables: its
    // lines, and the columns they add, must not land in either.
    @Test
    public void testWalTablesRefused() throws Exception {
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
                for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
                    setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
                    Assert.assertEquals(mode, CommitMode.fromString(mode), engine.getConfiguration().getCommitMode());
                    Assert.assertTrue(mode, engine.getConfiguration().getWalEnabledDefault());
                    final String n = "n_" + mode;
                    final String w = "w_" + mode;
                    final String m = "m_" + mode;
                    final String p = "p_" + mode;
                    execute("create table " + n + " (s symbol, v long, ts timestamp) timestamp(ts) partition by day bypass wal");
                    execute("create table " + p + " (s symbol, v long, ts timestamp) timestamp(ts) partition by day bypass wal");
                    execute("create table " + w + " (s symbol, v long, ts timestamp) timestamp(ts) partition by day wal");
                    execute("insert into " + w + " values ('wal', 0, '2024-01-01')");
                    drainWalQueue();
                    // 2024-01-02, one line an hour; a line of the WAL table and one of the missing table add a column.
                    final String[] datagrams = {
                            n + ",s=a v=1i 1704153600000000000\n"
                                    + w + ",s=b v=2i 1704157200000000000\n"
                                    + w + ",s=c v=3i 1704160800000000000\n"
                                    + n + ",s=d v=4i 1704164400000000000\n"
                                    + m + ",s=e v=5i 1704168000000000000\n"
                                    + m + ",s=f v=6i 1704171600000000000\n"
                                    + w + ",s=g v=7i,x=1i 1704175200000000000\n"
                                    + n + ",s=h v=8i 1704178800000000000\n",
                            n + ",s=i v=9i 1704182400000000000\n"
                                    + w + ",s=j v=10i 1704186000000000000\n"
                                    + w + ",s=k v=11i,y=1i 1704189600000000000\n"
                                    + p + ",s=l v=12i 1704193200000000000\n"
                                    + m + ",s=m v=13i 1704196800000000000\n"
                                    + m + ",s=n v=14i,z=1i 1704200400000000000\n"
                                    + n + ",s=o v=15i 1704204000000000000\n"
                    };
                    try (
                            LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
                            LineUdpLexer lexer = new LineUdpLexer(4096)
                    ) {
                        lexer.withParser(parser);
                        for (String datagram : datagrams) {
                            parseDatagram(lexer, datagram);
                            lexer.parseLast();
                            parser.commitAll();
                        }
                    }

                    assertQuery("select * from " + n)
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    s\tv\tts
                                    a\t1\t2024-01-02T00:00:00.000000Z
                                    d\t4\t2024-01-02T03:00:00.000000Z
                                    h\t8\t2024-01-02T07:00:00.000000Z
                                    i\t9\t2024-01-02T08:00:00.000000Z
                                    o\t15\t2024-01-02T14:00:00.000000Z
                                    """);
                    assertQuery("select * from " + p)
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    s\tv\tts
                                    l\t12\t2024-01-02T11:00:00.000000Z
                                    """);
                    assertQuery("select * from " + w)
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    s\tv\tts
                                    wal\t0\t2024-01-01T00:00:00.000000Z
                                    """);
                    Assert.assertFalse(mode, engine.verifyTableName(m).isWal());
                    assertQuery("select * from " + m)
                            .noLeakCheck()
                            .timestamp("timestamp")
                            .expectSize()
                            .returns("""
                                    s\tv\ttimestamp\tz
                                    e\t5\t2024-01-02T04:00:00.000000Z\tnull
                                    f\t6\t2024-01-02T05:00:00.000000Z\tnull
                                    m\t13\t2024-01-02T12:00:00.000000Z\tnull
                                    n\t14\t2024-01-02T13:00:00.000000Z\t1
                                    """);
                }
                capture.drain();
                for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
                    // A WAL table's token logs as its directory name, the table name with a ~id suffix.
                    capture.assertOnlyOnce("ILP over UDP cannot write to a WAL table.*table=w_" + mode + "~");
                    capture.assertNotLogged("convert the table to non-WAL [table=m_" + mode);
                }
            } finally {
                capture.stop();
            }
        });
    }

    private static void parseDatagram(LineUdpLexer lexer, String text) {
        final byte[] bytes = text.getBytes(Files.UTF_8);
        final long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(mem + i, bytes[i]);
            }
            lexer.parse(mem, mem + bytes.length);
        } finally {
            Unsafe.free(mem, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static LineUdpReceiverConfiguration udpConfiguration(boolean useLegacyString) {
        return new DefaultLineUdpReceiverConfiguration() {
            @Override
            public boolean isUseLegacyStringDefault() {
                return useLegacyString;
            }
        };
    }

    // a quoted string is a VARCHAR on a server and a STRING with the legacy setting, so the lines run
    // against a fresh table once with each
    private void assertForEachStringDefault(String expected, String lines, TableModel model) throws Exception {
        for (boolean useLegacyString : new boolean[]{false, true}) {
            AbstractCairoTest.create(model);
            assertThat(expected, lines, model.getTableName(), configuration, udpConfiguration(useLegacyString));
            execute("DROP TABLE " + model.getTableName());
        }
    }

    private void assertMultiTable(String expected1, String expected2, String lines) throws Exception {
        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                try {
                    return new TestMicroClock(MicrosFormatUtils.parseTimestamp("2017-10-03T10:00:00.000Z"), 10);
                } catch (NumericException e) {
                    throw CairoException.critical(0).put("numeric");
                }
            }
        };

        assertThat(expected1, lines, "x", configuration);
        assertTable(expected2, "y");
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        refreshTablesInBaseEngine();
        try (
                TableReader reader = newOffPoolReader(configuration, tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            assertCursorTwoPass(expected, cursor, reader.getMetadata());
        }
    }

    private void assertThat(String expected, String lines, CharSequence tableName, CairoConfiguration configuration, LineUdpReceiverConfiguration udpConfiguration) throws Exception {
        assertMemoryLeak(() -> {
            parse(lines, configuration, udpConfiguration);
            assertTable(expected, tableName);
        });
    }

    private void assertThat(String expected, String lines, CharSequence tableName, CairoConfiguration configuration) throws Exception {
        assertThat(expected, lines, tableName, configuration, new DefaultLineUdpReceiverConfiguration());
    }

    private void assertThat(String expected, String lines, CharSequence tableName) throws Exception {
        assertThat(expected, lines, tableName, configuration);
    }

    private void parse(String lines, CairoConfiguration configuration, LineUdpReceiverConfiguration udpConfiguration) {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            try (LineUdpParserImpl parser = new LineUdpParserImpl(engine, udpConfiguration)) {
                byte[] bytes = lines.getBytes(Files.UTF_8);
                int len = bytes.length;
                long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < len; i++) {
                        Unsafe.putByte(mem + i, bytes[i]);
                    }
                    try (LineUdpLexer lexer = new LineUdpLexer(4096)) {
                        lexer.withParser(parser);
                        lexer.parse(mem, mem + len);
                        lexer.parseLast();
                        parser.commitAll();
                    }
                } finally {
                    Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        }
    }

    private void testAddColumnFloat(short colType, String expected) throws Exception {
        final String lines = """
                tab,tag=abc,tag2=xyz field=100i,f4=9.034,field2="str",fx=true 100000000000
                tab,tag=woopsie,tag2=daisy field=127i,f4=3.08891,field2="comment",fx=true 100000000000
                tab,tag=444,tag2=d555 field=110i,f4=1.4,f5=55,field2="comment",fx=true 100000000000
                tab,tag=666,tag2=777 field=40i,f4=1.1,field2="comment\\ X",fx=false 100000000000
                """;

        assertThat(
                expected,
                lines,
                "tab",
                configuration,
                new DefaultLineUdpReceiverConfiguration() {
                    @Override
                    public short getDefaultColumnTypeForFloat() {
                        return colType;
                    }
                }
        );

        try (TableReader reader = newOffPoolReader(configuration, "tab")) {
            Assert.assertEquals(colType, reader.getMetadata().getColumnType("f5"));
        }
    }

    private void testAddColumnInteger(short colType, String nullValue) throws Exception {
        final String expected = "tag\ttag2\tfield\tf4\tfield2\tfx\ttimestamp\tf5\n" +
                "abc\txyz\t100\t9.034\tstr\ttrue\t1970-01-01T00:01:40.000000Z\t" + nullValue + "\n" +
                "woopsie\tdaisy\t127\t3.08891\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\t" + nullValue + "\n" +
                "444\td555\t110\t1.4\tcomment\ttrue\t1970-01-01T00:01:40.000000Z\t55\n" +
                "666\t777\t40\t1.1\tcomment\\ X\tfalse\t1970-01-01T00:01:40.000000Z\t" + nullValue + "\n";

        final String lines = """
                tab,tag=abc,tag2=xyz field=100i,f4=9.034,field2="str",fx=true 100000000000
                tab,tag=woopsie,tag2=daisy field=127i,f4=3.08891,field2="comment",fx=true 100000000000
                tab,tag=444,tag2=d555 field=110i,f4=1.4,f5=55i,field2="comment",fx=true 100000000000
                tab,tag=666,tag2=777 field=40i,f4=1.1,field2="comment\\ X",fx=false 100000000000
                """;

        assertThat(expected, lines, "tab", configuration, new DefaultLineUdpReceiverConfiguration() {
            @Override
            public short getDefaultColumnTypeForInteger() {
                return colType;
            }
        });

        refreshTablesInBaseEngine();
        try (TableReader reader = newOffPoolReader(configuration, "tab")) {
            Assert.assertEquals(colType, reader.getMetadata().getColumnType("f5"));
        }
    }

    // passes the lexer's events to a parser and counts the parser's reads of earlier tokens of the line
    private static class CacheReadCountingParser implements LineUdpParser {
        private final CharSequenceCache countingCache;
        private final LineUdpParser parser;
        private CharSequenceCache cache;
        private long reads;

        private CacheReadCountingParser(LineUdpParser parser) {
            this.parser = parser;
            this.countingCache = address -> {
                reads++;
                return cache.get(address);
            };
        }

        @Override
        public void onError(int position, int state, int code) {
            parser.onError(position, state, code);
        }

        @Override
        public void onEvent(CachedCharSequence token, int type, CharSequenceCache cache) {
            this.cache = cache;
            parser.onEvent(token, type, countingCache);
        }

        @Override
        public void onLineEnd(CharSequenceCache cache) {
            this.cache = cache;
            parser.onLineEnd(countingCache);
        }
    }
}
