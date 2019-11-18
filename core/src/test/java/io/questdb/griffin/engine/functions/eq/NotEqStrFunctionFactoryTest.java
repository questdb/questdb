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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class NotEqStrFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testSimple() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TJ\tCP\t0.084383207626\n" +
                "RX\tEH\t0.224523408561\n" +
                "ZS\t\t0.462183542913\n" +
                "BT\tPG\t0.422435666165\n" +
                "UD\tYY\t0.003598367215\n" +
                "HF\t\t0.977110314605\n" +
                "XY\tBE\t0.125030421903\n" +
                "SH\tUE\t0.891258753660\n" +
                "UL\t\t0.810161274171\n" +
                "TJ\t\t0.844525817721\n" +
                "YR\tBV\t0.490051044989\n" +
                "OO\tZV\t0.920500394699\n" +
                "YI\t\t0.228223359653\n" +
                "UI\tWE\t0.682166086100\n" +
                "UV\tDO\t0.540670984654\n" +
                "YY\t\t0.544915502152\n" +
                "LY\tWC\t0.713391027156\n" +
                "\tWD\t0.282002071667\n" +
                "\tHO\t0.255331933970\n" +
                "\tQB\t0.944165897553\n" +
                "VI\t\t0.579744709631\n" +
                "SU\tSR\t0.304525331063\n" +
                "\tSJ\t0.497651932297\n" +
                "HZ\tPI\t0.823124946199\n" +
                "OV\t\t0.589339848805\n" +
                "GL\tML\t0.324245626540\n" +
                "ZI\tNZ\t0.337470756550\n" +
                "MB\tZG\t0.282558271278\n" +
                "KF\tOP\t0.679756299095\n";

        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_str(2,2,1) a," +
                    " rnd_str(2,2,1) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(30)" +
                    ")", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("x where a <> b", sqlExecutionContext).getRecordCursorFactory()) {
                sink.clear();

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }

                TestUtils.assertEquals(expected, sink);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }


    @Test
    public void testStrEqualsConstant() throws Exception {
        final String expected = "a\tb\tc\n" +
                "RX\tEH\t0.224523408561\n" +
                "ZS\tUX\t0.421776884197\n" +
                "GP\tWF\t0.677856455884\n" +
                "EY\tQE\t0.524932106269\n" +
                "OW\tPD\t0.157866355996\n" +
                "EO\tOJ\t0.968742327694\n" +
                "ED\tQQ\t0.422813427274\n" +
                "JG\tTJ\t0.022965637513\n" +
                "RY\tFB\t0.001107536108\n";

        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10)" +
                    ")", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("x where a <> 'TJ'", sqlExecutionContext).getRecordCursorFactory()) {
                sink.clear();

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }

                TestUtils.assertEquals(expected, sink);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    @Test
    public void testStrEqualsConstant2() throws Exception {
        final String expected = "a\tb\tc\n" +
                "RX\tEH\t0.224523408561\n" +
                "ZS\tUX\t0.421776884197\n" +
                "GP\tWF\t0.677856455884\n" +
                "EY\tQE\t0.524932106269\n" +
                "OW\tPD\t0.157866355996\n" +
                "EO\tOJ\t0.968742327694\n" +
                "ED\tQQ\t0.422813427274\n" +
                "JG\tTJ\t0.022965637513\n" +
                "RY\tFB\t0.001107536108\n";

        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10)" +
                    ")", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("x where 'TJ' <> a", sqlExecutionContext).getRecordCursorFactory()) {
                sink.clear();

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }

                TestUtils.assertEquals(expected, sink);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    @Test
    public void testStrEqualsNull() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TJ\tCP\t0.084383207626\n" +
                "RX\tEH\t0.224523408561\n" +
                "ZS\tUX\t0.421776884197\n" +
                "GP\tWF\t0.677856455884\n" +
                "EY\tQE\t0.524932106269\n" +
                "OW\tPD\t0.157866355996\n" +
                "EO\tOJ\t0.968742327694\n" +
                "ED\tQQ\t0.422813427274\n" +
                "JG\tTJ\t0.022965637513\n" +
                "RY\tFB\t0.001107536108\n" +
                "GO\tZZ\t0.187697081573\n" +
                "MY\tCC\t0.404554697479\n" +
                "SE\tYY\t0.910141759290\n" +
                "OL\tXW\t0.494289051196\n" +
                "SU\tDS\t0.675250954711\n" +
                "HO\tNV\t0.894091712658\n";

        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_str(2,2,1) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(20)" +
                    ")", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("x where a <> null", sqlExecutionContext).getRecordCursorFactory()) {
                sink.clear();

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }

                TestUtils.assertEquals(expected, sink);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    @Test
    public void testStrEqualsNull2() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TJ\tCP\t0.084383207626\n" +
                "RX\tEH\t0.224523408561\n" +
                "ZS\tUX\t0.421776884197\n" +
                "GP\tWF\t0.677856455884\n" +
                "EY\tQE\t0.524932106269\n" +
                "OW\tPD\t0.157866355996\n" +
                "EO\tOJ\t0.968742327694\n" +
                "ED\tQQ\t0.422813427274\n" +
                "JG\tTJ\t0.022965637513\n" +
                "RY\tFB\t0.001107536108\n" +
                "GO\tZZ\t0.187697081573\n" +
                "MY\tCC\t0.404554697479\n" +
                "SE\tYY\t0.910141759290\n" +
                "OL\tXW\t0.494289051196\n" +
                "SU\tDS\t0.675250954711\n" +
                "HO\tNV\t0.894091712658\n";

        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_str(2,2,1) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(20)" +
                    ")", sqlExecutionContext);

            try (RecordCursorFactory factory = compiler.compile("x where null <> a", sqlExecutionContext).getRecordCursorFactory()) {
                sink.clear();

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }

                TestUtils.assertEquals(expected, sink);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }
}
