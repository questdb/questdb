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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MatchStrFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "name\n" +
                    "HZTCQXJOQ\n" +
                    "LXJNZ\n" +
                    "TXJBQVYTY\n" +
                    "XJSJ\n" +
                    "YMUJXJ\n" +
                    "MEJXJN\n" +
                    "PRXJOPHLL\n" +
                    "GYMXJ\n" +
                    "XJKL\n" +
                    "HQXVXJQ\n" +
                    "UIXJO\n" +
                    "VXJCPF\n" +
                    "SVXJHXBY\n" +
                    "ICFOQEVPXJ\n" +
                    "XJWJJSRNZL\n" +
                    "HXJULSPH\n" +
                    "IPCBXJG\n" +
                    "XJN\n";
            compiler.compile("create table x as (select rnd_str() name from long_sequence(2000))");

            try (RecordCursorFactory factory = compiler.compile("select * from x where name ~= 'XJ'").getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor()) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (select rnd_str() name from long_sequence(2000))");
            try {
                compiler.compile("select * from x where name ~= 'XJ**'");
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Dangling meta");
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    @Test
    public void testNullRegex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table x as (select rnd_str() name from long_sequence(2000))");
            try {
                compiler.compile("select * from x where name ~= null");
            } catch (SqlException e) {
                Assert.assertEquals(30, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "NULL regex");
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }
}