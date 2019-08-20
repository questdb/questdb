/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine.functions.regex;

import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.AbstractGriffinTest;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
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

            try (RecordCursorFactory factory = compiler.compile("select * from x where name ~= 'XJ'")) {
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