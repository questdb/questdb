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

package com.questdb.griffin;

import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AlterTableTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAddTwoColumns() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column mycol int, second symbol", bindVariableService));

                        assertQuery(
                                "c\tmycol\tsecond\n" +
                                        "XYZ\tNaN\t\n" +
                                        "ABC\tNaN\t\n" +
                                        "ABC\tNaN\t\n" +
                                        "XYZ\tNaN\t\n" +
                                        "\tNaN\t\n" +
                                        "CDE\tNaN\t\n" +
                                        "CDE\tNaN\t\n" +
                                        "ABC\tNaN\t\n" +
                                        "\tNaN\t\n" +
                                        "XYZ\tNaN\t\n",
                                "select c, mycol, second from x",
                                null,
                                true
                        );

                        Assert.assertEquals(0, engine.getBusyWriterCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                    }
                }
        );
    }

    @Test
    public void testExpectActionKeyword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                createX();
                compiler.compile("alter table x", bindVariableService);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(13, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'add' or 'drop' expected");
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("alter x", bindVariableService);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(6, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'table' expected");
            }
        });
    }

    @Test
    public void testExpectTableName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("alter table", bindVariableService);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(11, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table name expected");
            }
        });
    }

    @Test
    public void testHappyPath() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    try {
                        createX();

                        Assert.assertNull(compiler.compile("alter table x add column mycol int", bindVariableService));

                        assertQuery(
                                "c\tmycol\n" +
                                        "XYZ\tNaN\n" +
                                        "ABC\tNaN\n" +
                                        "ABC\tNaN\n" +
                                        "XYZ\tNaN\n" +
                                        "\tNaN\n" +
                                        "CDE\tNaN\n" +
                                        "CDE\tNaN\n" +
                                        "ABC\tNaN\n" +
                                        "\tNaN\n" +
                                        "XYZ\tNaN\n",
                                "select c, mycol from x",
                                null,
                                true
                        );

                        Assert.assertEquals(0, engine.getBusyWriterCount());
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                    } finally {
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                    }
                }
        );
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            try {
                Assert.assertNull(compiler.compile("alter table y", bindVariableService));
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "table 'y' does not");
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    private void createX() throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " to_int(x) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp)",
                bindVariableService
        );
    }
}
