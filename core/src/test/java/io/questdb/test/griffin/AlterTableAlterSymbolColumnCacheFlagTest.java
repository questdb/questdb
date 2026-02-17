/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableAlterSymbolColumnCacheFlagTest extends AbstractCairoTest {

    @Test
    public void testAlterExpectColumnKeyword() throws Exception {
        assertFailure("alter table x alter", 19, "'column' expected");
    }

    @Test
    public void testAlterExpectColumnName() throws Exception {
        assertFailure("alter table x alter column", 26, "column name expected");
    }

    @Test
    public void testAlterFlagInNonSymbolColumn() throws Exception {
        assertFailure("alter table x alter column b cache", 27, "cache is only supported for symbol type");
    }

    @Test
    public void testAlterSymbolCacheFlagToFalseAndCheckOpenReaderWithCursor() throws Exception {
        String expectedOrdered = "sym\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "googl\n" +
                "ibm\n" +
                "ibm\n" +
                "msft\n" +
                "msft\n";

        String expectedChronological = "sym\tk\n" +
                "msft\t1970-01-01T00:00:00.000000Z\n" +
                "googl\t1970-01-01T00:16:40.000000Z\n" +
                "googl\t1970-01-01T00:33:20.000000Z\n" +
                "ibm\t1970-01-01T00:50:00.000000Z\n" +
                "googl\t1970-01-01T01:06:40.000000Z\n" +
                "ibm\t1970-01-01T01:23:20.000000Z\n" +
                "googl\t1970-01-01T01:40:00.000000Z\n" +
                "googl\t1970-01-01T01:56:40.000000Z\n" +
                "googl\t1970-01-01T02:13:20.000000Z\n" +
                "msft\t1970-01-01T02:30:00.000000Z\n";

        assertMemoryLeak(() -> {
            createX();

            assertQueryNoLeakCheck(expectedOrdered,
                    "select sym from x order by sym"
            );

            assertSql(expectedChronological, "select sym, k from x");

            try (TableWriter writer = getWriter("x")) {
                writer.changeCacheFlag(1, false);
            }

            assertSql(expectedChronological, "select sym, k from x");

            assertQueryNoLeakCheck(expectedOrdered,
                    "select sym from x order by 1 asc"
            );
        });
    }

    @Test
    public void testAlterSymbolCacheFlagToTrueCheckOpenReaderWithCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i int, sym symbol nocache) ;");
            execute("insert into x values (1, 'GBP')");
            execute("insert into x values (2, 'CHF')");
            execute("insert into x values (3, 'GBP')");
            execute("insert into x values (4, 'JPY')");
            execute("insert into x values (5, 'USD')");
            execute("insert into x values (6, 'GBP')");
            execute("insert into x values (7, 'GBP')");
            execute("insert into x values (8, 'GBP')");
            execute("insert into x values (9, 'GBP')");

            String expectedOrdered = "sym\n" +
                    "CHF\n" +
                    "GBP\n" +
                    "GBP\n" +
                    "GBP\n" +
                    "GBP\n" +
                    "GBP\n" +
                    "GBP\n" +
                    "JPY\n" +
                    "USD\n";

            String expectedChronological = "i\tsym\n" +
                    "1\tGBP\n" +
                    "2\tCHF\n" +
                    "3\tGBP\n" +
                    "4\tJPY\n" +
                    "5\tUSD\n" +
                    "6\tGBP\n" +
                    "7\tGBP\n" +
                    "8\tGBP\n" +
                    "9\tGBP\n";

            assertSql(
                    expectedOrdered,
                    "select sym from x order by sym"
            );

            assertSql(
                    expectedChronological,
                    "select i, sym from x"
            );

            try (TableWriter writer = getWriter("x")) {
                writer.changeCacheFlag(1, true);
            }

            assertSql(
                    expectedChronological,
                    "select i, sym from x"
            );

            assertQueryNoLeakCheck(
                    expectedOrdered,
                    "select sym from x order by 1 asc"
            );
        });
    }

    @Test
    public void testBadSyntax() throws Exception {
        assertFailure("alter table x alter column c", 28, "'add index' or 'drop index' or 'type' or 'cache' or 'nocache' or 'symbol' expected");
    }

    @Test
    public void testInvalidColumn() throws Exception {
        assertFailure("alter table x alter column y cache", 27, "column 'y' does not exist in table 'x'");
    }

    @Test
    public void testWhenCacheOrNocacheAreNotInAlterStatement() throws Exception {
        assertFailure("alter table x alter column c ca", 29, "'cache' or 'nocache' expected");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                execute(sql);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void createX() throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
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
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp);"
        );
    }
}
