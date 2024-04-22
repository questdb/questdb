/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableChangeColumnTypeTest extends AbstractCairoTest {
    @Test
    public void testChangeStringToInt() throws Exception {
        assertFailure("alter table x alter column c type int", 34, "incompatible column type change [existing=STRING, new=INT]");
    }

    @Test
    public void testChangeStringToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type symbol", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from y",
                    "select c from x"
            );

            insert("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            assertSql("c\nabc\n", "select c from x limit -1");

            ddl("create table z as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type string", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from z",
                    "select c from x"

            );
        });
    }

    @Test
    public void testChangeStringToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type varchar", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from x",
                    "select c from y"
            );

            insert("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            assertSql("c\nabc\n", "select c from x limit -1");

            ddl("create table z as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type string", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from x",
                    "select c from z"
            );
        });
    }

    @Test
    public void testChangeTypePreservesColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select * from x)", sqlExecutionContext);
            ddl("alter table x alter column c type symbol", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select * from y limit 10",
                    "select * from x limit 10"
            );
        });
    }

    @Test
    public void testChangeVarcharStringSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select v from x)", sqlExecutionContext);

            Rnd rnd = new Rnd();
            for (int i = 0; i < 10; i++) {
                String type;
                switch (rnd.nextPositiveInt() % 3) {
                    case 0:
                        type = "string";
                        break;
                    case 1:
                        type = "varchar";
                        break;
                    default:
                        type = "symbol";
                        break;
                }

                ddl("alter table x alter column v type " + type, sqlExecutionContext);

                assertSqlCursorsConvertedStrings(
                        "select v from x",
                        "select v from y"
                );
            }
        });
    }

    @Test
    public void testChangeVarcharToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select v from x)", sqlExecutionContext);
            ddl("alter table x alter column v type symbol", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select v from x",
                    "select v from y"
            );

            insert("insert into x(v, timestamp) values('abc', now())", sqlExecutionContext);
            assertSql("v\nabc\n", "select v from x limit -1");

            ddl("create table z as (select v from x)", sqlExecutionContext);
            ddl("alter table x alter column v type varchar", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select v from x",
                    "select v from z"
            );
        });
    }

    @Test
    public void testColumnDoesNotExist() throws Exception {
        assertFailure("alter table x alter column non_existing", 27, "column 'non_existing' does not exists in table 'x'");
    }

    @Test
    public void testNewTypeInvalid() throws Exception {
        assertFailure("alter table x alter column c type abracadabra", 34, "invalid type");
    }

    @Test
    public void testNewTypeMissing() throws Exception {
        assertFailure("alter table x alter column c type", 33, "column type expected");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                ddl(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void createX() throws SqlException {
        ddl(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str(5,1024,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_varchar(5,64,2) v" +
                        " from long_sequence(1000)" +
                        ") timestamp (timestamp) PARTITION BY HOUR BYPASS WAL;"
        );
    }

    protected static void assertSqlCursorsConvertedStrings(CharSequence expectedSql, CharSequence actualSql) throws SqlException {
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    sqlCompiler,
                    sqlExecutionContext,
                    expectedSql,
                    actualSql,
                    LOG,
                    true
            );
        }
    }
}
