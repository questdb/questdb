/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.UpdateStatement;
import org.junit.Assert;
import org.junit.Test;

public class UpdateBasicTest extends AbstractGriffinTest {
    @Test
    public void testUpdateIdentical() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = x WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t3\n" +
                    "1970-01-01T00:00:03.000000Z\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateNoFilter() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t1\n" +
                    "1970-01-01T00:00:02.000000Z\t1\n" +
                    "1970-01-01T00:00:03.000000Z\t1\n" +
                    "1970-01-01T00:00:04.000000Z\t1\n");
        });
    }

    @Test
    public void testUpdateWithFilterAndFunction() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x," +
                    " x as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 10L * x WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t20\n" +
                    "1970-01-01T00:00:02.000000Z\t3\t30\n" +
                    "1970-01-01T00:00:03.000000Z\t4\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\t5\n");
        });
    }

    @Test
    public void testUpdateWithFilterAndFunctionValueUpcast() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x," +
                    " x as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 10 * x WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t20\n" +
                    "1970-01-01T00:00:02.000000Z\t3\t30\n" +
                    "1970-01-01T00:00:03.000000Z\t4\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\t5\n");
        });
    }

    @Test
    public void testUpdateWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x * 100 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y + x" +
                    " FROM down " +
                    " WHERE up.ts = down.ts and x > 1 and x < 4");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t202\n" +
                    "1970-01-01T00:00:02.000000Z\t303\n" +
                    "1970-01-01T00:00:03.000000Z\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateWithJoinNoVirtual() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x * 100 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y" +
                    " FROM down " +
                    " WHERE up.ts = down.ts and x < 4");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t100\n" +
                    "1970-01-01T00:00:01.000000Z\t200\n" +
                    "1970-01-01T00:00:02.000000Z\t300\n" +
                    "1970-01-01T00:00:03.000000Z\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateWith2TableJoinInWithClause() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', null) s," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down1 (s symbol index, y int)", sqlExecutionContext);
            executeInsert("insert into down1 values ('a', 1)");
            executeInsert("insert into down1 values ('a', 2)");
            executeInsert("insert into down1 values ('b', 3)");
            executeInsert("insert into down1 values ('b', 4)");
            executeInsert("insert into down1 values (null, 5)");
            executeInsert("insert into down1 values (null, 6)");

            compiler.compile("create table  down2 (s symbol index, y long)", sqlExecutionContext);
            executeInsert("insert into down2 values ('a', 100)");
            executeInsert("insert into down2 values ('b', 300)");
            executeInsert("insert into down2 values (null, 500)");

            // Check what will be in WITH before JOIN in UPDATE
            assertSql("select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s",
                    "sm\ts\n" +
                            "101\ta\n" +
                            "102\ta\n" +
                            "303\tb\n" +
                            "304\tb\n" +
                            "505\t\n" +
                            "506\t\n");

            executeUpdate("WITH jn AS (select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s)" +
                    "UPDATE up SET x = sm" +
                    " FROM jn " +
                    " WHERE up.s = jn.s");

            assertSql("up",
                    "ts\ts\tx\n" +
                            "1970-01-01T00:00:00.000000Z\ta\t101\n" +
                            "1970-01-01T00:00:01.000000Z\ta\t101\n" +
                            "1970-01-01T00:00:02.000000Z\tb\t303\n" +
                            "1970-01-01T00:00:03.000000Z\t\t505\n" +
                            "1970-01-01T00:00:04.000000Z\t\t505\n");
        });
    }

    private void applyUpdate(UpdateStatement updateStatement) throws SqlException {
        if (updateStatement != UpdateStatement.EMPTY) {
            try (TableWriter tableWriter = engine.getWriter(
                    sqlExecutionContext.getCairoSecurityContext(),
                    updateStatement.getUpdateTableName(),
                    "UPDATE")) {

                tableWriter.executeUpdate(updateStatement, sqlExecutionContext);
            }
        }
    }

    private void executeUpdate(String query) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
            applyUpdate(updateStatement);
        }
    }
}
