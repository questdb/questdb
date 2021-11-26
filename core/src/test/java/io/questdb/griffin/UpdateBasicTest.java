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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.UpdateStatement;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UpdateBasicTest extends AbstractGriffinTest {
    @Test
    public void testUpdate2ColumnsWith2TableJoinInWithClause() throws Exception {
        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', null) s," +
                    " x," +
                    " x + 1 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            executeUpdate("WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y FROM down1 JOIN down2 ON down1.s = down2.s)" +
                    "UPDATE up SET x = sm, y = jn.y" +
                    " FROM jn " +
                    " WHERE up.s = jn.s");

            assertSql(
                    "up",
                    "ts\ts\tx\ty\n" +
                            "1970-01-01T00:00:00.000000Z\ta\t101\t100\n" +
                            "1970-01-01T00:00:01.000000Z\ta\t101\t100\n" +
                            "1970-01-01T00:00:02.000000Z\tb\t303\t300\n" +
                            "1970-01-01T00:00:03.000000Z\t\t505\t500\n" +
                            "1970-01-01T00:00:04.000000Z\t\t505\t500\n"
            );
        });
    }

    @Test
    public void testUpdateDifferentColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) xint," +
                    " cast(x as long) xlong," +
                    " cast(x as double) xdouble," +
                    " cast(x as short) xshort," +
                    " cast(x as byte) xbyte," +
                    " cast(x as char) xchar," +
                    " cast(x as date) xdate," +
                    " cast(x as float) xfloat," +
                    " cast(x as timestamp) xts, " +
                    " cast(x as boolean) xbool" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            // All combinations to update xint
            executeUpdateFails("UPDATE up SET xint = xdouble", 21, "inconvertible types: DOUBLE -> INT [from=, to=xint]");
            executeUpdateFails("UPDATE up SET xint = xlong", 21, "inconvertible types: LONG -> INT [from=, to=xint]");

            String expected = "ts\txint\txlong\txdouble\txshort\txbyte\txchar\txdate\txfloat\txts\txbool\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1\t1\t\u0001\t1970-01-01T00:00:00.001Z\t1.0000\t1970-01-01T00:00:00.000001Z\ttrue\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t2\t2.0\t2\t2\t\u0002\t1970-01-01T00:00:00.002Z\t2.0000\t1970-01-01T00:00:00.000002Z\tfalse\n";

            executeUpdate("UPDATE up SET xint=xshort");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xfloat=xint");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xfloat");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xlong");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xshort=xbyte");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xshort=xchar");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xlong");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xts");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xts=xdate");
            // above call modified data from micro to milli. Revert the data back
            executeUpdate("UPDATE up SET xts=xlong");
            assertSql("up", expected);

            // Update all at once
            executeUpdate("UPDATE up SET xint=xshort, xfloat=xint, xdouble=xfloat, xshort=xbyte, xlong=xts, xts=xlong");
            assertSql("up", expected);

            // Update without conversion
            executeUpdate("UPDATE up" +
                    " SET xint=up2.xint," +
                    " xfloat=up2.xfloat," +
                    " xdouble=up2.xdouble," +
                    " xshort=up2.xshort," +
                    " xlong=up2.xlong," +
                    " xts=up2.xts, " +
                    " xchar=up2.xchar, " +
                    " xbool=up2.xbool, " +
                    " xbyte=up2.xbyte " +
                    " FROM up up2 WHERE up.ts = up2.ts");
            assertSql("up", expected);
        });
    }

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
    public void testUpdateMultipartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY)) {
                tml.col("xint", ColumnType.INT)
                        .col("xsym", ColumnType.SYMBOL).indexed(true, 256)
                        .timestamp("ts");
                createPopulateTable(tml, 5, "2020-01-01", 2);
            }

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts > '2020-01-02T14'");
            assertSql(
                    "up",
                    "xint\txsym\tts\n" +
                            "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" +
                            "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" +
                            "3\t\t2020-01-02T04:47:59.400000Z\n" +
                            "-1000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                            "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n"    // Updated
            );

            executeUpdate("UPDATE up SET xint = -2000 WHERE ts > '2020-01-02T14' AND xsym = 'VTJW'");
            assertSql(
                    "up",
                    "xint\txsym\tts\n" +
                            "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" +
                            "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" +
                            "3\t\t2020-01-02T04:47:59.400000Z\n" +
                            "-2000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                            "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n"
            );
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
    public void testUpdateTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdateFails("UPDATE up SET ts = 1", 14, "Designated timestamp column cannot be updated");
        });
    }

    @Test
    public void testUpdateWith2TableJoinInWithClause() throws Exception {
        assertMemoryLeak(() -> {

            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', 'c', null) s," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            executeUpdate("WITH jn AS (select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s)" +
                    "UPDATE up SET x = sm" +
                    " FROM jn " +
                    " WHERE up.s = jn.s");

            assertSql("up",
                    "ts\ts\tx\n" +
                            "1970-01-01T00:00:00.000000Z\ta\t101\n" +
                            "1970-01-01T00:00:01.000000Z\tc\t2\n" +
                            "1970-01-01T00:00:02.000000Z\tb\t303\n" +
                            "1970-01-01T00:00:03.000000Z\t\t505\n" +
                            "1970-01-01T00:00:04.000000Z\tb\t303\n");
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
    public void testUpdateWithFullCrossJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" +
                    " (select x * 100 as y," +
                    " timestamp_sequence(0, 1000000) ts" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y" +
                    " FROM down " +
                    " WHERE up.x < 4;");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t100\n" +
                    "1970-01-01T00:00:01.000000Z\t100\n" +
                    "1970-01-01T00:00:02.000000Z\t100\n" +
                    "1970-01-01T00:00:03.000000Z\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
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
    public void testUpdateWithJoinAndPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" +
                    " (select x * 100 as y," +
                    " timestamp_sequence(0, 1000000) ts" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y" +
                    " FROM down " +
                    " WHERE up.ts = down.ts and up.x < down.y and up.x < 4 and down.y > 100;");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t200\n" +
                    "1970-01-01T00:00:02.000000Z\t300\n" +
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
    public void testUpdateWithJoinNotEquals() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x * 100 as x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            compiler.compile("create table down as" +
                    " (select x * 50 as y," +
                    " timestamp_sequence(0, 1000000) ts" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = y + 1" +
                    " FROM down " +
                    " WHERE up.x < down.y;");

            assertSql("up",
                    "ts\tx\n" +
                            "1970-01-01T00:00:00.000000Z\t151\n" +
                            "1970-01-01T00:00:01.000000Z\t251\n" +
                            "1970-01-01T00:00:02.000000Z\t300\n" +
                            "1970-01-01T00:00:03.000000Z\t400\n" +
                            "1970-01-01T00:00:04.000000Z\t500\n");
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

    private void createTablesToJoin(String createTableSql) throws SqlException {
        compiler.compile(createTableSql, sqlExecutionContext);

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

        // Check what will be in JOIN between down1 and down2
        assertSql("select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s",
                "sm\ts\n" +
                        "101\ta\n" +
                        "102\ta\n" +
                        "303\tb\n" +
                        "304\tb\n" +
                        "505\t\n" +
                        "506\t\n");
    }

    private void executeUpdate(String query) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
            applyUpdate(updateStatement);
        }
    }

    private void executeUpdateFails(String sql, int position, String reason) {
        try {
            executeUpdate(sql);
            Assert.fail();
        } catch (SqlException exception) {
            TestUtils.assertContains(exception.getFlyweightMessage(), reason);
            Assert.assertEquals(position, exception.getPosition());
        }
    }
}
