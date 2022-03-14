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
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.griffin.update.UpdateExecution;
import io.questdb.griffin.update.UpdateStatement;
import io.questdb.std.Misc;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UpdateTest extends AbstractGriffinTest {
    private UpdateExecution updateExecution;

    @Before
    public void setUpUpdates() {
        updateExecution = new UpdateExecution(configuration);
    }

    @After
    public void tearDownUpdate() {
        updateExecution = Misc.free(updateExecution);
    }

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
    public void testUpdateColumnsTypeMismatch() throws Exception {
        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', null) s," +
                    " x," +
                    " x + 1 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            try {
                executeUpdate("WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y " +
                        "                         FROM down1 JOIN down2 ON down1.s = down2.s" +
                        ")" +
                        "UPDATE up SET s = sm, y = jn.y" +
                        " FROM jn " +
                        " WHERE up.s = jn.s");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "inconvertible types: LONG -> SYMBOL");
            }
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
                    " cast(x as boolean) xbool," +
                    " cast(x as long256) xl256" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            // All combinations to update xint
            executeUpdateFails("UPDATE up SET xint = xdouble", 21, "inconvertible types: DOUBLE -> INT [from=, to=xint]");
            executeUpdateFails("UPDATE up SET xint = xlong", 21, "inconvertible types: LONG -> INT [from=, to=xint]");
            executeUpdateFails("UPDATE up SET xshort = xlong", 23, "inconvertible types: LONG -> SHORT [from=, to=xshort]");
            executeUpdateFails("UPDATE up SET xchar = xlong", 22, "inconvertible types: LONG -> CHAR [from=, to=xchar]");
            executeUpdateFails("UPDATE up SET xbyte = xlong", 22, "inconvertible types: LONG -> BYTE [from=, to=xbyte]");
            executeUpdateFails("UPDATE up SET xlong = xl256", 22, "inconvertible types: LONG256 -> LONG [from=, to=xlong]");
            executeUpdateFails("UPDATE up SET xl256 = xlong", 22, "inconvertible types: LONG -> LONG256 [from=, to=xl256]");
            executeUpdateFails("UPDATE up SET xchar = xlong", 22, "inconvertible types: LONG -> CHAR [from=, to=xchar]");

            String expected = "ts\txint\txlong\txdouble\txshort\txbyte\txchar\txdate\txfloat\txts\txbool\txl256\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1\t1\t\u0001\t1970-01-01T00:00:00.001Z\t1.0000\t1970-01-01T00:00:00.000001Z\ttrue\t0x01\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t2\t2.0\t2\t2\t\u0002\t1970-01-01T00:00:00.002Z\t2.0000\t1970-01-01T00:00:00.000002Z\tfalse\t0x02\n";

            executeUpdate("UPDATE up SET xint=xshort");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xint=xshort WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xshort");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xlong=xshort WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xchar");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xlong=xchar WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xfloat=xint");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xfloat=xint WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xfloat");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdouble=xfloat WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdouble=xlong WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xshort=xbyte");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xshort=xbyte WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xshort=xchar");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xshort=xchar WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xchar=xshort");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xchar=xshort WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xint=xchar");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xint=xchar WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xdouble=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdouble=xlong WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xlong=xts");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xlong=xts WHERE ts='1970-01-01'");
            assertSql("up", expected);

            executeUpdate("UPDATE up SET xts=xdate");
            // above call modified data from micro to milli. Revert the data back
            executeUpdate("UPDATE up SET xts=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xts=xlong WHERE ts='1970-01-01'");
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
                    " FROM up up2 " +
                    " WHERE up.ts = up2.ts AND up.ts = '1970-01-01'");
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
    public void testUpdateToNull() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n" +
                    "1970-01-01T00:00:02.000000Z\tNaN\n" +
                    "1970-01-01T00:00:03.000000Z\tNaN\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateTimestampToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET ts1 = '1970-02-01' WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tts1\n" +
                    "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\n" +
                    "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\n" +
                    "1970-01-01T00:00:02.000000Z\t1970-02-01T00:00:00.000000Z\n" +
                    "1970-01-01T00:00:03.000000Z\t1970-02-01T00:00:00.000000Z\n" +
                    "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\n");
        });
    }

    @Test
    public void testUpdateTimestampToSymbolLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1, " +
                    " cast(to_str(timestamp_sequence(1000000, 1000000), 'yyyy-MM-ddTHH:mm:ss.SSSz') as symbol) as sym" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET ts1 = sym WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tts1\tsym\n" +
                    "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:01.000Z\n" +
                    "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:02.000Z\n" +
                    "1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:03.000Z\n" +
                    "1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000Z\n" +
                    "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:05.000Z\n");
        });
    }

    @Test
    public void testUpdateGeohashToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(15) as geo3," +
                    " rnd_geohash(25) as geo5 " +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up SET geo3 = 'questdb', geo5 = 'questdb' WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tgeo3\tgeo5\n" +
                    "1970-01-01T00:00:00.000000Z\t9v1\t46swg\n" +
                    "1970-01-01T00:00:01.000000Z\tjnw\tzfuqd\n" +
                    "1970-01-01T00:00:02.000000Z\tque\tquest\n" +
                    "1970-01-01T00:00:03.000000Z\tque\tquest\n" +
                    "1970-01-01T00:00:04.000000Z\tmmt\t71ftm\n");
        });
    }

    @Test
    public void testUpdateGeoHashColumnToLowerPrecision() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(5) g1c," +
                    " rnd_geohash(15) g3c," +
                    " rnd_geohash(25) g5c," +
                    " rnd_geohash(35) g7c" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up " +
                    "SET " +
                    "g1c = cast('questdb' as geohash(7c)), " +
                    "g3c = cast('questdb' as geohash(7c)), " +
                    "g5c = cast('questdb' as geohash(7c)), " +
                    "g7c = cast('questdb' as geohash(7c)) " +
                    "WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tg1c\tg3c\tg5c\tg7c\n" +
                    "1970-01-01T00:00:00.000000Z\t9\t46s\tjnw97\tzfuqd3b\n" +
                    "1970-01-01T00:00:01.000000Z\th\twh4\ts2z2f\t1cjjwk6\n" +
                    "1970-01-01T00:00:02.000000Z\tq\tque\tquest\tquestdb\n" +
                    "1970-01-01T00:00:03.000000Z\tq\tque\tquest\tquestdb\n" +
                    "1970-01-01T00:00:04.000000Z\tx\t76u\tq0s5w\ts2vqs1b\n");
        });
    }

    @Test
    public void testUpdateGeoHashColumnToLowerPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(5) g1c," +
                    " rnd_geohash(15) g3c," +
                    " rnd_geohash(25) g5c," +
                    " rnd_geohash(35) g7c" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            executeUpdate("UPDATE up " +
                    "SET " +
                    "g1c = g7c, " +
                    "g3c = g7c, " +
                    "g5c = g7c, " +
                    "g7c = g7c " +
                    "WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tg1c\tg3c\tg5c\tg7c\n" +
                    "1970-01-01T00:00:00.000000Z\t9\t46s\tjnw97\tzfuqd3b\n" +
                    "1970-01-01T00:00:01.000000Z\th\twh4\ts2z2f\t1cjjwk6\n" +
                    "1970-01-01T00:00:02.000000Z\tq\tq4s\tq4s2x\tq4s2xyt\n" +
                    "1970-01-01T00:00:03.000000Z\tb\tbuy\tbuyv3\tbuyv3pv\n" +
                    "1970-01-01T00:00:04.000000Z\tx\t76u\tq0s5w\ts2vqs1b\n");
        });
    }

    @Test
    public void testUpdateToBindVar() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            CompiledQuery cc = compiler.compile("UPDATE up SET x = $1 WHERE x > 1 and x < 4", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
            Assert.assertEquals(ColumnType.INT, sqlExecutionContext.getBindVariableService().getFunction(0).getType());

            sqlExecutionContext.getBindVariableService().setInt(0, 100);

            try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
                applyUpdate(updateStatement);
            }

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t100\n");
        });
    }

    @Test
    public void testUpdateWithBindVarInWhere() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            CompiledQuery cc = compiler.compile("UPDATE up SET x = 100 WHERE x < $1", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
            Assert.assertEquals(ColumnType.INT, sqlExecutionContext.getBindVariableService().getFunction(0).getType());

            sqlExecutionContext.getBindVariableService().setInt(0, 2);

            try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
                applyUpdate(updateStatement);
            }

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t100\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n");
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
    public void testUpdateMultiPartitionedTableSamePartitionManyFrames() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY)) {
                tml.col("xint", ColumnType.INT)
                        .col("xsym", ColumnType.SYMBOL).indexed(true, 256)
                        .timestamp("ts");
                createPopulateTable(tml, 10, "2020-01-01", 2);
            }

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T00;6h;12h;24'");
            assertSql(
                    "up",
                    "xint\txsym\tts\n" +
                            "-1000\tCPSW\t2020-01-01T04:47:59.900000Z\n" +
                            "2\tHYRX\t2020-01-01T09:35:59.800000Z\n" +
                            "-1000\t\t2020-01-01T14:23:59.700000Z\n" +
                            "4\tVTJW\t2020-01-01T19:11:59.600000Z\n" +
                            "5\tPEHN\t2020-01-01T23:59:59.500000Z\n" +
                            "-1000\t\t2020-01-02T04:47:59.400000Z\n" +
                            "7\tVTJW\t2020-01-02T09:35:59.300000Z\n" +
                            "-1000\t\t2020-01-02T14:23:59.200000Z\n" +
                            "9\tCPSW\t2020-01-02T19:11:59.100000Z\n" +
                            "10\t\t2020-01-02T23:59:59.000000Z\n"
            );

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T06;6h;12h;24' and xint > 7");
            assertSql(
                    "up",
                    "xint\txsym\tts\n" +
                            "-1000\tCPSW\t2020-01-01T04:47:59.900000Z\n" +
                            "2\tHYRX\t2020-01-01T09:35:59.800000Z\n" +
                            "-1000\t\t2020-01-01T14:23:59.700000Z\n" +
                            "4\tVTJW\t2020-01-01T19:11:59.600000Z\n" +
                            "5\tPEHN\t2020-01-01T23:59:59.500000Z\n" +
                            "-1000\t\t2020-01-02T04:47:59.400000Z\n" +
                            "7\tVTJW\t2020-01-02T09:35:59.300000Z\n" +
                            "-1000\t\t2020-01-02T14:23:59.200000Z\n" +
                            "-1000\tCPSW\t2020-01-02T19:11:59.100000Z\n" +
                            "-1000\t\t2020-01-02T23:59:59.000000Z\n"
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

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("alter table up drop column y", sqlExecutionContext);

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
    public void testUpdateOnAlteredTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(1))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("alter table up drop column y", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n");
        });
    }

    @Test
    public void testUpdateNoFilterOnAlteredTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(1))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);

            CompiledQuery cc = compiler.compile("UPDATE up SET x = 1", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());

            try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
                // Bump table version
                compile("alter table up add column y long", sqlExecutionContext);
                compile("alter table up drop column y", sqlExecutionContext);

                applyUpdate(updateStatement);
                Assert.fail();
            } catch (ReaderOutOfDateException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table='up'");
            }
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
    public void testUpdateTableWithoutDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdateFails("UPDATE up SET ts = 1", 7, "UPDATE query can only be executed on tables with designated timestamp");
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

    @Test
    public void testUpdateNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts)", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 123 WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t123\n" +
                    "1970-01-01T00:00:02.000000Z\t123\n" +
                    "1970-01-01T00:00:03.000000Z\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    private void applyUpdate(UpdateStatement updateStatement) throws SqlException {
        try (TableWriter tableWriter = engine.getWriter(
                sqlExecutionContext.getCairoSecurityContext(),
                updateStatement.getTableName(),
                "UPDATE")) {
            updateExecution.executeUpdate(tableWriter, updateStatement, sqlExecutionContext);
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
        cc.execute(null);
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
