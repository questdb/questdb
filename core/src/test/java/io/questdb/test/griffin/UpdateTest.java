/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

@RunWith(Parameterized.class)
public class UpdateTest extends AbstractGriffinTest {
    private static final long DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 300_000L;
    private final boolean walEnabled;

    public UpdateTest(WalMode walMode) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public boolean checkConnection() {
                return false;
            }
        }, MemoryTag.NATIVE_DEFAULT) {
        };
        circuitBreaker.setTimeout(DEFAULT_CIRCUIT_BREAKER_TIMEOUT);
        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testInsertAfterFailedUpdate() throws Exception {
        testInsertAfterFailed(false);
    }

    @Test
    public void testInsertAfterFailedUpdateWriterReopened() throws Exception {
        testInsertAfterFailed(true);
    }

    @Test
    public void testInsertAfterUpdate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 1");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t1\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t3\t1\t3\n" +
                    "1970-01-01T00:00:03.000000Z\t4\t1\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\t1\t5\n");

            executeUpdate("UPDATE up SET z = 2");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t2\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t1\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t3\t1\t2\n" +
                    "1970-01-01T00:00:03.000000Z\t4\t1\t2\n" +
                    "1970-01-01T00:00:04.000000Z\t5\t1\t2\n");

            executeUpdate("UPDATE up SET v = 33");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:01.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:03.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:04.000000Z\t33\t1\t2\n");

            compile("INSERT INTO up VALUES('1970-01-01T00:00:05.000000Z', 10.0, 10.0, 10.0)");
            compile("INSERT INTO up VALUES('1970-01-01T00:00:06.000000Z', 100.0, 100.0, 100.0)");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:01.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:03.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:04.000000Z\t33\t1\t2\n" +
                    "1970-01-01T00:00:05.000000Z\t10\t10\t10\n" +
                    "1970-01-01T00:00:06.000000Z\t100\t100\t100\n");
        });
    }

    @Test
    public void testNoRowsUpdated() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""),
                    sqlExecutionContext
            );

            executeUpdate("UPDATE up SET x = 1 WHERE x > 10");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t2\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t3\t3\t3\n" +
                    "1970-01-01T00:00:03.000000Z\t4\t4\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\t5\t5\n");
        });
    }

    @Test
    public void testNoRowsUpdated_TrivialNotEqualsFilter() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""),
                    sqlExecutionContext
            );

            executeUpdate("UPDATE up SET x = 1 WHERE 1 != 1");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\t2\t2\n" +
                    "1970-01-01T00:00:02.000000Z\t3\t3\t3\n" +
                    "1970-01-01T00:00:03.000000Z\t4\t4\t4\n" +
                    "1970-01-01T00:00:04.000000Z\t5\t5\t5\n");
        });
    }

    @Test
    public void testStringToIpv4() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(case when x = 1 then null else rnd_ipv4() end as string) as str," +
                            " cast(null as ipv4) as ip " +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET ip = str");

            String data = "ts\tstr\tip\n" +
                    "1970-01-01T00:00:00.000000Z\t\t\n" +
                    "1970-01-01T00:00:01.000000Z\t187.139.150.80\t187.139.150.80\n" +
                    "1970-01-01T00:00:02.000000Z\t18.206.96.238\t18.206.96.238\n" +
                    "1970-01-01T00:00:03.000000Z\t92.80.211.65\t92.80.211.65\n" +
                    "1970-01-01T00:00:04.000000Z\t212.159.205.29\t212.159.205.29\n";
            assertSql("up", data);

            executeUpdate("UPDATE up set str = 'abc'");
            executeUpdate("UPDATE up set str = ip");
            assertSql("up", data);
        });
    }

    @Test
    public void testSymbolIndexCopyOnWrite() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)" +
                    "), index(symCol) timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);
            assertSql("up",
                    "symCol\tts\tx\n" +
                            "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                            "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                            "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                            "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                            "\t1970-01-01T00:00:04.000000Z\t5\n"
            );

            CompiledQuery cq = compiler.compile("up where symCol = 'WCP'", sqlExecutionContext);
            try (RecordCursorFactory cursorFactory = cq.getRecordCursorFactory()) {
                try (RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)) {

                    executeUpdate("update up set symCol = null");
                    // Index is updated
                    assertSql("up where symCol = null",
                            "symCol\tts\tx\n" +
                                    "\t1970-01-01T00:00:00.000000Z\t1\n" +
                                    "\t1970-01-01T00:00:01.000000Z\t2\n" +
                                    "\t1970-01-01T00:00:02.000000Z\t3\n" +
                                    "\t1970-01-01T00:00:03.000000Z\t4\n" +
                                    "\t1970-01-01T00:00:04.000000Z\t5\n"
                    );

                    // Old index is still working
                    assertCursor("symCol\tts\tx\n" +
                            "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                            "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                            "WCP\t1970-01-01T00:00:02.000000Z\t3\n", cursor, cursorFactory.getMetadata(), true);
                }
            }
        });
    }

    @Test
    public void testSymbolIndexRebuiltOnAffectedPartitionsOnly() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean remove(LPSZ name) {
                    if (Chars.contains(name, "1970-01-01")) {
                        return false;
                    }
                    return super.remove(name);
                }
            };
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 60*60*1000000L) ts," +
                    " x" +
                    " from long_sequence(5)" +
                    "), index(symCol) timestamp(ts) Partition by hour" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            CompiledQuery cq = compiler.compile("up where symCol = 'WCP'", sqlExecutionContext);
            try (RecordCursorFactory cursorFactory = cq.getRecordCursorFactory()) {
                try (RecordCursor ignored = cursorFactory.getCursor(sqlExecutionContext)) {

                    executeUpdate("update up set symCol = null where ts >= '1970-01-01T03'");
                    // Index is updated
                    assertSql("up",
                            "symCol\tts\tx\n" +
                                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                                    "WCP\t1970-01-01T01:00:00.000000Z\t2\n" +
                                    "WCP\t1970-01-01T02:00:00.000000Z\t3\n" +
                                    "\t1970-01-01T03:00:00.000000Z\t4\n" +
                                    "\t1970-01-01T04:00:00.000000Z\t5\n"
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolIndexRebuiltOnColumnWithTopOverwrittenInO3() throws Exception {
        assertMemoryLeak(() -> {
            // Fill every second min from 00:00 to 02:30
            compiler.compile(
                    "create table symInd as" +
                            " (select " +
                            "timestamp_sequence(0, 2*60*1000000L) ts," +
                            " x" +
                            " from long_sequence(75)" +
                            ") timestamp(ts) Partition by hour" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            // Add indexed column in last partition
            compile("alter table symInd add column sym_index symbol index");

            // More data in order
            compiler.compile(
                    "insert into symInd " +
                            " select " +
                            " timestamp_sequence('1970-01-01T02:30', 60*1000000L) ts," +
                            " x," +
                            " cast(x as symbol)" +
                            " from long_sequence(45)", sqlExecutionContext
            );

            // O3 data in the first partition
            compiler.compile(
                    "insert into symInd " +
                            " select " +
                            " timestamp_sequence(1, 2 * 60*1000000L) ts," +
                            " x," +
                            " cast(x as symbol)" +
                            " from long_sequence(20)", sqlExecutionContext
            );

            // Update column to itself. Should rebuild whole index
            executeUpdate("update symInd set sym_index = sym_index");

            assertSql(
                    "select count(), min(ts), max(ts) from symInd where sym_index = null",
                    "count\tmin\tmax\n" +
                            "75\t1970-01-01T00:00:00.000000Z\t1970-01-01T02:28:00.000000Z\n"
            );

            for (int i = 0; i < 60; i += 10) {
                // Index is updated
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        "symInd where (sym_index || '') = '" + i + "'",
                        "symInd where sym_index = '" + i + "'",
                        LOG
                );
            }
        });
    }

    @Test
    public void testSymbolReplaceDistinctQueryIndexed() throws Exception {
        testSymbolsReplacedDistinct(true);
    }

    @Test
    public void testSymbolReplaceDistinctQueryNotIndexed() throws Exception {
        testSymbolsReplacedDistinct(false);
    }

    @Test
    public void testSymbolsIndexed_UpdateNull() throws Exception {
        testSymbols_UpdateNull(true);
    }

    @Test
    public void testSymbolsIndexed_UpdateWithExistingValue() throws Exception {
        testSymbol_UpdateWithExistingValue(true);
    }

    @Test
    public void testSymbolsIndexed_UpdateWithNewValue() throws Exception {
        testSymbols_UpdateWithNewValue(true);
    }

    @Test
    public void testSymbolsRolledBackOnFailedUpdate() throws Exception {
        //this test makes sense for non-WAL tables only, WAL tables behave differently
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Chars.endsWith(name, "s1.d.1") && Chars.contains(name, "1970-01-03")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                }
            };
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 24*60*60*1000000L) ts," +
                            " cast(x as int) v," +
                            " cast('a' as SYMBOL) s1, " +
                            " cast('b' as SYMBOL) s2 " +
                            " from long_sequence(5))" +
                            " ,index(s1) timestamp(ts) partition by DAY", sqlExecutionContext);

            try (TableWriter writer = getWriter("up")) {
                try {
                    CompiledQuery cq = compiler.compile("UPDATE up SET s1 = '11', s2 = '22'", sqlExecutionContext);
                    Assert.assertEquals(CompiledQuery.UPDATE, cq.getType());
                    try (OperationFuture fut = cq.execute(eventSubSequence)) {
                        writer.tick();
                        fut.await();
                    }
                    Assert.fail();
                } catch (SqlException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
                }

                try (TableReader reader = getReader("up")) {
                    Assert.assertEquals(1, reader.getSymbolMapReader(2).getSymbolCount());
                    Assert.assertEquals(1, reader.getSymbolMapReader(3).getSymbolCount());
                }

                try (TxReader txReader = new TxReader(ff)) {
                    TableToken tableToken = engine.verifyTableName("up");
                    txReader.ofRO(Path.getThreadLocal(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(), PartitionBy.DAY);
                    txReader.unsafeLoadAll();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(0));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(1));
                }
            }
        });
    }

    @Test
    public void testSymbols_UpdateNull() throws Exception {
        testSymbols_UpdateNull(false);
    }

    @Test
    public void testSymbols_UpdateWithExistingValue() throws Exception {
        testSymbol_UpdateWithExistingValue(false);
    }

    @Test
    public void testSymbols_UpdateWithNewValue() throws Exception {
        testSymbols_UpdateWithNewValue(false);
    }

    @Test
    public void testUpdate2ColumnsWith2TableJoinInWithClause() throws Exception {
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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

            assertSql("up", "ts\ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\ta\t101\t100\n" +
                    "1970-01-01T00:00:01.000000Z\ta\t101\t100\n" +
                    "1970-01-01T00:00:02.000000Z\tb\t303\t300\n" +
                    "1970-01-01T00:00:03.000000Z\t\t505\t500\n" +
                    "1970-01-01T00:00:04.000000Z\t\t505\t500\n");
        });
    }

    @Test
    public void testUpdateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table testUpdateAddedColumn as" +
                            " (select timestamp_sequence(0, 6*60*60*1000000L) ts," +
                            " cast(x - 1 as int) x" +
                            " from long_sequence(10))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""),
                    sqlExecutionContext
            );

            // Bump table version
            compile("alter table testUpdateAddedColumn add column y long", sqlExecutionContext);
            executeUpdate("UPDATE testUpdateAddedColumn SET y = x + 1 WHERE ts between '1970-01-01T12' and '1970-01-02T12'");

            assertSql("testUpdateAddedColumn", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t0\tNaN\n" +
                    "1970-01-01T06:00:00.000000Z\t1\tNaN\n" +
                    "1970-01-01T12:00:00.000000Z\t2\t3\n" +
                    "1970-01-01T18:00:00.000000Z\t3\t4\n" +
                    "1970-01-02T00:00:00.000000Z\t4\t5\n" +
                    "1970-01-02T06:00:00.000000Z\t5\t6\n" +
                    "1970-01-02T12:00:00.000000Z\t6\t7\n" +
                    "1970-01-02T18:00:00.000000Z\t7\tNaN\n" +
                    "1970-01-03T00:00:00.000000Z\t8\tNaN\n" +
                    "1970-01-03T06:00:00.000000Z\t9\tNaN\n");

            compile("alter table testUpdateAddedColumn drop column y");
            compile("alter table testUpdateAddedColumn add column y int");
            executeUpdate("UPDATE testUpdateAddedColumn SET y = COALESCE(y, x + 2) WHERE x%2 = 0");

            assertSql("testUpdateAddedColumn", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t0\t2\n" +
                    "1970-01-01T06:00:00.000000Z\t1\tNaN\n" +
                    "1970-01-01T12:00:00.000000Z\t2\t4\n" +
                    "1970-01-01T18:00:00.000000Z\t3\tNaN\n" +
                    "1970-01-02T00:00:00.000000Z\t4\t6\n" +
                    "1970-01-02T06:00:00.000000Z\t5\tNaN\n" +
                    "1970-01-02T12:00:00.000000Z\t6\t8\n" +
                    "1970-01-02T18:00:00.000000Z\t7\tNaN\n" +
                    "1970-01-03T00:00:00.000000Z\t8\t10\n" +
                    "1970-01-03T06:00:00.000000Z\t9\tNaN\n");

            compile("alter table testUpdateAddedColumn drop column x");
            executeUpdate("UPDATE testUpdateAddedColumn SET y = COALESCE(y, 1)");

            assertSql("testUpdateAddedColumn", "ts\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t2\n" +
                    "1970-01-01T06:00:00.000000Z\t1\n" +
                    "1970-01-01T12:00:00.000000Z\t4\n" +
                    "1970-01-01T18:00:00.000000Z\t1\n" +
                    "1970-01-02T00:00:00.000000Z\t6\n" +
                    "1970-01-02T06:00:00.000000Z\t1\n" +
                    "1970-01-02T12:00:00.000000Z\t8\n" +
                    "1970-01-02T18:00:00.000000Z\t1\n" +
                    "1970-01-03T00:00:00.000000Z\t10\n" +
                    "1970-01-03T06:00:00.000000Z\t1\n");
        });
    }

    @Test
    public void testUpdateAsyncMode() throws Exception {
        //this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        testUpdateAsyncMode(tableWriter -> {
                }, null,
                "ts\tx\n" +
                        "1970-01-01T00:00:00.000000Z\t1\n" +
                        "1970-01-01T00:00:01.000000Z\t123\n" +
                        "1970-01-01T00:00:02.000000Z\t123\n" +
                        "1970-01-01T00:00:03.000000Z\t4\n" +
                        "1970-01-01T00:00:04.000000Z\t5\n");
    }

    @Test
    public void testUpdateAsyncModeAddColumnInMiddle() throws Exception {
        //this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        testUpdateAsyncMode(tableWriter -> tableWriter.addColumn("newCol", ColumnType.INT), "cached query plan cannot be used because table schema has changed [table='up']",
                "ts\tx\tnewCol\n" +
                        "1970-01-01T00:00:00.000000Z\t1\tNaN\n" +
                        "1970-01-01T00:00:01.000000Z\t2\tNaN\n" +
                        "1970-01-01T00:00:02.000000Z\t3\tNaN\n" +
                        "1970-01-01T00:00:03.000000Z\t4\tNaN\n" +
                        "1970-01-01T00:00:04.000000Z\t5\tNaN\n");
    }

    @Test
    public void testUpdateAsyncModeFailed() throws Exception {
        //this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        final SqlExecutionContext oldContext = sqlExecutionContext;
        try {
            sqlExecutionContext = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public Rnd getAsyncRandom() {
                    throw new RuntimeException("test error");
                }
            }.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null);

            testUpdateAsyncMode(tableWriter -> {
                    }, "[43] test error",
                    "ts\tx\n" +
                            "1970-01-01T00:00:00.000000Z\t1\n" +
                            "1970-01-01T00:00:01.000000Z\t2\n" +
                            "1970-01-01T00:00:02.000000Z\t3\n" +
                            "1970-01-01T00:00:03.000000Z\t4\n" +
                            "1970-01-01T00:00:04.000000Z\t5\n");
        } finally {
            sqlExecutionContext = oldContext;
        }
    }

    @Test
    public void testUpdateAsyncModeRemoveColumnInMiddle() throws Exception {
        //this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        testUpdateAsyncMode(tableWriter -> tableWriter.removeColumn("x"), "cached query plan cannot be used because table schema has changed [table='up']",
                "ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:02.000000Z\n" +
                        "1970-01-01T00:00:03.000000Z\n" +
                        "1970-01-01T00:00:04.000000Z\n");
    }

    @Test
    public void testUpdateBinaryColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_bin(10, 20, 2) as bin1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET bin1 = cast(null as binary) WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql("up", "ts\tbin1\tlng2\n" +
                    "1970-01-01T00:00:00.000000Z\t00000000 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91\n" +
                    "00000010 3b 72 db f3\t1\n" +
                    "1970-01-01T06:00:00.000000Z\t00000000 c7 88 de a0 79 3c 77 15 68 61 26 af 19 c4 95 94\n" +
                    "00000010 36 53\t2\n" +
                    "1970-01-01T12:00:00.000000Z\t\t3\n" +
                    "1970-01-01T18:00:00.000000Z\t\t4\n" +
                    "1970-01-02T00:00:00.000000Z\t\t5\n" +
                    "1970-01-02T06:00:00.000000Z\t00000000 08 a1 1e 38 8d 1b 9e f4 c8 39 09 fe d8\t6\n" +
                    "1970-01-02T12:00:00.000000Z\t\t7\n" +
                    "1970-01-02T18:00:00.000000Z\t00000000 78 b5 b9 11 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b\n" +
                    "00000010 b1 3e e3 f1\t8\n" +
                    "1970-01-03T00:00:00.000000Z\t\t9\n" +
                    "1970-01-03T06:00:00.000000Z\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\t10\n");
        });
    }

    @Test
    public void testUpdateBinaryColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_bin(10, 20, 0) as bin1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            compile("alter table up add column bin2 binary", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_bin(10, 20, 0) as bin1," +
                    " x + 10 as lng2," +
                    " rnd_bin(10, 20, 0) as bin2" +
                    " from long_sequence(5))", sqlExecutionContext);
            executeUpdate("UPDATE up SET bin1 = cast(null as binary), bin2 = cast(null as binary) WHERE lng2 in (6,8,10,12,14)");

            assertSql("up", "ts\tbin1\tlng2\tbin2\n" +
                    "1970-01-01T00:00:00.000000Z\t00000000 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91\n" +
                    "00000010 3b 72 db f3\t1\t\n" +
                    "1970-01-01T06:00:00.000000Z\t00000000 c7 88 de a0 79 3c 77 15 68 61 26 af 19 c4 95 94\n" +
                    "00000010 36 53\t2\t\n" +
                    "1970-01-01T12:00:00.000000Z\t00000000 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09 fe\t3\t\n" +
                    "1970-01-01T18:00:00.000000Z\t00000000 30 78 36 6a 32 de e4 7c d2 35 07 42 fc 31 79\t4\t\n" +
                    "1970-01-02T00:00:00.000000Z\t00000000 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0\t5\t\n" +
                    "1970-01-02T06:00:00.000000Z\t\t6\t\n" +
                    "1970-01-02T12:00:00.000000Z\t00000000 ac 37 c8 cd 82 89 2b 4d 5f f6 46\t7\t\n" +
                    "1970-01-02T18:00:00.000000Z\t\t8\t\n" +
                    "1970-01-03T00:00:00.000000Z\t00000000 d2 85 7f a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0\t9\t\n" +
                    "1970-01-03T06:00:00.000000Z\t\t10\t\n" +
                    "1970-01-07T22:40:00.000000Z\t00000000 a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a e7\n" +
                    "00000010 0c 89\t11\t00000000 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9 73 93\n" +
                    "00000010 46 fe\n" +
                    "1970-01-08T04:40:00.000000Z\t\t12\t\n" +
                    "1970-01-08T10:40:00.000000Z\t00000000 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\t13\t00000000 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c\n" +
                    "1970-01-08T16:40:00.000000Z\t\t14\t\n" +
                    "1970-01-08T22:40:00.000000Z\t00000000 e4 35 e4 3a dc 5c 65 ff 27 67 77\t15\t00000000 52 d0 29 26 c5 aa da 18 ce 5f b2\n");
        });
    }

    @Test
    public void testUpdateBoolean() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) xint," +
                            " cast(x as boolean) xbool" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""),
                    sqlExecutionContext
            );

            assertSql("up", "ts\txint\txbool\n" +
                    "1970-01-01T00:00:00.000000Z\t1\ttrue\n" +
                    "1970-01-01T00:00:01.000000Z\t2\ttrue\n" +
                    "1970-01-01T00:00:02.000000Z\t3\ttrue\n" +
                    "1970-01-01T00:00:03.000000Z\t4\ttrue\n" +
                    "1970-01-01T00:00:04.000000Z\t5\ttrue\n");

            executeUpdate("UPDATE up SET xbool = false WHERE xint = 2");

            assertSql("up", "ts\txint\txbool\n" +
                    "1970-01-01T00:00:00.000000Z\t1\ttrue\n" +
                    "1970-01-01T00:00:01.000000Z\t2\tfalse\n" +
                    "1970-01-01T00:00:02.000000Z\t3\ttrue\n" +
                    "1970-01-01T00:00:03.000000Z\t4\ttrue\n" +
                    "1970-01-01T00:00:04.000000Z\t5\ttrue\n");
        });
    }

    @Test
    public void testUpdateColumnNameCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET X = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n" +
                    "1970-01-01T00:00:02.000000Z\tNaN\n" +
                    "1970-01-01T00:00:03.000000Z\tNaN\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateColumnsTypeMismatch() throws Exception {
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', null) s," +
                    " x," +
                    " x + 1 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            executeUpdateFails("WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y " +
                    "                         FROM down1 JOIN down2 ON down1.s = down2.s" +
                    ")" +
                    "UPDATE up SET s = sm, y = jn.y" +
                    " FROM jn " +
                    " WHERE jn.s = up.s", 147, "inconvertible types: LONG -> SYMBOL");
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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
                    "1970-01-01T00:00:01.000000Z\t2\t2\t2.0\t2\t2\t\u0002\t1970-01-01T00:00:00.002Z\t2.0000\t1970-01-01T00:00:00.000002Z\ttrue\t0x02\n";

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

            executeUpdate("UPDATE up SET xdate=xlong");
            assertSql("up", expected);
            executeUpdate("UPDATE up SET xdate=xlong WHERE ts='1970-01-01'");
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

            //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
            if (!walEnabled) {
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
            }
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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
    public void testUpdateGeohashColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            compile("alter table up add column geo1 geohash(1c)", sqlExecutionContext);
            compile("alter table up add column geo2 geohash(2c)", sqlExecutionContext);
            compile("alter table up add column geo4 geohash(5c)", sqlExecutionContext);
            compile("alter table up add column geo8 geohash(8c)", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x + 10 as lng2," +
                    " rnd_geohash(5) as geo1," +
                    " rnd_geohash(10) as geo2," +
                    " rnd_geohash(25) as geo4," +
                    " rnd_geohash(40) as geo8" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET geo1 = cast('q' as geohash(1c)), geo2 = 'qu', geo4='quest', geo8='questdb0' WHERE lng2 in (6, 8, 10, 12, 14)");

            assertSql("up", "ts\tstr1\tlng2\tgeo1\tgeo2\tgeo4\tgeo8\n" +
                    "1970-01-01T00:00:00.000000Z\t15\t1\t\t\t\t\n" +
                    "1970-01-01T06:00:00.000000Z\t15\t2\t\t\t\t\n" +
                    "1970-01-01T12:00:00.000000Z\t\t3\t\t\t\t\n" +
                    "1970-01-01T18:00:00.000000Z\t1\t4\t\t\t\t\n" +
                    "1970-01-02T00:00:00.000000Z\t1\t5\t\t\t\t\n" +
                    "1970-01-02T06:00:00.000000Z\t1\t6\tq\tqu\tquest\tquestdb0\n" +
                    "1970-01-02T12:00:00.000000Z\t190232\t7\t\t\t\t\n" +
                    "1970-01-02T18:00:00.000000Z\t\t8\tq\tqu\tquest\tquestdb0\n" +
                    "1970-01-03T00:00:00.000000Z\t15\t9\t\t\t\t\n" +
                    "1970-01-03T06:00:00.000000Z\t\t10\tq\tqu\tquest\tquestdb0\n" +
                    "1970-01-07T22:40:00.000000Z\t\t11\tn\tpn\t2gjm2\t7qgcr0y6\n" +
                    "1970-01-08T04:40:00.000000Z\t\t12\tq\tqu\tquest\tquestdb0\n" +
                    "1970-01-08T10:40:00.000000Z\t\t13\t8\t1y\tcd0fj\t5h18p8vz\n" +
                    "1970-01-08T16:40:00.000000Z\t\t14\tq\tqu\tquest\tquestdb0\n" +
                    "1970-01-08T22:40:00.000000Z\t\t15\t1\trc\t5vm2w\tz22qdyty\n");
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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
    public void testUpdateIdentical() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
    public void testUpdateMultiPartitionEmptyColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 25000000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            compile("alter table up add column y long", sqlExecutionContext);
            executeUpdate("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 6 or x = 8 or x = 13");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t1\tNaN\n" +
                    "1970-01-01T06:56:40.000000Z\t2\t42\n" +
                    "1970-01-01T13:53:20.000000Z\t3\tNaN\n" +
                    "1970-01-01T20:50:00.000000Z\t4\t42\n" +
                    "1970-01-02T03:46:40.000000Z\t5\tNaN\n" +
                    "1970-01-02T10:43:20.000000Z\t6\t42\n" +
                    "1970-01-02T17:40:00.000000Z\t7\tNaN\n" +
                    "1970-01-03T00:36:40.000000Z\t8\t42\n" +
                    "1970-01-03T07:33:20.000000Z\t9\tNaN\n" +
                    "1970-01-03T14:30:00.000000Z\t10\tNaN\n");
        });
    }

    @Test
    public void testUpdateMultiPartitionedTableSamePartitionManyFrames() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY)) {
                tml.col("xint", ColumnType.INT).col("xsym", ColumnType.SYMBOL).indexed(true, 256).timestamp("ts");
                if (walEnabled) {
                    tml.wal();
                }
                createPopulateTable(tml, 10, "2020-01-01", 2);
            }

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T00;6h;12h;24'");
            assertSql("up", "xint\txsym\tts\n" +
                    "-1000\tCPSW\t2020-01-01T04:47:59.900000Z\n" +
                    "2\tHYRX\t2020-01-01T09:35:59.800000Z\n" +
                    "-1000\t\t2020-01-01T14:23:59.700000Z\n" +
                    "4\tVTJW\t2020-01-01T19:11:59.600000Z\n" +
                    "5\tPEHN\t2020-01-01T23:59:59.500000Z\n" +
                    "-1000\t\t2020-01-02T04:47:59.400000Z\n" +
                    "7\tVTJW\t2020-01-02T09:35:59.300000Z\n" +
                    "-1000\t\t2020-01-02T14:23:59.200000Z\n" +
                    "9\tCPSW\t2020-01-02T19:11:59.100000Z\n" +
                    "10\t\t2020-01-02T23:59:59.000000Z\n");

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T06;6h;12h;24' and xint > 7");
            assertSql("up", "xint\txsym\tts\n" +
                    "-1000\tCPSW\t2020-01-01T04:47:59.900000Z\n" +
                    "2\tHYRX\t2020-01-01T09:35:59.800000Z\n" +
                    "-1000\t\t2020-01-01T14:23:59.700000Z\n" +
                    "4\tVTJW\t2020-01-01T19:11:59.600000Z\n" +
                    "5\tPEHN\t2020-01-01T23:59:59.500000Z\n" +
                    "-1000\t\t2020-01-02T04:47:59.400000Z\n" +
                    "7\tVTJW\t2020-01-02T09:35:59.300000Z\n" +
                    "-1000\t\t2020-01-02T14:23:59.200000Z\n" +
                    "-1000\tCPSW\t2020-01-02T19:11:59.100000Z\n" +
                    "-1000\t\t2020-01-02T23:59:59.000000Z\n");
        });
    }

    @Test
    public void testUpdateMultiPartitionsWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 25000000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(250000000000, 25000000000) ts," +
                    " cast(x as int) + 10 as x," +
                    " cast(x as long) * 10 as y" +
                    " from long_sequence(10))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 9 or x = 6 or x = 13 or x = 20");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t1\tNaN\n" +
                    "1970-01-01T06:56:40.000000Z\t2\t42\n" +
                    "1970-01-01T13:53:20.000000Z\t3\tNaN\n" +
                    "1970-01-01T20:50:00.000000Z\t4\t42\n" +
                    "1970-01-02T03:46:40.000000Z\t5\tNaN\n" +
                    "1970-01-02T10:43:20.000000Z\t6\t42\n" +
                    "1970-01-02T17:40:00.000000Z\t7\tNaN\n" +
                    "1970-01-03T00:36:40.000000Z\t8\tNaN\n" +
                    "1970-01-03T07:33:20.000000Z\t9\t42\n" +
                    "1970-01-03T14:30:00.000000Z\t10\tNaN\n" +
                    "1970-01-03T21:26:40.000000Z\t11\t10\n" +
                    "1970-01-04T04:23:20.000000Z\t12\t20\n" +
                    "1970-01-04T11:20:00.000000Z\t13\t42\n" +
                    "1970-01-04T18:16:40.000000Z\t14\t40\n" +
                    "1970-01-05T01:13:20.000000Z\t15\t50\n" +
                    "1970-01-05T08:10:00.000000Z\t16\t60\n" +
                    "1970-01-05T15:06:40.000000Z\t17\t70\n" +
                    "1970-01-05T22:03:20.000000Z\t18\t80\n" +
                    "1970-01-06T05:00:00.000000Z\t19\t90\n" +
                    "1970-01-06T11:56:40.000000Z\t20\t42\n");
        });
    }

    @Test
    public void testUpdateMultipartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY)) {
                tml.col("xint", ColumnType.INT).col("xsym", ColumnType.SYMBOL).indexed(true, 256).timestamp("ts");
                if (walEnabled) {
                    tml.wal();
                }
                createPopulateTable(tml, 5, "2020-01-01", 2);
            }

            executeUpdate("UPDATE up SET xint = -1000 WHERE ts > '2020-01-02T14'");
            assertSql("up", "xint\txsym\tts\n" +
                    "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" +
                    "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" +
                    "3\t\t2020-01-02T04:47:59.400000Z\n" +
                    "-1000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                    "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n"    // Updated
            );

            executeUpdate("UPDATE up SET xint = -2000 WHERE ts > '2020-01-02T14' AND xsym = 'VTJW'");
            assertSql("up", "xint\txsym\tts\n" +
                    "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" +
                    "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" +
                    "3\t\t2020-01-02T04:47:59.400000Z\n" +
                    "-2000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                    "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n");
        });
    }

    @Test
    public void testUpdateNoFilter() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
    public void testUpdateNoFilterOnAlteredTable() throws Exception {
        //this test makes sense for non-WAL tables only, WAL tables behave differently
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(1))" +
                    " timestamp(ts) partition by DAY", sqlExecutionContext);
            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n");

            CompiledQuery cc = compiler.compile("UPDATE up SET x = 2", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());

            try (UpdateOperation updateOperation = cc.getUpdateOperation()) {
                // Bump table version
                compile("alter table up add column y long", sqlExecutionContext);
                compile("alter table up drop column y", sqlExecutionContext);

                applyUpdate(updateOperation);
                Assert.fail();
            } catch (TableReferenceOutOfDateException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table='up'");
            }
        });
    }

    @Test
    public void testUpdateNonPartitionedTable() throws Exception {
        //this test makes sense for non-WAL tables only, WAL table has to be partitioned
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts)", sqlExecutionContext);

            CompiledQuery cq = compiler.compile("UPDATE up SET x = 123 WHERE x > 1 and x < 5", sqlExecutionContext);
            try (OperationFuture fut = cq.execute(eventSubSequence)) {
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, fut.getStatus());
                Assert.assertEquals(3, fut.getAffectedRowsCount());
            }

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t123\n" +
                    "1970-01-01T00:00:02.000000Z\t123\n" +
                    "1970-01-01T00:00:03.000000Z\t123\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateOnAlteredTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(1))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("alter table up drop column y", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 44");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t44\n");
        });
    }

    @Test
    public void testUpdateReadonlyFails() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            SqlExecutionContext roExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                    ReadOnlySecurityContext.INSTANCE,
                    bindVariableService,
                    null,
                    -1
                    , null
            );

            try {
                compiler.compile("UPDATE up SET x = x WHERE x > 1 and x < 4", roExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "permission denied");
            }
        });
    }

    @Test
    public void testUpdateSinglePartitionColumnTopAndAroundDense() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x - 1 as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(100000000, 1000000) ts," +
                    " cast(x - 1 as int) + 10 as x," +
                    " cast(x * 10 as long) as y" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 9 or x = 10 or x = 11");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t0\tNaN\n" +
                    "1970-01-01T00:00:01.000000Z\t1\tNaN\n" +
                    "1970-01-01T00:00:02.000000Z\t2\tNaN\n" +
                    "1970-01-01T00:00:03.000000Z\t3\tNaN\n" +
                    "1970-01-01T00:00:04.000000Z\t4\tNaN\n" +
                    "1970-01-01T00:00:05.000000Z\t5\tNaN\n" +
                    "1970-01-01T00:00:06.000000Z\t6\tNaN\n" +
                    "1970-01-01T00:00:07.000000Z\t7\tNaN\n" +
                    "1970-01-01T00:00:08.000000Z\t8\tNaN\n" +
                    "1970-01-01T00:00:09.000000Z\t9\t42\n" +
                    "1970-01-01T00:01:40.000000Z\t10\t42\n" +
                    "1970-01-01T00:01:41.000000Z\t11\t42\n" +
                    "1970-01-01T00:01:42.000000Z\t12\t30\n" +
                    "1970-01-01T00:01:43.000000Z\t13\t40\n" +
                    "1970-01-01T00:01:44.000000Z\t14\t50\n");
        });
    }

    @Test
    public void testUpdateSinglePartitionColumnTopAndAroundSparse() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x - 1 as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(100000000, 1000000) ts," +
                    " cast(x - 1 as int) + 10 as x," +
                    " cast(x * 10 as long) as y" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 5 or x = 7 or x = 10 or x = 13 or x = 14");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t0\tNaN\n" +
                    "1970-01-01T00:00:01.000000Z\t1\tNaN\n" +
                    "1970-01-01T00:00:02.000000Z\t2\tNaN\n" +
                    "1970-01-01T00:00:03.000000Z\t3\tNaN\n" +
                    "1970-01-01T00:00:04.000000Z\t4\tNaN\n" +
                    "1970-01-01T00:00:05.000000Z\t5\t42\n" +
                    "1970-01-01T00:00:06.000000Z\t6\tNaN\n" +
                    "1970-01-01T00:00:07.000000Z\t7\t42\n" +
                    "1970-01-01T00:00:08.000000Z\t8\tNaN\n" +
                    "1970-01-01T00:00:09.000000Z\t9\tNaN\n" +
                    "1970-01-01T00:01:40.000000Z\t10\t42\n" +
                    "1970-01-01T00:01:41.000000Z\t11\t20\n" +
                    "1970-01-01T00:01:42.000000Z\t12\t30\n" +
                    "1970-01-01T00:01:43.000000Z\t13\t42\n" +
                    "1970-01-01T00:01:44.000000Z\t14\t42\n");
        });
    }

    @Test
    public void testUpdateSinglePartitionEmptyColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 100000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);
            compile("alter table up add column y long", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 6 or x = 8 or x = 13");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t1\tNaN\n" +
                    "1970-01-01T00:01:40.000000Z\t2\t42\n" +
                    "1970-01-01T00:03:20.000000Z\t3\tNaN\n" +
                    "1970-01-01T00:05:00.000000Z\t4\t42\n" +
                    "1970-01-01T00:06:40.000000Z\t5\tNaN\n" +
                    "1970-01-01T00:08:20.000000Z\t6\t42\n" +
                    "1970-01-01T00:10:00.000000Z\t7\tNaN\n" +
                    "1970-01-01T00:11:40.000000Z\t8\t42\n" +
                    "1970-01-01T00:13:20.000000Z\t9\tNaN\n" +
                    "1970-01-01T00:15:00.000000Z\t10\tNaN\n");
        });
    }

    @Test
    public void testUpdateSinglePartitionGapAroundColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x - 1 as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            // Bump table version
            compile("alter table up add column y long", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(100000000, 1000000) ts," +
                    " cast(x - 1 as int) + 10 as x," +
                    " cast(x * 10 as long) as y" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET y = 42 where x = 6 or x = 8 or x = 12 or x = 14");

            assertSql("up", "ts\tx\ty\n" +
                    "1970-01-01T00:00:00.000000Z\t0\tNaN\n" +
                    "1970-01-01T00:00:01.000000Z\t1\tNaN\n" +
                    "1970-01-01T00:00:02.000000Z\t2\tNaN\n" +
                    "1970-01-01T00:00:03.000000Z\t3\tNaN\n" +
                    "1970-01-01T00:00:04.000000Z\t4\tNaN\n" +
                    "1970-01-01T00:00:05.000000Z\t5\tNaN\n" +
                    "1970-01-01T00:00:06.000000Z\t6\t42\n" +
                    "1970-01-01T00:00:07.000000Z\t7\tNaN\n" +
                    "1970-01-01T00:00:08.000000Z\t8\t42\n" +
                    "1970-01-01T00:00:09.000000Z\t9\tNaN\n" +
                    "1970-01-01T00:01:40.000000Z\t10\t10\n" +
                    "1970-01-01T00:01:41.000000Z\t11\t20\n" +
                    "1970-01-01T00:01:42.000000Z\t12\t42\n" +
                    "1970-01-01T00:01:43.000000Z\t13\t40\n" +
                    "1970-01-01T00:01:44.000000Z\t14\t42\n");
        });
    }

    @Test
    public void testUpdateString() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                            " (select" +
                            " rnd_str('foo','bar') as s," +
                            " timestamp_sequence(0, 1000000) ts," +
                            " x" +
                            " from long_sequence(5)) timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""),
                    sqlExecutionContext);

            // char
            executeUpdate("update up set s = 'a' where s = 'bar'");
            assertSql("up", "s\tts\tx\n" +
                    "foo\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "foo\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "a\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "a\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "a\t1970-01-01T00:00:04.000000Z\t5\n");

            // string
            executeUpdate("update up set s = 'baz' where s = 'a'");
            assertSql("up", "s\tts\tx\n" +
                    "foo\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "foo\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "baz\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "baz\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "baz\t1970-01-01T00:00:04.000000Z\t5\n");

            // UUID
            executeUpdate("update up set s = cast('11111111-1111-1111-1111-111111111111' as uuid) where s = 'baz'");
            assertSql("up", "s\tts\tx\n" +
                    "foo\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "foo\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateStringAndFixedColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = concat('questdb', str1), lng2 = -1 WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql("up", "ts\tstr1\tlng2\n" +
                    "1970-01-01T00:00:00.000000Z\t15\t1\n" +
                    "1970-01-01T06:00:00.000000Z\t15\t2\n" +
                    "1970-01-01T12:00:00.000000Z\tquestdb\t-1\n" +
                    "1970-01-01T18:00:00.000000Z\t1\t4\n" +
                    "1970-01-02T00:00:00.000000Z\tquestdb1\t-1\n" +
                    "1970-01-02T06:00:00.000000Z\t1\t6\n" +
                    "1970-01-02T12:00:00.000000Z\tquestdb190232\t-1\n" +
                    "1970-01-02T18:00:00.000000Z\t\t8\n" +
                    "1970-01-03T00:00:00.000000Z\tquestdb15\t-1\n" +
                    "1970-01-03T06:00:00.000000Z\t\t10\n");
        });
    }

    @Test
    public void testUpdateStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb' WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql("up", "ts\tstr1\tlng2\n" +
                    "1970-01-01T00:00:00.000000Z\t15\t1\n" +
                    "1970-01-01T06:00:00.000000Z\t15\t2\n" +
                    "1970-01-01T12:00:00.000000Z\tquestdb\t3\n" +
                    "1970-01-01T18:00:00.000000Z\t1\t4\n" +
                    "1970-01-02T00:00:00.000000Z\tquestdb\t5\n" +
                    "1970-01-02T06:00:00.000000Z\t1\t6\n" +
                    "1970-01-02T12:00:00.000000Z\tquestdb\t7\n" +
                    "1970-01-02T18:00:00.000000Z\t\t8\n" +
                    "1970-01-03T00:00:00.000000Z\tquestdb\t9\n" +
                    "1970-01-03T06:00:00.000000Z\t\t10\n");
        });
    }

    @Test
    public void testUpdateStringColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(100000)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb' WHERE ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1");
            assertSql("select count() from up where str1 = 'questdb'", "count\n" +
                    "7201\n");
            assertSql("select count() from up where ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1", "count\n" +
                    "7201\n");
        });
    }

    @Test
    public void testUpdateStringColumnUpdate1Value() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 30 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            compile("alter table up add column str2 string", sqlExecutionContext);

            compile("insert into up select * from " +
                    " (select timestamp_sequence('1970-01-01T00:30', 6 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x + 10 as lng2," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str2" +
                    " from long_sequence(10))", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb2', str2 = 'questdb2' WHERE ts = '1970-01-01T01:18:00.000000Z'");

            assertSql("up", "ts\tstr1\tlng2\tstr2\n" +
                    "1970-01-01T00:00:00.000000Z\t15\t1\t\n" +
                    "1970-01-01T00:03:00.000000Z\t15\t2\t\n" +
                    "1970-01-01T00:06:00.000000Z\t\t3\t\n" +
                    "1970-01-01T00:09:00.000000Z\t1\t4\t\n" +
                    "1970-01-01T00:12:00.000000Z\t1\t5\t\n" +
                    "1970-01-01T00:15:00.000000Z\t1\t6\t\n" +
                    "1970-01-01T00:18:00.000000Z\t190232\t7\t\n" +
                    "1970-01-01T00:21:00.000000Z\t\t8\t\n" +
                    "1970-01-01T00:24:00.000000Z\t15\t9\t\n" +
                    "1970-01-01T00:27:00.000000Z\t\t10\t\n" +
                    "1970-01-01T00:30:00.000000Z\t\t11\t190232\n" +
                    "1970-01-01T00:36:00.000000Z\t\t12\t\n" +
                    "1970-01-01T00:42:00.000000Z\t\t13\t15\n" +
                    "1970-01-01T00:48:00.000000Z\t15\t14\t\n" +
                    "1970-01-01T00:54:00.000000Z\trdgb\t15\t\n" +
                    "1970-01-01T01:00:00.000000Z\t\t16\trdgb\n" +
                    "1970-01-01T01:06:00.000000Z\t15\t17\t1\n" +
                    "1970-01-01T01:12:00.000000Z\trdgb\t18\t\n" +
                    "1970-01-01T01:18:00.000000Z\tquestdb2\t19\tquestdb2\n" +
                    "1970-01-01T01:24:00.000000Z\t\t20\t15\n");
        });
    }

    @Test
    public void testUpdateStringColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            compile("alter table up add column str2 string", sqlExecutionContext);
            compile("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x + 10 as lng2," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str2" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET str1 = 'questdb1', str2 = 'questdb2' WHERE lng2 in (6, 8, 10, 12, 14)");

            assertSql("up", "ts\tstr1\tlng2\tstr2\n" +
                    "1970-01-01T00:00:00.000000Z\t15\t1\t\n" +
                    "1970-01-01T06:00:00.000000Z\t15\t2\t\n" +
                    "1970-01-01T12:00:00.000000Z\t\t3\t\n" +
                    "1970-01-01T18:00:00.000000Z\t1\t4\t\n" +
                    "1970-01-02T00:00:00.000000Z\t1\t5\t\n" +
                    "1970-01-02T06:00:00.000000Z\tquestdb1\t6\tquestdb2\n" +
                    "1970-01-02T12:00:00.000000Z\t190232\t7\t\n" +
                    "1970-01-02T18:00:00.000000Z\tquestdb1\t8\tquestdb2\n" +
                    "1970-01-03T00:00:00.000000Z\t15\t9\t\n" +
                    "1970-01-03T06:00:00.000000Z\tquestdb1\t10\tquestdb2\n" +
                    "1970-01-07T22:40:00.000000Z\t\t11\t190232\n" +
                    "1970-01-08T04:40:00.000000Z\tquestdb1\t12\tquestdb2\n" +
                    "1970-01-08T10:40:00.000000Z\t\t13\t15\n" +
                    "1970-01-08T16:40:00.000000Z\tquestdb1\t14\tquestdb2\n" +
                    "1970-01-08T22:40:00.000000Z\trdgb\t15\t\n");
        });
    }

    @Test
    public void testUpdateSymbolToChar() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " rnd_symbol('ab', 'bc') sym," +
                            " cast(null as timestamp) ts2" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE up \n" +
                    "SET sym = NULLIF(CONCAT(to_str(ts2, 'yyyy-MM-dd'), 'n'), 'n')");

            assertSql("up", "ts\tsym\tts2\n" +
                    "1970-01-01T00:00:00.000000Z\t\t\n" +
                    "1970-01-01T00:00:01.000000Z\t\t\n" +
                    "1970-01-01T00:00:02.000000Z\t\t\n" +
                    "1970-01-01T00:00:03.000000Z\t\t\n" +
                    "1970-01-01T00:00:04.000000Z\t\t\n");

            executeUpdate("UPDATE up \n" +
                    "SET sym = COALESCE(CONCAT(to_str(ts2, 'yyyy-MM-dd'), 'n'), 'n')");

            assertSql("up", "ts\tsym\tts2\n" +
                    "1970-01-01T00:00:00.000000Z\tn\t\n" +
                    "1970-01-01T00:00:01.000000Z\tn\t\n" +
                    "1970-01-01T00:00:02.000000Z\tn\t\n" +
                    "1970-01-01T00:00:03.000000Z\tn\t\n" +
                    "1970-01-01T00:00:04.000000Z\tn\t\n");
        });
    }

    @Test
    public void testUpdateSymbolWithNotEqualsInWhere() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);
            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            CompiledQuery cq = compiler.compile("UPDATE up SET symCol = 'VTJ' WHERE symCol != 'WCP'", sqlExecutionContext);
            try (OperationFuture fut = cq.execute(eventSubSequence)) {
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, fut.getStatus());
                Assert.assertEquals(2, fut.getAffectedRowsCount());
            }
            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "VTJ\t1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateTableNameContainsSpace() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table \"віт ер\" as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdate("UPDATE \"віт ер\" SET X = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql("\"віт ер\"", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n" +
                    "1970-01-01T00:00:02.000000Z\tNaN\n" +
                    "1970-01-01T00:00:03.000000Z\tNaN\n" +
                    "1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    @Test
    public void testUpdateTableWithoutDesignatedTimestamp() throws Exception {
        //this test makes sense for non-WAL tables only, WAL table has to be partitioned
        // and table cannot be partitioned without designated timestamp
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))", sqlExecutionContext);

            executeUpdate("UPDATE up SET x = 12");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t12\n" +
                    "1970-01-01T00:00:01.000000Z\t12\n" +
                    "1970-01-01T00:00:02.000000Z\t12\n" +
                    "1970-01-01T00:00:03.000000Z\t12\n" +
                    "1970-01-01T00:00:04.000000Z\t12\n");
        });
    }

    @Test
    public void testUpdateTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            executeUpdateFails("UPDATE up SET ts = 1", 14, "Designated timestamp column cannot be updated");
        });
    }

    @Test
    public void testUpdateTimestampToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
    public void testUpdateToBindVar() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            sqlExecutionContext.getBindVariableService().setInt(0, 100);
            executeUpdate("UPDATE up SET x = $1 WHERE x > 1 and x < 4");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t100\n");
        });
    }

    @Test
    public void testUpdateToNull() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
    public void testUpdateUnsupportedKeyword() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" +
                    "XUX\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "IBB\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "IBB\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "GZS\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = 'VTJ' JOIN t2 ON up.x = t2.x", 29, "FROM, WHERE or EOF expected");
        });
    }

    @Test
    public void testUpdateWith2TableJoinInWithClause() throws Exception {
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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

            assertSql("up", "ts\ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\ta\t101\n" +
                    "1970-01-01T00:00:01.000000Z\tc\t2\n" +
                    "1970-01-01T00:00:02.000000Z\tb\t303\n" +
                    "1970-01-01T00:00:03.000000Z\t\t505\n" +
                    "1970-01-01T00:00:04.000000Z\tb\t303\n");
        });
    }

    @Test
    public void testUpdateWithBindVarInWhere() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

            sqlExecutionContext.getBindVariableService().setInt(0, 2);
            executeUpdate("UPDATE up SET x = 100 WHERE x < $1");

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t100\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n");
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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""), sqlExecutionContext);

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
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

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

            assertSql("up", "ts\tx\n" +
                    "1970-01-01T00:00:00.000000Z\t151\n" +
                    "1970-01-01T00:00:01.000000Z\t251\n" +
                    "1970-01-01T00:00:02.000000Z\t300\n" +
                    "1970-01-01T00:00:03.000000Z\t400\n" +
                    "1970-01-01T00:00:04.000000Z\t500\n");
        });
    }

    @Test
    public void testUpdateWithJoinUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? "partition by DAY WAL" : ""), sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" +
                    "XUX\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "IBB\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "IBB\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "GZS\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = 'VTJ' FROM t2 CROSS JOIN up ON up.x = t2.x", 37, "JOIN is not supported on UPDATE statement");
        });
    }

    @Test
    public void testUpdateWithLatestOnUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = 'ABC' LATEST ON ts PARTITION BY symCol", 29, "FROM, WHERE or EOF expected");
        });
    }

    @Test
    public void testUpdateWithSubSelectUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" +
                    "XUX\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "IBB\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "IBB\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "GZS\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            executeUpdateFails("UPDATE up SET symCol = (select symCol2 from t2 where x = 4)", 24, "query is not allowed here");
        });
    }

    @Test
    public void testUpdateWithSymbolJoin() throws Exception {
        //this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)", sqlExecutionContext);

            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            compiler.compile("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)", sqlExecutionContext);

            assertSql("t2", "symCol2\tts\tx\n" +
                    "XUX\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "IBB\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "IBB\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "GZS\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            CompiledQuery cq = compiler.compile("UPDATE up SET symCol = 'VTJ' FROM t2 WHERE up.symCol = t2.symCol2", sqlExecutionContext);
            try (OperationFuture fut = cq.execute(eventSubSequence)) {
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, fut.getStatus());
                Assert.assertEquals(1, fut.getAffectedRowsCount());
            }
            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "VTJ\t1970-01-01T00:00:04.000000Z\t5\n");
        });
    }

    private void applyUpdate(UpdateOperation updateOperation) {
        updateOperation.withContext(sqlExecutionContext);
        try (TableWriter tableWriter = getWriter(updateOperation.getTableToken())) {
            updateOperation.apply(tableWriter, false);
        }
    }

    private void createTablesToJoin(String createTableSql) throws SqlException {
        compiler.compile(createTableSql, sqlExecutionContext);

        compiler.compile("create table down1 (s symbol index, y int)" + (walEnabled ? " WAL" : ""), sqlExecutionContext);
        executeInsert("insert into down1 values ('a', 1)");
        executeInsert("insert into down1 values ('a', 2)");
        executeInsert("insert into down1 values ('b', 3)");
        executeInsert("insert into down1 values ('b', 4)");
        executeInsert("insert into down1 values (null, 5)");
        executeInsert("insert into down1 values (null, 6)");

        compiler.compile("create table  down2 (s symbol index, y long)" + (walEnabled ? " WAL" : ""), sqlExecutionContext);
        executeInsert("insert into down2 values ('a', 100)");
        executeInsert("insert into down2 values ('b', 300)");
        executeInsert("insert into down2 values (null, 500)");

        // Check what will be in JOIN between down1 and down2
        assertSql("select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s", "sm\ts\n" +
                "101\ta\n" +
                "102\ta\n" +
                "303\tb\n" +
                "304\tb\n" +
                "505\t\n" +
                "506\t\n");
    }

    private void executeUpdate(String query) throws SqlException {
        try {
            if (walEnabled) {
                circuitBreaker.setTimeout(1);
            }
            executeOperation(query, CompiledQuery.UPDATE);
        } finally {
            circuitBreaker.setTimeout(DEFAULT_CIRCUIT_BREAKER_TIMEOUT);
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

    private void testInsertAfterFailed(boolean closeWriter) throws Exception {
        //this test makes sense for non-WAL tables only, WAL tables behave differently
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Chars.endsWith(name, "x.d.1") && Chars.contains(name, "1970-01-03")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                }
            };
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 24*60*60*1000000L) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY", sqlExecutionContext);

            try {
                executeUpdate("UPDATE up SET x = 1");
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "could not open read-write");
            }

            if (closeWriter) {
                engine.releaseInactive();
            }

            assertSql("up",
                    "ts\tv\tx\tz\n" +
                            "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" +
                            "1970-01-02T00:00:00.000000Z\t2\t2\t2\n" +
                            "1970-01-03T00:00:00.000000Z\t3\t3\t3\n" +
                            "1970-01-04T00:00:00.000000Z\t4\t4\t4\n" +
                            "1970-01-05T00:00:00.000000Z\t5\t5\t5\n");

            compile("INSERT INTO up VALUES('1970-01-01T00:00:05.000000Z', 10.0, 10.0, 10.0)");
            compile("INSERT INTO up VALUES('1970-01-01T00:00:06.000000Z', 100.0, 100.0, 100.0)");

            assertSql("up", "ts\tv\tx\tz\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1\n" +
                    "1970-01-01T00:00:05.000000Z\t10\t10\t10\n" +
                    "1970-01-01T00:00:06.000000Z\t100\t100\t100\n" +
                    "1970-01-02T00:00:00.000000Z\t2\t2\t2\n" +
                    "1970-01-03T00:00:00.000000Z\t3\t3\t3\n" +
                    "1970-01-04T00:00:00.000000Z\t4\t4\t4\n" +
                    "1970-01-05T00:00:00.000000Z\t5\t5\t5\n");
        });
    }

    private void testSymbol_UpdateWithExistingValue(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            executeUpdate("update up set symCol = 'VTJ' where symCol = 'WCP'");
            assertSql("up", "symCol\tts\tx\n" +
                    "VTJ\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "VTJ\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "VTJ\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            try (RecordCursorFactory factory = compiler.compile("up where symCol = 'VTJ'", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(indexed, cursor.isUsingIndex());
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, TestUtils.printer);
                }
            }
            TestUtils.assertEquals("symCol\tts\tx\n" +
                    "VTJ\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "VTJ\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "VTJ\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n", sink);

            assertSql("up where symCol = 'WCP'", "symCol\tts\tx\n");
        });
    }

    private void testSymbolsReplacedDistinct(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            executeUpdate("update up set symCol = 'ABC' where symCol = 'WCP'");
            assertSql("up", "symCol\tts\tx\n" +
                    "ABC\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "ABC\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "ABC\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            assertQuery("symCol\n" +
                            "\n" +
                            "ABC\n" +
                            "VTJ\n",
                    "select distinct symCol from up order by symCol",
                    null,
                    true,
                    true);

            assertSql("select symCol, count() from up order by symCol", "symCol\tcount\n" +
                    "\t1\n" +
                    "ABC\t3\n" +
                    "VTJ\t1\n");
        });
    }

    private void testSymbols_UpdateNull(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            executeUpdate("update up set symCol = 'ABC' where symCol is null");
            assertSql("up", "symCol\tts\tx\n" +
                    "WCP\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "WCP\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "WCP\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "ABC\t1970-01-01T00:00:04.000000Z\t5\n");

            assertSql("up where symCol = 'ABC'", "symCol\tts\tx\n" +
                    "ABC\t1970-01-01T00:00:04.000000Z\t5\n");
            assertSql("up where symCol is null", "symCol\tts\tx\n");
        });
    }

    private void testSymbols_UpdateWithNewValue(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""), sqlExecutionContext);

            executeUpdate("update up set symCol = 'ABC' where symCol = 'WCP'");
            assertSql("up", "symCol\tts\tx\n" +
                    "ABC\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "ABC\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "ABC\t1970-01-01T00:00:02.000000Z\t3\n" +
                    "VTJ\t1970-01-01T00:00:03.000000Z\t4\n" +
                    "\t1970-01-01T00:00:04.000000Z\t5\n");

            try (RecordCursorFactory factory = compiler.compile("up where symCol = 'ABC'", sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(indexed, cursor.isUsingIndex());
                    TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, TestUtils.printer);
                }
            }
            TestUtils.assertEquals("symCol\tts\tx\n" +
                    "ABC\t1970-01-01T00:00:00.000000Z\t1\n" +
                    "ABC\t1970-01-01T00:00:01.000000Z\t2\n" +
                    "ABC\t1970-01-01T00:00:02.000000Z\t3\n", sink);

            assertSql("up where symCol = 'WCP'", "symCol\tts\tx\n");
        });
    }

    private void testUpdateAsyncMode(Consumer<TableWriter> writerConsumer, String errorMsg, String expectedData) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " x" +
                            " from long_sequence(5))" +
                            " timestamp(ts)", sqlExecutionContext
            );

            CyclicBarrier barrier = new CyclicBarrier(2);

            final Thread th = new Thread(() -> {
                try {
                    TableWriter tableWriter = getWriter("up");
                    barrier.await(); // table is locked
                    barrier.await(); // update is on writer async cmd queue
                    writerConsumer.accept(tableWriter);
                    tableWriter.tick();
                    tableWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            });
            th.start();

            barrier.await(); // table is locked
            CompiledQuery cq = compiler.compile("UPDATE up SET x = 123 WHERE x > 1 and x < 4", sqlExecutionContext);
            try (OperationFuture fut = cq.execute(eventSubSequence)) {
                Assert.assertEquals(OperationFuture.QUERY_NO_RESPONSE, fut.getStatus());
                Assert.assertEquals(0, fut.getAffectedRowsCount());
                barrier.await(); // update is on writer async cmd queue

                if (errorMsg == null) {
                    fut.await(10 * Timestamps.SECOND_MILLIS); // 10 seconds timeout
                    Assert.assertEquals(OperationFuture.QUERY_COMPLETE, fut.getStatus());
                    Assert.assertEquals(2, fut.getAffectedRowsCount());
                } else {
                    try {
                        fut.await(10 * Timestamps.SECOND_MILLIS); // 10 seconds timeout
                        Assert.fail("Expected exception missing");
                    } catch (TableReferenceOutOfDateException | SqlException e) {
                        Assert.assertEquals(errorMsg, e.getMessage());
                        Assert.assertEquals(0, fut.getAffectedRowsCount());
                    }
                }
            }
            th.join();

            assertSql("up", expectedData);
        });
    }

    @Override
    protected void assertSql(CharSequence sql, CharSequence expected) throws SqlException {
        if (walEnabled) {
            drainWalQueue();
        }
        super.assertSql(sql, expected);
    }
}
