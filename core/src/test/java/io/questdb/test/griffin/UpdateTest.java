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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class UpdateTest extends AbstractCairoTest {
    private static final long DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 300_000L;
    protected final SCSequence eventSubSequence = new SCSequence();
    private final boolean walEnabled;

    public UpdateTest() {
        this.walEnabled = TestUtils.isWal();
    }

    @Override
    @Before
    public void setUp() {
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine,
                new DefaultSqlExecutionCircuitBreakerConfiguration() {
                    @Override
                    public boolean checkConnection() {
                        return false;
                    }
                },
                MemoryTag.NATIVE_DEFAULT
        ) {
        };
        circuitBreaker.setTimeout(DEFAULT_CIRCUIT_BREAKER_TIMEOUT);
        super.setUp();
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
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            update("UPDATE up SET x = 1");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t1\t1\t1
                            1970-01-01T00:00:01.000000Z\t2\t1\t2
                            1970-01-01T00:00:02.000000Z\t3\t1\t3
                            1970-01-01T00:00:03.000000Z\t4\t1\t4
                            1970-01-01T00:00:04.000000Z\t5\t1\t5
                            """,
                    "up"
            );

            update("UPDATE up SET z = 2");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t1\t1\t2
                            1970-01-01T00:00:01.000000Z\t2\t1\t2
                            1970-01-01T00:00:02.000000Z\t3\t1\t2
                            1970-01-01T00:00:03.000000Z\t4\t1\t2
                            1970-01-01T00:00:04.000000Z\t5\t1\t2
                            """,
                    "up"
            );

            update("UPDATE up SET v = 33");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t33\t1\t2
                            1970-01-01T00:00:01.000000Z\t33\t1\t2
                            1970-01-01T00:00:02.000000Z\t33\t1\t2
                            1970-01-01T00:00:03.000000Z\t33\t1\t2
                            1970-01-01T00:00:04.000000Z\t33\t1\t2
                            """,
                    "up"
            );

            execute("INSERT INTO up VALUES('1970-01-01T00:00:05.000000Z', 10.0, 10.0, 10.0)");
            execute("INSERT INTO up VALUES('1970-01-01T00:00:06.000000Z', 100.0, 100.0, 100.0)");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t33\t1\t2
                            1970-01-01T00:00:01.000000Z\t33\t1\t2
                            1970-01-01T00:00:02.000000Z\t33\t1\t2
                            1970-01-01T00:00:03.000000Z\t33\t1\t2
                            1970-01-01T00:00:04.000000Z\t33\t1\t2
                            1970-01-01T00:00:05.000000Z\t10\t10\t10
                            1970-01-01T00:00:06.000000Z\t100\t100\t100
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testNoRowsUpdated() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            update("UPDATE up SET x = 1 WHERE x > 10");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t1\t1\t1
                            1970-01-01T00:00:01.000000Z\t2\t2\t2
                            1970-01-01T00:00:02.000000Z\t3\t3\t3
                            1970-01-01T00:00:03.000000Z\t4\t4\t4
                            1970-01-01T00:00:04.000000Z\t5\t5\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testNoRowsUpdated_TrivialNotEqualsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            update("UPDATE up SET x = 1 WHERE 1 != 1");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t1\t1\t1
                            1970-01-01T00:00:01.000000Z\t2\t2\t2
                            1970-01-01T00:00:02.000000Z\t3\t3\t3
                            1970-01-01T00:00:03.000000Z\t4\t4\t4
                            1970-01-01T00:00:04.000000Z\t5\t5\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testStringToIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(case when x = 1 then null else rnd_ipv4() end as string) as str," +
                            " cast(null as ipv4) as ip " +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            update("UPDATE up SET ip = str");

            String data = """
                    ts\tstr\tip
                    1970-01-01T00:00:00.000000Z\t\t
                    1970-01-01T00:00:01.000000Z\t187.139.150.80\t187.139.150.80
                    1970-01-01T00:00:02.000000Z\t18.206.96.238\t18.206.96.238
                    1970-01-01T00:00:03.000000Z\t92.80.211.65\t92.80.211.65
                    1970-01-01T00:00:04.000000Z\t212.159.205.29\t212.159.205.29
                    """;
            assertSql(data, "up");

            update("UPDATE up set str = 'abc'");
            update("UPDATE up set str = ip");
            assertSql(data, "up");
        });
    }

    @Test
    public void testSymbolIndexCopyOnWrite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)" +
                    "), index(symCol) timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""));
            assertSql("""
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            try (RecordCursorFactory factory = select("up where symCol = 'WCP'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    update("update up set symCol = null");
                    // Index is updated
                    assertSql(
                            """
                                    symCol\tts\tx
                                    \t1970-01-01T00:00:00.000000Z\t1
                                    \t1970-01-01T00:00:01.000000Z\t2
                                    \t1970-01-01T00:00:02.000000Z\t3
                                    \t1970-01-01T00:00:03.000000Z\t4
                                    \t1970-01-01T00:00:04.000000Z\t5
                                    """,
                            "up where symCol = null"
                    );

                    // Old index is still working
                    assertCursor(
                            """
                                    symCol\tts\tx
                                    WCP\t1970-01-01T00:00:00.000000Z\t1
                                    WCP\t1970-01-01T00:00:01.000000Z\t2
                                    WCP\t1970-01-01T00:00:02.000000Z\t3
                                    """,
                            cursor,
                            factory.getMetadata(),
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolIndexRebuiltOnAffectedPartitionsOnly() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean removeQuiet(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return false;
                    }
                    return super.removeQuiet(name);
                }
            };
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 60*60*1000000L) ts," +
                    " x" +
                    " from long_sequence(5)" +
                    "), index(symCol) timestamp(ts) Partition by hour" + (walEnabled ? " WAL" : ""));

            try (
                    RecordCursorFactory factory = select("up where symCol = 'WCP'");
                    RecordCursor ignored = factory.getCursor(sqlExecutionContext)
            ) {

                update("update up set symCol = null where ts >= '1970-01-01T03'");
                // Index is updated
                assertSql(
                        """
                                symCol\tts\tx
                                WCP\t1970-01-01T00:00:00.000000Z\t1
                                WCP\t1970-01-01T01:00:00.000000Z\t2
                                WCP\t1970-01-01T02:00:00.000000Z\t3
                                \t1970-01-01T03:00:00.000000Z\t4
                                \t1970-01-01T04:00:00.000000Z\t5
                                """,
                        "up"
                );
            }
        });
    }

    @Test
    public void testSymbolIndexRebuiltOnColumnWithTopOverwrittenInO3() throws Exception {
        assertMemoryLeak(() -> {
            // Fill every second min from 00:00 to 02:30
            execute(
                    "create table symInd as" +
                            " (select " +
                            "timestamp_sequence(0, 2*60*1000000L) ts," +
                            " x" +
                            " from long_sequence(75)" +
                            ") timestamp(ts) Partition by hour" + (walEnabled ? " WAL" : "")
            );

            // Add indexed column in last partition
            execute("alter table symInd add column sym_index symbol index");

            // More data in order
            execute(
                    "insert into symInd " +
                            " select " +
                            " timestamp_sequence('1970-01-01T02:30', 60*1000000L) ts," +
                            " x," +
                            " cast(x as symbol)" +
                            " from long_sequence(45)"
            );

            // O3 data in the first partition
            execute(
                    "insert into symInd " +
                            " select " +
                            " timestamp_sequence(1, 2 * 60*1000000L) ts," +
                            " x," +
                            " cast(x as symbol)" +
                            " from long_sequence(20)"
            );

            // Update column to itself. Should rebuild whole index
            update("update symInd set sym_index = sym_index");

            assertSql(
                    """
                            count\tmin\tmax
                            75\t1970-01-01T00:00:00.000000Z\t1970-01-01T02:28:00.000000Z
                            """,
                    "select count(), min(ts), max(ts) from symInd where sym_index = null"
            );

            for (int i = 0; i < 60; i += 10) {
                // Index is updated
                assertSqlCursors(
                        "symInd where (sym_index || '') = '" + i + "'",
                        "symInd where sym_index = '" + i + "'"
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
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, "s1.d.1") && Utf8s.containsAscii(name, "1970-01-03")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                }
            };
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 24*60*60*1000000L) ts," +
                            " cast(x as int) v," +
                            " cast('a' as SYMBOL) s1, " +
                            " cast('b' as SYMBOL) s2 " +
                            " from long_sequence(5))" +
                            " ,index(s1) timestamp(ts) partition by DAY"
            );

            try (TableWriter writer = getWriter("up")) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
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
                    txReader.ofRO(
                            Path.getThreadLocal(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                            writer.getMetadata().getTimestampType(),
                            PartitionBy.DAY
                    );
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
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', null) s," +
                    " x," +
                    " x + 1 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update(
                    "WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y FROM down1 JOIN down2 ON down1.s = down2.s)" +
                            "UPDATE up SET x = sm, y = jn.y" +
                            " FROM jn " +
                            " WHERE up.s = jn.s"
            );

            assertSql(
                    """
                            ts\ts\tx\ty
                            1970-01-01T00:00:00.000000Z\ta\t101\t100
                            1970-01-01T00:00:01.000000Z\ta\t101\t100
                            1970-01-01T00:00:02.000000Z\tb\t303\t300
                            1970-01-01T00:00:03.000000Z\t\t505\t500
                            1970-01-01T00:00:04.000000Z\t\t505\t500
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table testUpdateAddedColumn as" +
                            " (select timestamp_sequence(0, 6*60*60*1000000L) ts," +
                            " cast(x - 1 as int) x" +
                            " from long_sequence(10))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            // Bump table version
            execute("alter table testUpdateAddedColumn add column y long", sqlExecutionContext);
            update("UPDATE testUpdateAddedColumn SET y = x + 1 WHERE ts between '1970-01-01T12' and '1970-01-02T12'");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t0\tnull
                            1970-01-01T06:00:00.000000Z\t1\tnull
                            1970-01-01T12:00:00.000000Z\t2\t3
                            1970-01-01T18:00:00.000000Z\t3\t4
                            1970-01-02T00:00:00.000000Z\t4\t5
                            1970-01-02T06:00:00.000000Z\t5\t6
                            1970-01-02T12:00:00.000000Z\t6\t7
                            1970-01-02T18:00:00.000000Z\t7\tnull
                            1970-01-03T00:00:00.000000Z\t8\tnull
                            1970-01-03T06:00:00.000000Z\t9\tnull
                            """,
                    "testUpdateAddedColumn"
            );

            execute("alter table testUpdateAddedColumn drop column y");
            execute("alter table testUpdateAddedColumn add column y int");
            update("UPDATE testUpdateAddedColumn SET y = COALESCE(y, x + 2) WHERE x%2 = 0");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t0\t2
                            1970-01-01T06:00:00.000000Z\t1\tnull
                            1970-01-01T12:00:00.000000Z\t2\t4
                            1970-01-01T18:00:00.000000Z\t3\tnull
                            1970-01-02T00:00:00.000000Z\t4\t6
                            1970-01-02T06:00:00.000000Z\t5\tnull
                            1970-01-02T12:00:00.000000Z\t6\t8
                            1970-01-02T18:00:00.000000Z\t7\tnull
                            1970-01-03T00:00:00.000000Z\t8\t10
                            1970-01-03T06:00:00.000000Z\t9\tnull
                            """,
                    "testUpdateAddedColumn"
            );

            execute("alter table testUpdateAddedColumn drop column x");
            update("UPDATE testUpdateAddedColumn SET y = COALESCE(y, 1)");

            assertSql(
                    """
                            ts\ty
                            1970-01-01T00:00:00.000000Z\t2
                            1970-01-01T06:00:00.000000Z\t1
                            1970-01-01T12:00:00.000000Z\t4
                            1970-01-01T18:00:00.000000Z\t1
                            1970-01-02T00:00:00.000000Z\t6
                            1970-01-02T06:00:00.000000Z\t1
                            1970-01-02T12:00:00.000000Z\t8
                            1970-01-02T18:00:00.000000Z\t1
                            1970-01-03T00:00:00.000000Z\t10
                            1970-01-03T06:00:00.000000Z\t1
                            """,
                    "testUpdateAddedColumn"
            );
        });
    }

    @Test
    public void testUpdateAsyncMode() throws Exception {
        // this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        testUpdateAsyncMode(
                tableWriter -> {
                },
                null,
                """
                        ts\tx
                        1970-01-01T00:00:00.000000Z\t1
                        1970-01-01T00:00:01.000000Z\t123
                        1970-01-01T00:00:02.000000Z\t123
                        1970-01-01T00:00:03.000000Z\t4
                        1970-01-01T00:00:04.000000Z\t5
                        """
        );
    }

    @Test
    public void testUpdateAsyncModeAddColumnInMiddle() throws Exception {
        //this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        testUpdateAsyncMode(
                tableWriter -> tableWriter.addColumn("newCol", ColumnType.INT),
                "cached query plan cannot be used because table schema has changed [table=up]",
                """
                        ts\tx\tnewCol
                        1970-01-01T00:00:00.000000Z\t1\tnull
                        1970-01-01T00:00:01.000000Z\t2\tnull
                        1970-01-01T00:00:02.000000Z\t3\tnull
                        1970-01-01T00:00:03.000000Z\t4\tnull
                        1970-01-01T00:00:04.000000Z\t5\tnull
                        """
        );
    }

    @Test
    public void testUpdateAsyncModeFailed() throws Exception {
        // this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        final SqlExecutionContext oldContext = sqlExecutionContext;
        try {
            sqlExecutionContext = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public Rnd getAsyncRandom() {
                    throw new RuntimeException("test error");
                }
            }.with(engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext());

            testUpdateAsyncMode(
                    tableWriter -> {
                    },
                    "[43] test error",
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t2
                            1970-01-01T00:00:02.000000Z\t3
                            1970-01-01T00:00:03.000000Z\t4
                            1970-01-01T00:00:04.000000Z\t5
                            """
            );
        } finally {
            sqlExecutionContext = oldContext;
        }
    }

    @Test
    public void testUpdateAsyncModeRemoveColumnInMiddle() throws Exception {
        // this test makes sense for non-WAL tables only, UPDATE cannot go async in TableWriter for WAL tables
        Assume.assumeFalse(walEnabled);

        testUpdateAsyncMode(
                tableWriter -> tableWriter.removeColumn("x"),
                "cached query plan cannot be used because table schema has changed [table=up]",
                """
                        ts
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:01.000000Z
                        1970-01-01T00:00:02.000000Z
                        1970-01-01T00:00:03.000000Z
                        1970-01-01T00:00:04.000000Z
                        """
        );
    }

    @Test
    public void testUpdateBinaryColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_bin(10, 20, 2) as bin1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET bin1 = cast(null as binary) WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql(
                    """
                            ts\tbin1\tlng2
                            1970-01-01T00:00:00.000000Z\t00000000 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91
                            00000010 3b 72 db f3\t1
                            1970-01-01T06:00:00.000000Z\t00000000 c7 88 de a0 79 3c 77 15 68 61 26 af 19 c4 95 94
                            00000010 36 53\t2
                            1970-01-01T12:00:00.000000Z\t\t3
                            1970-01-01T18:00:00.000000Z\t\t4
                            1970-01-02T00:00:00.000000Z\t\t5
                            1970-01-02T06:00:00.000000Z\t00000000 08 a1 1e 38 8d 1b 9e f4 c8 39 09 fe d8\t6
                            1970-01-02T12:00:00.000000Z\t\t7
                            1970-01-02T18:00:00.000000Z\t00000000 78 b5 b9 11 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b
                            00000010 b1 3e e3 f1\t8
                            1970-01-03T00:00:00.000000Z\t\t9
                            1970-01-03T06:00:00.000000Z\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\t10
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateBinaryColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_bin(10, 20, 0) as bin1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column bin2 binary");
            execute("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_bin(10, 20, 0) as bin1," +
                    " x + 10 as lng2," +
                    " rnd_bin(10, 20, 0) as bin2" +
                    " from long_sequence(5))");
            update("UPDATE up SET bin1 = cast(null as binary), bin2 = cast(null as binary) WHERE lng2 in (6,8,10,12,14)");

            assertSql(
                    """
                            ts\tbin1\tlng2\tbin2
                            1970-01-01T00:00:00.000000Z\t00000000 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91
                            00000010 3b 72 db f3\t1\t
                            1970-01-01T06:00:00.000000Z\t00000000 c7 88 de a0 79 3c 77 15 68 61 26 af 19 c4 95 94
                            00000010 36 53\t2\t
                            1970-01-01T12:00:00.000000Z\t00000000 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09 fe\t3\t
                            1970-01-01T18:00:00.000000Z\t00000000 30 78 36 6a 32 de e4 7c d2 35 07 42 fc 31 79\t4\t
                            1970-01-02T00:00:00.000000Z\t00000000 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0\t5\t
                            1970-01-02T06:00:00.000000Z\t\t6\t
                            1970-01-02T12:00:00.000000Z\t00000000 ac 37 c8 cd 82 89 2b 4d 5f f6 46\t7\t
                            1970-01-02T18:00:00.000000Z\t\t8\t
                            1970-01-03T00:00:00.000000Z\t00000000 d2 85 7f a5 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0\t9\t
                            1970-01-03T06:00:00.000000Z\t\t10\t
                            1970-01-07T22:40:00.000000Z\t00000000 a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a e7
                            00000010 0c 89\t11\t00000000 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9 73 93
                            00000010 46 fe
                            1970-01-08T04:40:00.000000Z\t\t12\t
                            1970-01-08T10:40:00.000000Z\t00000000 e0 b0 e9 98 f7 67 62 28 60 b0 ec 0b 92\t13\t00000000 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37 11 2c
                            1970-01-08T16:40:00.000000Z\t\t14\t
                            1970-01-08T22:40:00.000000Z\t00000000 e4 35 e4 3a dc 5c 65 ff 27 67 77\t15\t00000000 52 d0 29 26 c5 aa da 18 ce 5f b2
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateBindArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x, Array[[1, 2], [3, 4]] as y" +
                    " from long_sequence(3))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            try (
                    DirectArray array = new DirectArray();
                    DirectArray array2 = new DirectArray()
            ) {
                array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
                array.setDimLen(0, 2);
                array.setDimLen(1, 2);
                array.applyShape();
                MemoryA mem = array.startMemoryA();
                mem.putDouble(2);
                mem.putDouble(3);
                mem.putDouble(4);
                mem.putDouble(5);
                array2.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
                array2.setDimLen(0, 0);
                array2.setDimLen(1, 0);
                array2.applyShape();
                sqlExecutionContext.getBindVariableService().setArray(0, array);
                sqlExecutionContext.getBindVariableService().setInt(1, 2);
                sqlExecutionContext.getBindVariableService().setArray(2, array2);
                sqlExecutionContext.getBindVariableService().setInt(3, 3);
                update("UPDATE tab SET y = $1 WHERE x = $2");
                update("UPDATE tab SET y = $3 WHERE x = $4");
            }

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t1\t[[1.0,2.0],[3.0,4.0]]
                            1970-01-01T00:00:01.000000Z\t2\t[[2.0,3.0],[4.0,5.0]]
                            1970-01-01T00:00:02.000000Z\t3\t[]
                            """,
                    "tab"
            );
        });
    }

    @Test
    public void testUpdateBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " cast(x as int) xint," +
                            " cast(x as boolean) xbool" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            assertSql(
                    """
                            ts\txint\txbool
                            1970-01-01T00:00:00.000000Z\t1\ttrue
                            1970-01-01T00:00:01.000000Z\t2\ttrue
                            1970-01-01T00:00:02.000000Z\t3\ttrue
                            1970-01-01T00:00:03.000000Z\t4\ttrue
                            1970-01-01T00:00:04.000000Z\t5\ttrue
                            """,
                    "up"
            );

            update("UPDATE up SET xbool = false WHERE xint = 2");

            assertSql(
                    """
                            ts\txint\txbool
                            1970-01-01T00:00:00.000000Z\t1\ttrue
                            1970-01-01T00:00:01.000000Z\t2\tfalse
                            1970-01-01T00:00:02.000000Z\t3\ttrue
                            1970-01-01T00:00:03.000000Z\t4\ttrue
                            1970-01-01T00:00:04.000000Z\t5\ttrue
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateColumnNameCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET X = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t2
                            1970-01-01T00:00:02.000000Z\tnull
                            1970-01-01T00:00:03.000000Z\tnull
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateColumnsTypeMismatch() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', null) s," +
                    " x," +
                    " x + 1 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            assertException(
                    "WITH jn AS (select down1.y + down2.y AS sm, down1.s, down2.y " +
                            "                         FROM down1 JOIN down2 ON down1.s = down2.s" +
                            ")" +
                            "UPDATE up SET s = sm, y = jn.y" +
                            " FROM jn " +
                            " WHERE jn.s = up.s",
                    147,
                    "inconvertible types: LONG -> SYMBOL"
            );
        });
    }

    @Test
    public void testUpdateDecimalColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as decimal(2, 1)) d8," +
                    " cast(x as decimal(4, 0)) d16," +
                    " cast(x as decimal(9, 3)) d32," +
                    " cast(x as decimal(15, 12)) d64," +
                    " cast(x as decimal(35, 0)) d128," +
                    " cast(x as decimal(64, 18)) d256" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up " +
                    "SET " +
                    "d8 = 3.30m, " +
                    "d16 = 0m, " +
                    "d32 = 128L, " +
                    "d64 = 123m, " +
                    "d128 = 123456789L, " +
                    "d256 = 123.456789m " +
                    "WHERE ts >= '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    "ts\td8\td16\td32\td64\td128\td256\n" +
                            "1970-01-01T00:00:00.000000Z\t1.0\t1\t1.000\t1.000000000000\t1\t1.000000000000000000\n" +
                            "1970-01-01T00:00:01.000000Z\t3.3\t0\t128.000\t123.000000000000\t123456789\t123.456789000000000000\n",
                    "up"
            );
        });
    }

    @Test
    public void testUpdateDifferentColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) xint," +
                    " cast(x as long) xlong," +
                    " cast(x as double) xdouble," +
                    " cast(x as short) xshort," +
                    " cast(x as byte) xbyte," +
                    " cast(x+48 as char) xchar," +
                    " cast(x as date) xdate," +
                    " cast(x as float) xfloat," +
                    " cast(x as timestamp) xts, " +
                    " cast(x as boolean) xbool," +
                    " cast(x as long256) xl256" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            // All combinations to update xint
            assertException(
                    "UPDATE up SET xint = xdouble",
                    21,
                    "inconvertible types: DOUBLE -> INT [from=, to=xint]"
            );
            assertException(
                    "UPDATE up SET xint = xlong",
                    21,
                    "inconvertible types: LONG -> INT [from=, to=xint]"
            );
            assertException(
                    "UPDATE up SET xshort = xlong",
                    23,
                    "inconvertible types: LONG -> SHORT [from=, to=xshort]"
            );
            assertException(
                    "UPDATE up SET xchar = xlong",
                    22,
                    "inconvertible types: LONG -> CHAR [from=, to=xchar]"
            );
            assertException(
                    "UPDATE up SET xbyte = xlong",
                    22,
                    "inconvertible types: LONG -> BYTE [from=, to=xbyte]"
            );
            assertException(
                    "UPDATE up SET xlong = xl256",
                    22,
                    "inconvertible types: LONG256 -> LONG [from=, to=xlong]"
            );
            assertException(
                    "UPDATE up SET xl256 = xlong",
                    22,
                    "inconvertible types: LONG -> LONG256 [from=, to=xl256]"
            );
            assertException(
                    "UPDATE up SET xchar = xlong",
                    22,
                    "inconvertible types: LONG -> CHAR [from=, to=xchar]"
            );

            String expected = """
                    ts\txint\txlong\txdouble\txshort\txbyte\txchar\txdate\txfloat\txts\txbool\txl256
                    1970-01-01T00:00:00.000000Z\t1\t1\t1.0\t1\t1\t1\t1970-01-01T00:00:00.001Z\t1.0\t1970-01-01T00:00:00.000001Z\ttrue\t0x01
                    1970-01-01T00:00:01.000000Z\t2\t2\t2.0\t2\t2\t2\t1970-01-01T00:00:00.002Z\t2.0\t1970-01-01T00:00:00.000002Z\ttrue\t0x02
                    """;

            update("UPDATE up SET xint=xshort");
            assertSql(expected, "up");
            update("UPDATE up SET xint=xshort WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xlong=xshort");
            assertSql(expected, "up");
            update("UPDATE up SET xlong=xshort WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xlong=xchar");
            assertSql(expected, "up");
            update("UPDATE up SET xlong=xchar WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xfloat=xint");
            assertSql(expected, "up");
            update("UPDATE up SET xfloat=xint WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xdouble=xfloat");
            assertSql(expected, "up");
            update("UPDATE up SET xdouble=xfloat WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xdouble=xlong");
            assertSql(expected, "up");
            update("UPDATE up SET xdouble=xlong WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xshort=xbyte");
            assertSql(expected, "up");
            update("UPDATE up SET xshort=xbyte WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xshort=xchar");
            assertSql(expected, "up");
            update("UPDATE up SET xshort=xchar WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xchar=(xshort+48)::short");
            assertSql(expected, "up");
            update("UPDATE up SET xchar=(xshort+48)::short WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xint=xchar");
            assertSql(expected, "up");
            update("UPDATE up SET xint=xchar WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xdouble=xlong");
            assertSql(expected, "up");
            update("UPDATE up SET xdouble=xlong WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xlong=xts");
            assertSql(expected, "up");
            update("UPDATE up SET xlong=xts WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xdate=xlong");
            assertSql(expected, "up");
            update("UPDATE up SET xdate=xlong WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            update("UPDATE up SET xts=xdate");
            // above call modified data from micro to milli. Revert the data back
            update("UPDATE up SET xts=xlong");
            assertSql(expected, "up");
            update("UPDATE up SET xts=xlong WHERE ts='1970-01-01'");
            assertSql(expected, "up");

            // Update all at once
            update("UPDATE up SET xint=xshort, xfloat=xint, xdouble=xfloat, xshort=xbyte, xlong=xts, xts=xlong");
            assertSql(expected, "up");

            // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
            if (!walEnabled) {
                // Update without conversion
                update("UPDATE up" +
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
                assertSql(expected, "up");
            }
        });
    }

    @Test
    public void testUpdateGeoHashColumnToLowerPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(5) g1c," +
                    " rnd_geohash(15) g3c," +
                    " rnd_geohash(25) g5c," +
                    " rnd_geohash(35) g7c" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up " +
                    "SET " +
                    "g1c = cast('questdb' as geohash(7c)), " +
                    "g3c = cast('questdb' as geohash(7c)), " +
                    "g5c = cast('questdb' as geohash(7c)), " +
                    "g7c = cast('questdb' as geohash(7c)) " +
                    "WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tg1c\tg3c\tg5c\tg7c
                            1970-01-01T00:00:00.000000Z\t9\t46s\tjnw97\tzfuqd3b
                            1970-01-01T00:00:01.000000Z\th\twh4\ts2z2f\t1cjjwk6
                            1970-01-01T00:00:02.000000Z\tq\tque\tquest\tquestdb
                            1970-01-01T00:00:03.000000Z\tq\tque\tquest\tquestdb
                            1970-01-01T00:00:04.000000Z\tx\t76u\tq0s5w\ts2vqs1b
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateGeoHashColumnToLowerPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(5) g1c," +
                    " rnd_geohash(15) g3c," +
                    " rnd_geohash(25) g5c," +
                    " rnd_geohash(35) g7c" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up " +
                    "SET " +
                    "g1c = g7c, " +
                    "g3c = g7c, " +
                    "g5c = g7c, " +
                    "g7c = g7c " +
                    "WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tg1c\tg3c\tg5c\tg7c
                            1970-01-01T00:00:00.000000Z\t9\t46s\tjnw97\tzfuqd3b
                            1970-01-01T00:00:01.000000Z\th\twh4\ts2z2f\t1cjjwk6
                            1970-01-01T00:00:02.000000Z\tq\tq4s\tq4s2x\tq4s2xyt
                            1970-01-01T00:00:03.000000Z\tb\tbuy\tbuyv3\tbuyv3pv
                            1970-01-01T00:00:04.000000Z\tx\t76u\tq0s5w\ts2vqs1b
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateGeohashColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column geo1 geohash(1c)", sqlExecutionContext);
            execute("alter table up add column geo2 geohash(2c)", sqlExecutionContext);
            execute("alter table up add column geo4 geohash(5c)", sqlExecutionContext);
            execute("alter table up add column geo8 geohash(8c)", sqlExecutionContext);
            execute("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x + 10 as lng2," +
                    " rnd_geohash(5) as geo1," +
                    " rnd_geohash(10) as geo2," +
                    " rnd_geohash(25) as geo4," +
                    " rnd_geohash(40) as geo8" +
                    " from long_sequence(5))", sqlExecutionContext);

            update("UPDATE up SET geo1 = cast('q' as geohash(1c)), geo2 = 'qu', geo4='quest', geo8='questdb0' WHERE lng2 in (6, 8, 10, 12, 14)");

            assertSql(
                    """
                            ts\tstr1\tlng2\tgeo1\tgeo2\tgeo4\tgeo8
                            1970-01-01T00:00:00.000000Z\t15\t1\t\t\t\t
                            1970-01-01T06:00:00.000000Z\t15\t2\t\t\t\t
                            1970-01-01T12:00:00.000000Z\t\t3\t\t\t\t
                            1970-01-01T18:00:00.000000Z\t1\t4\t\t\t\t
                            1970-01-02T00:00:00.000000Z\t1\t5\t\t\t\t
                            1970-01-02T06:00:00.000000Z\t1\t6\tq\tqu\tquest\tquestdb0
                            1970-01-02T12:00:00.000000Z\t190232\t7\t\t\t\t
                            1970-01-02T18:00:00.000000Z\t\t8\tq\tqu\tquest\tquestdb0
                            1970-01-03T00:00:00.000000Z\t15\t9\t\t\t\t
                            1970-01-03T06:00:00.000000Z\t\t10\tq\tqu\tquest\tquestdb0
                            1970-01-07T22:40:00.000000Z\t\t11\tn\tpn\t2gjm2\t7qgcr0y6
                            1970-01-08T04:40:00.000000Z\t\t12\tq\tqu\tquest\tquestdb0
                            1970-01-08T10:40:00.000000Z\t\t13\t8\t1y\tcd0fj\t5h18p8vz
                            1970-01-08T16:40:00.000000Z\t\t14\tq\tqu\tquest\tquestdb0
                            1970-01-08T22:40:00.000000Z\t\t15\t1\trc\t5vm2w\tz22qdyty
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateGeohashToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(15) as geo3," +
                    " rnd_geohash(25) as geo5 " +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET geo3 = 'questdb', geo5 = 'questdb' WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tgeo3\tgeo5
                            1970-01-01T00:00:00.000000Z\t9v1\t46swg
                            1970-01-01T00:00:01.000000Z\tjnw\tzfuqd
                            1970-01-01T00:00:02.000000Z\tque\tquest
                            1970-01-01T00:00:03.000000Z\tque\tquest
                            1970-01-01T00:00:04.000000Z\tmmt\t71ftm
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateGeohashToVarcharConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_geohash(15) as geo3," +
                    " rnd_geohash(25) as geo5 " +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET geo3 = 'questdb'::varchar, geo5 = 'questdb'::varchar WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tgeo3\tgeo5
                            1970-01-01T00:00:00.000000Z\t9v1\t46swg
                            1970-01-01T00:00:01.000000Z\tjnw\tzfuqd
                            1970-01-01T00:00:02.000000Z\tque\tquest
                            1970-01-01T00:00:03.000000Z\tque\tquest
                            1970-01-01T00:00:04.000000Z\tmmt\t71ftm
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateIdentical() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET x = x WHERE x > 1 and x < 4");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t2
                            1970-01-01T00:00:02.000000Z\t3
                            1970-01-01T00:00:03.000000Z\t4
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateMultiPartitionEmptyColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 25000000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column y long", sqlExecutionContext);
            update("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 6 or x = 8 or x = 13");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t1\tnull
                            1970-01-01T06:56:40.000000Z\t2\t42
                            1970-01-01T13:53:20.000000Z\t3\tnull
                            1970-01-01T20:50:00.000000Z\t4\t42
                            1970-01-02T03:46:40.000000Z\t5\tnull
                            1970-01-02T10:43:20.000000Z\t6\t42
                            1970-01-02T17:40:00.000000Z\t7\tnull
                            1970-01-03T00:36:40.000000Z\t8\t42
                            1970-01-03T07:33:20.000000Z\t9\tnull
                            1970-01-03T14:30:00.000000Z\t10\tnull
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateMultiPartitionedTableSamePartitionManyFrames() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY);
            tml.col("xint", ColumnType.INT).col("xsym", ColumnType.SYMBOL).indexed(true, 256).timestamp("ts");
            if (walEnabled) {
                tml.wal();
            }
            createPopulateTable(tml, 10, "2020-01-01", 2);

            update("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T00;6h;12h;24'");
            assertSql(
                    """
                            xint\txsym\tts
                            -1000\tCPSW\t2020-01-01T04:47:59.900000Z
                            2\tHYRX\t2020-01-01T09:35:59.800000Z
                            -1000\t\t2020-01-01T14:23:59.700000Z
                            4\tVTJW\t2020-01-01T19:11:59.600000Z
                            5\tPEHN\t2020-01-01T23:59:59.500000Z
                            -1000\t\t2020-01-02T04:47:59.400000Z
                            7\tVTJW\t2020-01-02T09:35:59.300000Z
                            -1000\t\t2020-01-02T14:23:59.200000Z
                            9\tCPSW\t2020-01-02T19:11:59.100000Z
                            10\t\t2020-01-02T23:59:59.000000Z
                            """,
                    "up"
            );

            update("UPDATE up SET xint = -1000 WHERE ts in '2020-01-01T06;6h;12h;24' and xint > 7");
            assertSql(
                    """
                            xint\txsym\tts
                            -1000\tCPSW\t2020-01-01T04:47:59.900000Z
                            2\tHYRX\t2020-01-01T09:35:59.800000Z
                            -1000\t\t2020-01-01T14:23:59.700000Z
                            4\tVTJW\t2020-01-01T19:11:59.600000Z
                            5\tPEHN\t2020-01-01T23:59:59.500000Z
                            -1000\t\t2020-01-02T04:47:59.400000Z
                            7\tVTJW\t2020-01-02T09:35:59.300000Z
                            -1000\t\t2020-01-02T14:23:59.200000Z
                            -1000\tCPSW\t2020-01-02T19:11:59.100000Z
                            -1000\t\t2020-01-02T23:59:59.000000Z
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateMultiPartitionsWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 25000000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            // Bump table version
            execute("alter table up add column y long", sqlExecutionContext);
            execute("insert into up select * from " +
                    " (select timestamp_sequence(250000000000, 25000000000) ts," +
                    " cast(x as int) + 10 as x," +
                    " cast(x as long) * 10 as y" +
                    " from long_sequence(10))", sqlExecutionContext);

            update("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 9 or x = 6 or x = 13 or x = 20");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t1\tnull
                            1970-01-01T06:56:40.000000Z\t2\t42
                            1970-01-01T13:53:20.000000Z\t3\tnull
                            1970-01-01T20:50:00.000000Z\t4\t42
                            1970-01-02T03:46:40.000000Z\t5\tnull
                            1970-01-02T10:43:20.000000Z\t6\t42
                            1970-01-02T17:40:00.000000Z\t7\tnull
                            1970-01-03T00:36:40.000000Z\t8\tnull
                            1970-01-03T07:33:20.000000Z\t9\t42
                            1970-01-03T14:30:00.000000Z\t10\tnull
                            1970-01-03T21:26:40.000000Z\t11\t10
                            1970-01-04T04:23:20.000000Z\t12\t20
                            1970-01-04T11:20:00.000000Z\t13\t42
                            1970-01-04T18:16:40.000000Z\t14\t40
                            1970-01-05T01:13:20.000000Z\t15\t50
                            1970-01-05T08:10:00.000000Z\t16\t60
                            1970-01-05T15:06:40.000000Z\t17\t70
                            1970-01-05T22:03:20.000000Z\t18\t80
                            1970-01-06T05:00:00.000000Z\t19\t90
                            1970-01-06T11:56:40.000000Z\t20\t42
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateMultipartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            TableModel tml = new TableModel(configuration, "up", PartitionBy.DAY);
            tml.col("xint", ColumnType.INT).col("xsym", ColumnType.SYMBOL).indexed(true, 256).timestamp("ts");
            if (walEnabled) {
                tml.wal();
            }
            createPopulateTable(tml, 5, "2020-01-01", 2);

            update("UPDATE up SET xint = -1000 WHERE ts > '2020-01-02T14'");
            assertSql(
                    "xint\txsym\tts\n" +
                            "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" +
                            "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" +
                            "3\t\t2020-01-02T04:47:59.400000Z\n" +
                            "-1000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                            "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n",   // Updated
                    "up"
            );

            update("UPDATE up SET xint = -2000 WHERE ts > '2020-01-02T14' AND xsym = 'VTJW'");
            assertSql(
                    "xint\txsym\tts\n" +
                            "1\tCPSW\t2020-01-01T09:35:59.800000Z\n" +
                            "2\tHYRX\t2020-01-01T19:11:59.600000Z\n" +
                            "3\t\t2020-01-02T04:47:59.400000Z\n" +
                            "-2000\tVTJW\t2020-01-02T14:23:59.200000Z\n" +  // Updated
                            "-1000\tPEHN\t2020-01-02T23:59:59.000000Z\n",
                    "up"
            );
        });
    }

    @Test
    public void testUpdateNoFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET x = 1");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t1
                            1970-01-01T00:00:02.000000Z\t1
                            1970-01-01T00:00:03.000000Z\t1
                            1970-01-01T00:00:04.000000Z\t1
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateNoFilterOnAlteredTable() throws Exception {
        // this test makes sense for non-WAL tables only, WAL tables behave differently
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(1))" +
                    " timestamp(ts) partition by DAY");
            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            """,
                    "up"
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cc = compiler.compile("UPDATE up SET x = 2", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
                try (UpdateOperation updateOperation = cc.getUpdateOperation()) {
                    // Bump table version
                    execute("alter table up add column y long", sqlExecutionContext);
                    execute("alter table up drop column y", sqlExecutionContext);

                    applyUpdate(updateOperation);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "table=up");
                }
            }
        });
    }

    @Test
    public void testUpdateNonPartitionedTable() throws Exception {
        // this test makes sense for non-WAL tables only, WAL table has to be partitioned
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts)");

            update("UPDATE up SET x = 123 WHERE x > 1 and x < 5");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t123
                            1970-01-01T00:00:02.000000Z\t123
                            1970-01-01T00:00:03.000000Z\t123
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateOnAlteredTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(1))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            // Bump table version
            execute("alter table up add column y long", sqlExecutionContext);
            execute("alter table up drop column y", sqlExecutionContext);

            update("UPDATE up SET x = 44");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t44
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateReadonlyFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            SqlExecutionContext roExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                    ReadOnlySecurityContext.INSTANCE,
                    bindVariableService,
                    null,
                    -1,
                    null
            );

            try {
                execute("UPDATE up SET x = x WHERE x > 1 and x < 4", roExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "permission denied");
            }
        });
    }

    @Test
    public void testUpdateRenamedSymbol() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            execute(
                    "create table test (ts timestamp, x int, y string, sym symbol, symi symbol index) timestamp(ts) partition by DAY WAL"
            );
            execute("insert into test select timestamp_sequence('2022-02-24T01:01', 1000000L * 60 * 60), x, 'a', 'abc', 'i' from long_sequence(5)");

            execute("alter table test add column abc int");
            execute("alter table test drop column x");
            execute("alter table test rename column y to xxx");
            execute("alter table test alter column sym add index");
            execute("alter table test dedup enable upsert keys(ts)");
            execute("alter table test dedup disable");
            execute("alter table test drop partition list '2022-02-23'");
            execute("alter table test detach partition list '2022-02-23'");
            execute("alter table test attach partition list '2022-02-23'");
            execute("alter table test alter column sym cache");
            execute("alter table test alter column symi drop index");
            execute("alter table test set type bypass wal");
            update("update test set sym = '2' where sym = '1'");

            drainWalQueue();

            assertSql(
                    """
                            ts\txxx\tsym\tsymi\tabc
                            2022-02-24T01:01:00.000000Z\ta\tabc\ti\tnull
                            2022-02-24T02:01:00.000000Z\ta\tabc\ti\tnull
                            2022-02-24T03:01:00.000000Z\ta\tabc\ti\tnull
                            2022-02-24T04:01:00.000000Z\ta\tabc\ti\tnull
                            2022-02-24T05:01:00.000000Z\ta\tabc\ti\tnull
                            """,
                    "test"
            );

        });
    }

    @Test
    public void testUpdateSinglePartitionColumnTopAndAroundDense() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x - 1 as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            // Bump table version
            execute("alter table up add column y long");
            execute("insert into up select * from " +
                    " (select timestamp_sequence(100000000, 1000000) ts," +
                    " cast(x - 1 as int) + 10 as x," +
                    " cast(x * 10 as long) as y" +
                    " from long_sequence(5))");

            update("UPDATE up SET y = 42 where x = 9 or x = 10 or x = 11");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t0\tnull
                            1970-01-01T00:00:01.000000Z\t1\tnull
                            1970-01-01T00:00:02.000000Z\t2\tnull
                            1970-01-01T00:00:03.000000Z\t3\tnull
                            1970-01-01T00:00:04.000000Z\t4\tnull
                            1970-01-01T00:00:05.000000Z\t5\tnull
                            1970-01-01T00:00:06.000000Z\t6\tnull
                            1970-01-01T00:00:07.000000Z\t7\tnull
                            1970-01-01T00:00:08.000000Z\t8\tnull
                            1970-01-01T00:00:09.000000Z\t9\t42
                            1970-01-01T00:01:40.000000Z\t10\t42
                            1970-01-01T00:01:41.000000Z\t11\t42
                            1970-01-01T00:01:42.000000Z\t12\t30
                            1970-01-01T00:01:43.000000Z\t13\t40
                            1970-01-01T00:01:44.000000Z\t14\t50
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateSinglePartitionColumnTopAndAroundSparse() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x - 1 as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            // Bump table version
            execute("alter table up add column y long", sqlExecutionContext);
            execute("insert into up select * from " +
                    " (select timestamp_sequence(100000000, 1000000) ts," +
                    " cast(x - 1 as int) + 10 as x," +
                    " cast(x * 10 as long) as y" +
                    " from long_sequence(5))", sqlExecutionContext);

            update("UPDATE up SET y = 42 where x = 5 or x = 7 or x = 10 or x = 13 or x = 14");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t0\tnull
                            1970-01-01T00:00:01.000000Z\t1\tnull
                            1970-01-01T00:00:02.000000Z\t2\tnull
                            1970-01-01T00:00:03.000000Z\t3\tnull
                            1970-01-01T00:00:04.000000Z\t4\tnull
                            1970-01-01T00:00:05.000000Z\t5\t42
                            1970-01-01T00:00:06.000000Z\t6\tnull
                            1970-01-01T00:00:07.000000Z\t7\t42
                            1970-01-01T00:00:08.000000Z\t8\tnull
                            1970-01-01T00:00:09.000000Z\t9\tnull
                            1970-01-01T00:01:40.000000Z\t10\t42
                            1970-01-01T00:01:41.000000Z\t11\t20
                            1970-01-01T00:01:42.000000Z\t12\t30
                            1970-01-01T00:01:43.000000Z\t13\t42
                            1970-01-01T00:01:44.000000Z\t14\t42
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateSinglePartitionEmptyColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 100000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));
            execute("alter table up add column y long", sqlExecutionContext);

            update("UPDATE up SET y = 42 where x = 2 or x = 4 or x = 6 or x = 8 or x = 13");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t1\tnull
                            1970-01-01T00:01:40.000000Z\t2\t42
                            1970-01-01T00:03:20.000000Z\t3\tnull
                            1970-01-01T00:05:00.000000Z\t4\t42
                            1970-01-01T00:06:40.000000Z\t5\tnull
                            1970-01-01T00:08:20.000000Z\t6\t42
                            1970-01-01T00:10:00.000000Z\t7\tnull
                            1970-01-01T00:11:40.000000Z\t8\t42
                            1970-01-01T00:13:20.000000Z\t9\tnull
                            1970-01-01T00:15:00.000000Z\t10\tnull
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateSinglePartitionGapAroundColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x - 1 as int) x" +
                    " from long_sequence(10))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            // Bump table version
            execute("alter table up add column y long", sqlExecutionContext);
            execute("insert into up select * from " +
                    " (select timestamp_sequence(100000000, 1000000) ts," +
                    " cast(x - 1 as int) + 10 as x," +
                    " cast(x * 10 as long) as y" +
                    " from long_sequence(5))", sqlExecutionContext);

            update("UPDATE up SET y = 42 where x = 6 or x = 8 or x = 12 or x = 14");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t0\tnull
                            1970-01-01T00:00:01.000000Z\t1\tnull
                            1970-01-01T00:00:02.000000Z\t2\tnull
                            1970-01-01T00:00:03.000000Z\t3\tnull
                            1970-01-01T00:00:04.000000Z\t4\tnull
                            1970-01-01T00:00:05.000000Z\t5\tnull
                            1970-01-01T00:00:06.000000Z\t6\t42
                            1970-01-01T00:00:07.000000Z\t7\tnull
                            1970-01-01T00:00:08.000000Z\t8\t42
                            1970-01-01T00:00:09.000000Z\t9\tnull
                            1970-01-01T00:01:40.000000Z\t10\t10
                            1970-01-01T00:01:41.000000Z\t11\t20
                            1970-01-01T00:01:42.000000Z\t12\t42
                            1970-01-01T00:01:43.000000Z\t13\t40
                            1970-01-01T00:01:44.000000Z\t14\t42
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateString() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select" +
                            " rnd_str('foo','bar') as s," +
                            " timestamp_sequence(0, 1000000) ts," +
                            " x" +
                            " from long_sequence(5)) timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""),
                    sqlExecutionContext
            );

            // char
            update("update up set s = 'a' where s = 'bar'");
            assertSql(
                    """
                            s\tts\tx
                            foo\t1970-01-01T00:00:00.000000Z\t1
                            foo\t1970-01-01T00:00:01.000000Z\t2
                            a\t1970-01-01T00:00:02.000000Z\t3
                            a\t1970-01-01T00:00:03.000000Z\t4
                            a\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            // string
            update("update up set s = 'baz' where s = 'a'");
            assertSql(
                    """
                            s\tts\tx
                            foo\t1970-01-01T00:00:00.000000Z\t1
                            foo\t1970-01-01T00:00:01.000000Z\t2
                            baz\t1970-01-01T00:00:02.000000Z\t3
                            baz\t1970-01-01T00:00:03.000000Z\t4
                            baz\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            // UUID
            update("update up set s = cast('11111111-1111-1111-1111-111111111111' as uuid) where s = 'baz'");
            assertSql(
                    """
                            s\tts\tx
                            foo\t1970-01-01T00:00:00.000000Z\t1
                            foo\t1970-01-01T00:00:01.000000Z\t2
                            11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:02.000000Z\t3
                            11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:03.000000Z\t4
                            11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET str1 = 'questdb' WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql(
                    """
                            ts\tstr1\tlng2
                            1970-01-01T00:00:00.000000Z\t15\t1
                            1970-01-01T06:00:00.000000Z\t15\t2
                            1970-01-01T12:00:00.000000Z\tquestdb\t3
                            1970-01-01T18:00:00.000000Z\t1\t4
                            1970-01-02T00:00:00.000000Z\tquestdb\t5
                            1970-01-02T06:00:00.000000Z\t1\t6
                            1970-01-02T12:00:00.000000Z\tquestdb\t7
                            1970-01-02T18:00:00.000000Z\t\t8
                            1970-01-03T00:00:00.000000Z\tquestdb\t9
                            1970-01-03T06:00:00.000000Z\t\t10
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateStringColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(100000)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET str1 = 'questdb' WHERE ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1");
            assertSql(
                    """
                            count
                            7201
                            """,
                    "select count() from up where str1 = 'questdb'"
            );
            assertSql(
                    """
                            count
                            7201
                            """,
                    "select count() from up where ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1"
            );
        });
    }

    @Test
    public void testUpdateStringColumnUpdate1Value() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 30 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column str2 string", sqlExecutionContext);

            execute("insert into up select * from " +
                    " (select timestamp_sequence('1970-01-01T00:30', 6 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x + 10 as lng2," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str2" +
                    " from long_sequence(10))", sqlExecutionContext);

            update("UPDATE up SET str1 = 'questdb2', str2 = 'questdb2' WHERE ts = '1970-01-01T01:18:00.000000Z'");

            assertSql(
                    """
                            ts\tstr1\tlng2\tstr2
                            1970-01-01T00:00:00.000000Z\t15\t1\t
                            1970-01-01T00:03:00.000000Z\t15\t2\t
                            1970-01-01T00:06:00.000000Z\t\t3\t
                            1970-01-01T00:09:00.000000Z\t1\t4\t
                            1970-01-01T00:12:00.000000Z\t1\t5\t
                            1970-01-01T00:15:00.000000Z\t1\t6\t
                            1970-01-01T00:18:00.000000Z\t190232\t7\t
                            1970-01-01T00:21:00.000000Z\t\t8\t
                            1970-01-01T00:24:00.000000Z\t15\t9\t
                            1970-01-01T00:27:00.000000Z\t\t10\t
                            1970-01-01T00:30:00.000000Z\t\t11\t190232
                            1970-01-01T00:36:00.000000Z\t\t12\t
                            1970-01-01T00:42:00.000000Z\t\t13\t15
                            1970-01-01T00:48:00.000000Z\t15\t14\t
                            1970-01-01T00:54:00.000000Z\trdgb\t15\t
                            1970-01-01T01:00:00.000000Z\t\t16\trdgb
                            1970-01-01T01:06:00.000000Z\t15\t17\t1
                            1970-01-01T01:12:00.000000Z\trdgb\t18\t
                            1970-01-01T01:18:00.000000Z\tquestdb2\t19\tquestdb2
                            1970-01-01T01:24:00.000000Z\t\t20\t15
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateStringColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column str2 string", sqlExecutionContext);
            execute("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x + 10 as lng2," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str2" +
                    " from long_sequence(5))", sqlExecutionContext);

            update("UPDATE up SET str1 = 'questdb1', str2 = 'questdb2' WHERE lng2 in (6, 8, 10, 12, 14)");

            assertSql(
                    """
                            ts\tstr1\tlng2\tstr2
                            1970-01-01T00:00:00.000000Z\t15\t1\t
                            1970-01-01T06:00:00.000000Z\t15\t2\t
                            1970-01-01T12:00:00.000000Z\t\t3\t
                            1970-01-01T18:00:00.000000Z\t1\t4\t
                            1970-01-02T00:00:00.000000Z\t1\t5\t
                            1970-01-02T06:00:00.000000Z\tquestdb1\t6\tquestdb2
                            1970-01-02T12:00:00.000000Z\t190232\t7\t
                            1970-01-02T18:00:00.000000Z\tquestdb1\t8\tquestdb2
                            1970-01-03T00:00:00.000000Z\t15\t9\t
                            1970-01-03T06:00:00.000000Z\tquestdb1\t10\tquestdb2
                            1970-01-07T22:40:00.000000Z\t\t11\t190232
                            1970-01-08T04:40:00.000000Z\tquestdb1\t12\tquestdb2
                            1970-01-08T10:40:00.000000Z\t\t13\t15
                            1970-01-08T16:40:00.000000Z\tquestdb1\t14\tquestdb2
                            1970-01-08T22:40:00.000000Z\trdgb\t15\t
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateStringFixedColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_str('15', null, '190232', 'rdgb', '', '1') as str1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET str1 = concat('questdb', str1), lng2 = -1 WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql(
                    """
                            ts\tstr1\tlng2
                            1970-01-01T00:00:00.000000Z\t15\t1
                            1970-01-01T06:00:00.000000Z\t15\t2
                            1970-01-01T12:00:00.000000Z\tquestdb\t-1
                            1970-01-01T18:00:00.000000Z\t1\t4
                            1970-01-02T00:00:00.000000Z\tquestdb1\t-1
                            1970-01-02T06:00:00.000000Z\t1\t6
                            1970-01-02T12:00:00.000000Z\tquestdb190232\t-1
                            1970-01-02T18:00:00.000000Z\t\t8
                            1970-01-03T00:00:00.000000Z\tquestdb15\t-1
                            1970-01-03T06:00:00.000000Z\t\t10
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateStringToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1," +
                    " rnd_str(10,30,3) s" +
                    " from long_sequence(1000))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));
            execute("alter table up add column v varchar");

            update("UPDATE up SET v = s");

            final String expected = "count\n879\n";
            assertSql(
                    expected,
                    "select count() from up where s is not null"
            );
            assertSql(
                    expected,
                    "select count() from up where v is not null"
            );
        });
    }

    @Test
    public void testUpdateSymbolToChar() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " rnd_symbol('ab', 'bc') sym," +
                            " cast(null as timestamp) ts2" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : "")
            );

            update("UPDATE up \n" +
                    "SET sym = NULLIF(CONCAT(to_str(ts2, 'yyyy-MM-dd'), 'n'), 'n')");

            assertSql(
                    """
                            ts\tsym\tts2
                            1970-01-01T00:00:00.000000Z\t\t
                            1970-01-01T00:00:01.000000Z\t\t
                            1970-01-01T00:00:02.000000Z\t\t
                            1970-01-01T00:00:03.000000Z\t\t
                            1970-01-01T00:00:04.000000Z\t\t
                            """,
                    "up"
            );

            update("UPDATE up \n" +
                    "SET sym = COALESCE(CONCAT(to_str(ts2, 'yyyy-MM-dd'), 'n'), 'n')");

            assertSql(
                    """
                            ts\tsym\tts2
                            1970-01-01T00:00:00.000000Z\tn\t
                            1970-01-01T00:00:01.000000Z\tn\t
                            1970-01-01T00:00:02.000000Z\tn\t
                            1970-01-01T00:00:03.000000Z\tn\t
                            1970-01-01T00:00:04.000000Z\tn\t
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateSymbolToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1," +
                    " rnd_symbol(10,10,10,3) s" +
                    " from long_sequence(1000))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));
            execute("alter table up add column v varchar");

            update("UPDATE up SET v = s");

            final String expected = "count\n735\n";
            assertSql(
                    expected,
                    "select count() from up where s is not null"
            );
            assertSql(
                    expected,
                    "select count() from up where v is not null"
            );
        });
    }

    @Test
    public void testUpdateSymbolWithNotEqualsInWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""));
            assertSql("""
                    symCol\tts\tx
                    WCP\t1970-01-01T00:00:00.000000Z\t1
                    WCP\t1970-01-01T00:00:01.000000Z\t2
                    WCP\t1970-01-01T00:00:02.000000Z\t3
                    VTJ\t1970-01-01T00:00:03.000000Z\t4
                    \t1970-01-01T00:00:04.000000Z\t5
                    """, "up");

            Assert.assertEquals(2, update("UPDATE up SET symCol = 'VTJ' WHERE symCol != 'WCP'"));

            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            VTJ\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateTableNameCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("update UP set x = null where ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t2
                            1970-01-01T00:00:02.000000Z\tnull
                            1970-01-01T00:00:03.000000Z\tnull
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateTableNameContainsSpace() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"віт ер\" as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE \"віт ер\" SET X = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t2
                            1970-01-01T00:00:02.000000Z\tnull
                            1970-01-01T00:00:03.000000Z\tnull
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "\"віт ер\""
            );
        });
    }

    @Test
    public void testUpdateTableWithoutDesignatedTimestamp() throws Exception {
        //this test makes sense for non-WAL tables only, WAL table has to be partitioned
        // and table cannot be partitioned without designated timestamp
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))");

            update("UPDATE up SET x = 12");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t12
                            1970-01-01T00:00:01.000000Z\t12
                            1970-01-01T00:00:02.000000Z\t12
                            1970-01-01T00:00:03.000000Z\t12
                            1970-01-01T00:00:04.000000Z\t12
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            assertException(
                    "UPDATE up SET ts = 1",
                    14,
                    "Designated timestamp column cannot be updated"
            );
        });
    }

    @Test
    public void testUpdateTimestampToStringLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET ts1 = '1970-02-01' WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tts1
                            1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z
                            1970-01-01T00:00:02.000000Z\t1970-02-01T00:00:00.000000Z
                            1970-01-01T00:00:03.000000Z\t1970-02-01T00:00:00.000000Z
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateTimestampToSymbolLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1, " +
                    " cast(to_str(timestamp_sequence(1000000, 1000000), 'yyyy-MM-ddTHH:mm:ss.SSSz') as symbol) as sym" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET ts1 = sym WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tts1\tsym
                            1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:01.000Z
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:02.000Z
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:03.000Z
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000Z
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:05.000Z
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateTimestampToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1," +
                    " '1970-02-01'::varchar v" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET ts1 = v WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tts1\tv
                            1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\t1970-02-01
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\t1970-02-01
                            1970-01-01T00:00:02.000000Z\t1970-02-01T00:00:00.000000Z\t1970-02-01
                            1970-01-01T00:00:03.000000Z\t1970-02-01T00:00:00.000000Z\t1970-02-01
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:04.000000Z\t1970-02-01
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateToBindVar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            sqlExecutionContext.getBindVariableService().setInt(0, 100);
            update("UPDATE up SET x = $1 WHERE x > 1 and x < 4");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t100
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateToNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET x = null WHERE ts > '1970-01-01T00:00:01' and ts < '1970-01-01T00:00:04'");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t2
                            1970-01-01T00:00:02.000000Z\tnull
                            1970-01-01T00:00:03.000000Z\tnull
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateUnsupportedKeyword() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""));

            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            execute("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)");

            assertSql(
                    """
                            symCol2\tts\tx
                            XUX\t1970-01-01T00:00:00.000000Z\t1
                            IBB\t1970-01-01T00:00:01.000000Z\t2
                            IBB\t1970-01-01T00:00:02.000000Z\t3
                            GZS\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "t2"
            );

            assertException(
                    "UPDATE up SET symCol = 'VTJ' JOIN t2 ON up.x = t2.x",
                    29,
                    "FROM, WHERE or EOF expected"
            );
        });
    }

    @Test
    public void testUpdateVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select" +
                            " rnd_varchar('foo','bar') as v," +
                            " timestamp_sequence(0, 1000000) ts," +
                            " x" +
                            " from long_sequence(5)) timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""),
                    sqlExecutionContext
            );

            // char
            update("update up set v = 'a' where v = 'bar'");
            assertSql(
                    """
                            v\tts\tx
                            foo\t1970-01-01T00:00:00.000000Z\t1
                            foo\t1970-01-01T00:00:01.000000Z\t2
                            a\t1970-01-01T00:00:02.000000Z\t3
                            a\t1970-01-01T00:00:03.000000Z\t4
                            a\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            // string
            update("update up set v = 'baz' where v = 'a'");
            assertSql(
                    """
                            v\tts\tx
                            foo\t1970-01-01T00:00:00.000000Z\t1
                            foo\t1970-01-01T00:00:01.000000Z\t2
                            baz\t1970-01-01T00:00:02.000000Z\t3
                            baz\t1970-01-01T00:00:03.000000Z\t4
                            baz\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            // UUID
            update("update up set v = cast('11111111-1111-1111-1111-111111111111' as uuid) where v = 'baz'");
            assertSql(
                    """
                            v\tts\tx
                            foo\t1970-01-01T00:00:00.000000Z\t1
                            foo\t1970-01-01T00:00:01.000000Z\t2
                            11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:02.000000Z\t3
                            11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:03.000000Z\t4
                            11111111-1111-1111-1111-111111111111\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET v1 = 'questdb' WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql(
                    """
                            ts\tv1\tlng2
                            1970-01-01T00:00:00.000000Z\t15\t1
                            1970-01-01T06:00:00.000000Z\t15\t2
                            1970-01-01T12:00:00.000000Z\tquestdb\t3
                            1970-01-01T18:00:00.000000Z\t1\t4
                            1970-01-02T00:00:00.000000Z\tquestdb\t5
                            1970-01-02T06:00:00.000000Z\t1\t6
                            1970-01-02T12:00:00.000000Z\tquestdb\t7
                            1970-01-02T18:00:00.000000Z\t\t8
                            1970-01-03T00:00:00.000000Z\tquestdb\t9
                            1970-01-03T06:00:00.000000Z\t\t10
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateVarcharColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x as lng2" +
                    " from long_sequence(100000)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET v1 = 'questdb' WHERE ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1");
            assertSql(
                    """
                            count
                            7201
                            """,
                    "select count() from up where v1 = 'questdb'"
            );
            assertSql(
                    """
                            count
                            7201
                            """,
                    "select count() from up where ts between '1970-01-01T08' and '1970-01-01T12' and lng2 % 2 = 1"
            );
        });
    }

    @Test
    public void testUpdateVarcharColumnUpdate1Value() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 30 * 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column v2 varchar", sqlExecutionContext);

            execute("insert into up select * from " +
                    " (select timestamp_sequence('1970-01-01T00:30', 6 * 60 * 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x + 10 as lng2," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v2" +
                    " from long_sequence(10))", sqlExecutionContext);

            update("UPDATE up SET v1 = 'questdb2', v2 = 'questdb2' WHERE ts = '1970-01-01T01:18:00.000000Z'");

            assertSql(
                    """
                            ts\tv1\tlng2\tv2
                            1970-01-01T00:00:00.000000Z\t15\t1\t
                            1970-01-01T00:03:00.000000Z\t15\t2\t
                            1970-01-01T00:06:00.000000Z\t\t3\t
                            1970-01-01T00:09:00.000000Z\t1\t4\t
                            1970-01-01T00:12:00.000000Z\t1\t5\t
                            1970-01-01T00:15:00.000000Z\t1\t6\t
                            1970-01-01T00:18:00.000000Z\t190232\t7\t
                            1970-01-01T00:21:00.000000Z\t\t8\t
                            1970-01-01T00:24:00.000000Z\t15\t9\t
                            1970-01-01T00:27:00.000000Z\t\t10\t
                            1970-01-01T00:30:00.000000Z\t\t11\t190232
                            1970-01-01T00:36:00.000000Z\t\t12\t
                            1970-01-01T00:42:00.000000Z\t\t13\t15
                            1970-01-01T00:48:00.000000Z\t15\t14\t
                            1970-01-01T00:54:00.000000Z\trdgb\t15\t
                            1970-01-01T01:00:00.000000Z\t\t16\trdgb
                            1970-01-01T01:06:00.000000Z\t15\t17\t1
                            1970-01-01T01:12:00.000000Z\trdgb\t18\t
                            1970-01-01T01:18:00.000000Z\tquestdb2\t19\tquestdb2
                            1970-01-01T01:24:00.000000Z\t\t20\t15
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateVarcharColumnWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            execute("alter table up add column v2 varchar");
            execute("insert into up select * from " +
                    " (select timestamp_sequence(6*100000000000L, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x + 10 as lng2," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v2" +
                    " from long_sequence(5))");

            update("UPDATE up SET v1 = 'questdb1', v2 = 'questdb2' WHERE lng2 in (6, 8, 10, 12, 14)");

            assertSql(
                    """
                            ts\tv1\tlng2\tv2
                            1970-01-01T00:00:00.000000Z\t15\t1\t
                            1970-01-01T06:00:00.000000Z\t15\t2\t
                            1970-01-01T12:00:00.000000Z\t\t3\t
                            1970-01-01T18:00:00.000000Z\t1\t4\t
                            1970-01-02T00:00:00.000000Z\t1\t5\t
                            1970-01-02T06:00:00.000000Z\tquestdb1\t6\tquestdb2
                            1970-01-02T12:00:00.000000Z\t190232\t7\t
                            1970-01-02T18:00:00.000000Z\tquestdb1\t8\tquestdb2
                            1970-01-03T00:00:00.000000Z\t15\t9\t
                            1970-01-03T06:00:00.000000Z\tquestdb1\t10\tquestdb2
                            1970-01-07T22:40:00.000000Z\t\t11\t190232
                            1970-01-08T04:40:00.000000Z\tquestdb1\t12\tquestdb2
                            1970-01-08T10:40:00.000000Z\t\t13\t15
                            1970-01-08T16:40:00.000000Z\tquestdb1\t14\tquestdb2
                            1970-01-08T22:40:00.000000Z\trdgb\t15\t
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateVarcharFixedColumnPageSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 6 * 60 * 60 * 1000000L) ts," +
                    " rnd_varchar('15', null, '190232', 'rdgb', '', '1') as v1," +
                    " x as lng2" +
                    " from long_sequence(10)" +
                    " )" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET v1 = concat('questdb', v1), lng2 = -1 WHERE ts > '1970-01-01T08' and lng2 % 2 = 1");

            assertSql(
                    """
                            ts\tv1\tlng2
                            1970-01-01T00:00:00.000000Z\t15\t1
                            1970-01-01T06:00:00.000000Z\t15\t2
                            1970-01-01T12:00:00.000000Z\tquestdb\t-1
                            1970-01-01T18:00:00.000000Z\t1\t4
                            1970-01-02T00:00:00.000000Z\tquestdb1\t-1
                            1970-01-02T06:00:00.000000Z\t1\t6
                            1970-01-02T12:00:00.000000Z\tquestdb190232\t-1
                            1970-01-02T18:00:00.000000Z\t\t8
                            1970-01-03T00:00:00.000000Z\tquestdb15\t-1
                            1970-01-03T06:00:00.000000Z\t\t10
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateVarcharToString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " timestamp_sequence(0, 1000000) ts1," +
                    " rnd_varchar(10,30,3) v" +
                    " from long_sequence(1000))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));
            execute("alter table up add column s string");

            update("UPDATE up SET s = v");

            final String expected = "count\n883\n";
            assertSql(
                    expected,
                    "select count() from up where s is not null"
            );
            assertSql(
                    expected,
                    "select count() from up where v is not null"
            );
        });
    }

    @Test
    public void testUpdateWith2TableJoinInWithClause() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            createTablesToJoin("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " rnd_symbol('a', 'b', 'c', null) s," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update("WITH jn AS (select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s)" +
                    "UPDATE up SET x = sm" +
                    " FROM jn " +
                    " WHERE up.s = jn.s");

            assertSql(
                    """
                            ts\ts\tx
                            1970-01-01T00:00:00.000000Z\ta\t101
                            1970-01-01T00:00:01.000000Z\tc\t2
                            1970-01-01T00:00:02.000000Z\tb\t303
                            1970-01-01T00:00:03.000000Z\t\t505
                            1970-01-01T00:00:04.000000Z\tb\t303
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithBindVarInWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(2))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            sqlExecutionContext.getBindVariableService().setInt(0, 2);
            update("UPDATE up SET x = 100 WHERE x < $1");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t100
                            1970-01-01T00:00:01.000000Z\t2
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithFilterAndFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x," +
                    " x as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET y = 10L * x WHERE x > 1 and x < 4");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:01.000000Z\t2\t20
                            1970-01-01T00:00:02.000000Z\t3\t30
                            1970-01-01T00:00:03.000000Z\t4\t4
                            1970-01-01T00:00:04.000000Z\t5\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithFilterAndFunctionValueUpcast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x," +
                    " x as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET y = 10 * x WHERE x > 1 and x < 4");

            assertSql(
                    """
                            ts\tx\ty
                            1970-01-01T00:00:00.000000Z\t1\t1
                            1970-01-01T00:00:01.000000Z\t2\t20
                            1970-01-01T00:00:02.000000Z\t3\t30
                            1970-01-01T00:00:03.000000Z\t4\t4
                            1970-01-01T00:00:04.000000Z\t5\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithFullCrossJoin() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            execute("create table down as" +
                    " (select x * 100 as y," +
                    " timestamp_sequence(0, 1000000) ts" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update("UPDATE up SET x = y" +
                    " FROM down " +
                    " WHERE up.x < 4;");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t100
                            1970-01-01T00:00:01.000000Z\t100
                            1970-01-01T00:00:02.000000Z\t100
                            1970-01-01T00:00:03.000000Z\t4
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithJoin() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            execute("create table down as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x * 100 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update("UPDATE up SET x = y + x" +
                    " FROM down " +
                    " WHERE up.ts = down.ts and x > 1 and x < 4");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t202
                            1970-01-01T00:00:02.000000Z\t303
                            1970-01-01T00:00:03.000000Z\t4
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithJoinAndPostJoinFilter() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            execute("create table down as" +
                    " (select x * 100 as y," +
                    " timestamp_sequence(0, 1000000) ts" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update("UPDATE up SET x = y" +
                    " FROM down " +
                    " WHERE up.ts = down.ts and up.x < down.y and up.x < 4 and down.y > 100;");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t1
                            1970-01-01T00:00:01.000000Z\t200
                            1970-01-01T00:00:02.000000Z\t300
                            1970-01-01T00:00:03.000000Z\t4
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithJoinNoVirtual() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            execute("create table down as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x * 100 as y" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update("UPDATE up SET x = y" +
                    " FROM down " +
                    " WHERE up.ts = down.ts and x < 4");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t100
                            1970-01-01T00:00:01.000000Z\t200
                            1970-01-01T00:00:02.000000Z\t300
                            1970-01-01T00:00:03.000000Z\t4
                            1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithJoinNotEquals() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " x * 100 as x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            execute("create table down as" +
                    " (select x * 50 as y," +
                    " timestamp_sequence(0, 1000000) ts" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY");

            update("UPDATE up SET x = y + 1" +
                    " FROM down " +
                    " WHERE up.x < down.y;");

            assertSql(
                    """
                            ts\tx
                            1970-01-01T00:00:00.000000Z\t151
                            1970-01-01T00:00:01.000000Z\t251
                            1970-01-01T00:00:02.000000Z\t300
                            1970-01-01T00:00:03.000000Z\t400
                            1970-01-01T00:00:04.000000Z\t500
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithJoinUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? "partition by DAY WAL" : ""));

            assertSql("""
                    symCol\tts\tx
                    WCP\t1970-01-01T00:00:00.000000Z\t1
                    WCP\t1970-01-01T00:00:01.000000Z\t2
                    WCP\t1970-01-01T00:00:02.000000Z\t3
                    VTJ\t1970-01-01T00:00:03.000000Z\t4
                    \t1970-01-01T00:00:04.000000Z\t5
                    """, "up");

            execute("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)");

            assertSql(
                    """
                            symCol2\tts\tx
                            XUX\t1970-01-01T00:00:00.000000Z\t1
                            IBB\t1970-01-01T00:00:01.000000Z\t2
                            IBB\t1970-01-01T00:00:02.000000Z\t3
                            GZS\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "t2"
            );

            assertException(
                    "UPDATE up SET symCol = 'VTJ' FROM t2 CROSS JOIN up ON up.x = t2.x",
                    37,
                    "JOIN is not supported on UPDATE statement"
            );
        });
    }

    @Test
    public void testUpdateWithLatestOnUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""));

            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            assertException(
                    "UPDATE up SET symCol = 'ABC' LATEST ON ts PARTITION BY symCol",
                    29,
                    "FROM, WHERE or EOF expected"
            );
        });
    }

    @Test
    public void testUpdateWithSubSelectUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)" + (walEnabled ? " partition by DAY WAL" : ""));

            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            execute("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)");

            assertSql(
                    """
                            symCol2\tts\tx
                            XUX\t1970-01-01T00:00:00.000000Z\t1
                            IBB\t1970-01-01T00:00:01.000000Z\t2
                            IBB\t1970-01-01T00:00:02.000000Z\t3
                            GZS\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "t2"
            );

            assertException(
                    "UPDATE up SET symCol = (select symCol2 from t2 where x = 4)",
                    24,
                    "query is not allowed here"
            );
        });
    }

    @Test
    public void testUpdateWithSymbolJoin() throws Exception {
        // this test makes sense for non-WAL tables only, no joins in UPDATE for WAL table yet
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol)" +
                    " timestamp(ts)");

            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            execute("create table t2 as" +
                    " (select rnd_symbol(3,3,3,3) as symCol2, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5)), index(symCol2)" +
                    " timestamp(ts)");

            assertSql(
                    """
                            symCol2\tts\tx
                            XUX\t1970-01-01T00:00:00.000000Z\t1
                            IBB\t1970-01-01T00:00:01.000000Z\t2
                            IBB\t1970-01-01T00:00:02.000000Z\t3
                            GZS\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "t2"
            );

            Assert.assertEquals(1, update("UPDATE up SET symCol = 'VTJ' FROM t2 WHERE up.symCol = t2.symCol2"));

            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            VTJ\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );
        });
    }

    @Test
    public void testUpdateWithTimestampMicrosArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table metrics (id int, ts timestamp, value double, adjusted_ts timestamp) timestamp(ts) partition by day");

            long baseMicros = 1_704_067_200_000_000L; // 2024-01-01T00:00:00.000000

            execute("insert into metrics values " +
                    "(1, " + baseMicros + ", 10.0, null), " +
                    "(2, " + (baseMicros + 500_000L) + ", 20.0, null), " + // +500ms
                    "(3, " + (baseMicros + 1_000_000L) + ", 30.0, null)"); // +1s

            update("update metrics set adjusted_ts = ts + 2_000_000"); // +2 seconds in microseconds

            assertQueryNoLeakCheck("""
                            id\tts\tvalue\tadjusted_ts
                            1\t2024-01-01T00:00:00.000000Z\t10.0\t2024-01-01T00:00:02.000000Z
                            2\t2024-01-01T00:00:00.500000Z\t20.0\t2024-01-01T00:00:02.500000Z
                            3\t2024-01-01T00:00:01.000000Z\t30.0\t2024-01-01T00:00:03.000000Z
                            """,
                    "select * from metrics", "ts", true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampMixedArithmetic_microsToNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table metrics (id int, ts timestamp, value double, adjusted_ts_ns timestamp_ns) timestamp(ts) partition by day");

            long baseMicros = 1_704_067_200_000_000L; // 2024-01-01T00:00:00.000000

            execute("insert into metrics values " +
                    "(1, " + baseMicros + ", 10.0, null), " +
                    "(2, " + (baseMicros + 500_000L) + ", 20.0, null), " + // +500ms
                    "(3, " + (baseMicros + 1_000_000L) + ", 30.0, null)"); // +1s


            // +2 seconds in microseconds
            update("update metrics set adjusted_ts_ns = ts + 2_000_000");

            assertQueryNoLeakCheck("""
                            id\tts\tvalue\tadjusted_ts_ns
                            1\t2024-01-01T00:00:00.000000Z\t10.0\t2024-01-01T00:00:02.000000000Z
                            2\t2024-01-01T00:00:00.500000Z\t20.0\t2024-01-01T00:00:02.500000000Z
                            3\t2024-01-01T00:00:01.000000Z\t30.0\t2024-01-01T00:00:03.000000000Z
                            """,
                    "select * from metrics", "ts", true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampMixedArithmetic_nanosToMicros() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table metrics (id int, ts_ns timestamp_ns, value double, adjusted_ts timestamp) timestamp(ts_ns) partition by day");

            long baseNanos = 1_704_067_200_000_000_000L; // 2024-01-01T00:00:00.000000000

            execute("insert into metrics values " +
                    "(1, " + baseNanos + ", 10.0, null), " +
                    "(2, " + (baseNanos + 500_000_000L) + ", 20.0, null), " + // +500ms
                    "(3, " + (baseNanos + 1_000_000_000L) + ", 30.0, null)"); // +1s


            // +2 seconds in nanoseconds
            update("update metrics set adjusted_ts = ts_ns + 2_000_000_000");

            assertQueryNoLeakCheck("""
                            id\tts_ns\tvalue\tadjusted_ts
                            1\t2024-01-01T00:00:00.000000000Z\t10.0\t2024-01-01T00:00:02.000000Z
                            2\t2024-01-01T00:00:00.500000000Z\t20.0\t2024-01-01T00:00:02.500000Z
                            3\t2024-01-01T00:00:01.000000000Z\t30.0\t2024-01-01T00:00:03.000000Z
                            """,
                    "select * from metrics", "ts_ns", true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampNsArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table metrics (id int, ts timestamp_ns, value double, adjusted_ts timestamp_ns) timestamp(ts) partition by day");

            long baseNanos = 1_704_067_200_000_000_000L; // 2024-01-01T00:00:00.000000000

            execute("insert into metrics values " +
                    "(1, " + baseNanos + ", 10.0, null), " +
                    "(2, " + (baseNanos + 500_000_000L) + ", 20.0, null), " + // +500ms
                    "(3, " + (baseNanos + 1_000_000_000L) + ", 30.0, null)"); // +1s

            update("update metrics set adjusted_ts = ts + 2_000_000_000"); // +2 seconds in nanoseconds

            assertQueryNoLeakCheck("""
                            id\tts\tvalue\tadjusted_ts
                            1\t2024-01-01T00:00:00.000000000Z\t10.0\t2024-01-01T00:00:02.000000000Z
                            2\t2024-01-01T00:00:00.500000000Z\t20.0\t2024-01-01T00:00:02.500000000Z
                            3\t2024-01-01T00:00:01.000000000Z\t30.0\t2024-01-01T00:00:03.000000000Z
                            """,
                    "select * from metrics", "ts", true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampNsComparisons() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table sensors (sensor_id int, ts_nano timestamp_ns, reading double, status symbol) timestamp(ts_nano) partition by day");

            long baseNanos = 1_640_995_200_000_000_000L; // 2022-01-01T00:00:00.000000000

            execute("insert into sensors values " +
                    "(1, " + baseNanos + ", 25.5, 'active'), " +
                    "(2, " + (baseNanos + 1_111_111L) + ", 26.0, 'active'), " + // +1.111111ms
                    "(3, " + (baseNanos + 999_999_999L) + ", 27.0, 'active'), " + // +999.999999ms
                    "(4, " + (baseNanos + 1_000_000_000L) + ", 28.0, 'inactive'), " + // +1s exactly
                    "(5, " + (baseNanos + 1_000_000_123L) + ", 29.0, 'active')"); // +1.000000123s

            long result = update("update sensors set status = 'calibrated' " +
                    "where ts_nano < " + (baseNanos + 1_000_000_000L));

            Assert.assertEquals(3, result);

            assertQueryNoLeakCheck("""
                            sensor_id\tts_nano\treading\tstatus
                            1\t2022-01-01T00:00:00.000000000Z\t25.5\tcalibrated
                            2\t2022-01-01T00:00:00.001111111Z\t26.0\tcalibrated
                            3\t2022-01-01T00:00:00.999999999Z\t27.0\tcalibrated
                            4\t2022-01-01T00:00:01.000000000Z\t28.0\tinactive
                            5\t2022-01-01T00:00:01.000000123Z\t29.0\tactive
                            """,
                    "select * from sensors", "ts_nano", true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampNsMixedPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events (id int, ts_nano timestamp_ns, ts_micro timestamp, type symbol) timestamp(ts_nano) partition by day");

            long baseMicros = 1_577_836_800_123_456L; // 2020-01-01T00:00:00.123456
            long baseNanos = baseMicros * 1000;

            execute("insert into events values " +
                    "(1, " + baseNanos + ", " + baseMicros + ", 'A'), " +
                    "(2, " + (baseNanos + 789L) + ", " + baseMicros + ", 'B'), " + // +789ns (same microsecond)
                    "(3, " + ((baseMicros + 1000) * 1000) + ", " + (baseMicros + 1000) + ", 'C')"); // +1ms

            update("update events set type = 'precise' " +
                    "where ts_nano != ts_micro::timestamp_ns");

            assertQueryNoLeakCheck("""
                            id\tts_nano\tts_micro\ttype
                            1\t2020-01-01T00:00:00.123456000Z\t2020-01-01T00:00:00.123456Z\tA
                            2\t2020-01-01T00:00:00.123456789Z\t2020-01-01T00:00:00.123456Z\tprecise
                            3\t2020-01-01T00:00:00.124456000Z\t2020-01-01T00:00:00.124456Z\tC
                            """,
                    "select * from events", "ts_nano", true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampNsNulls() throws Exception {
        assertMemoryLeak(() -> {
            long baseNanos = 1_672_531_200_000_000_000L; // 2023-01-01T00:00:00.000000000

            execute("create table logs (id int, ts_nano timestamp_ns, message string, level symbol)");

            execute("insert into logs values " +
                    "(1, " + baseNanos + ", 'Started', 'INFO'), " +
                    "(2, null, 'Error occurred', 'ERROR'), " +
                    "(3, " + (baseNanos + 1_000_000_000L) + ", 'Processing', 'DEBUG'), " +
                    "(4, null, 'Warning issued', 'WARN')");

            long result = update("update logs set level = 'UNKNOWN', ts_nano = " + (baseNanos + 5_000_000_000L) +
                    " where ts_nano is null");

            Assert.assertEquals(2, result);

            assertQueryNoLeakCheck("""
                            id\tts_nano\tmessage\tlevel
                            1\t2023-01-01T00:00:00.000000000Z\tStarted\tINFO
                            2\t2023-01-01T00:00:05.000000000Z\tError occurred\tUNKNOWN
                            3\t2023-01-01T00:00:01.000000000Z\tProcessing\tDEBUG
                            4\t2023-01-01T00:00:05.000000000Z\tWarning issued\tUNKNOWN
                            """,
                    "select * from logs", null, true, true);
        });
    }

    @Test
    public void testUpdateWithTimestampNsWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events (id int, ts_nano timestamp_ns, status symbol, value double) timestamp(ts_nano) partition by day");

            long baseNanos = 1_577_836_800_000_000_000L; // 2020-01-01T00:00:00.000000000

            execute("insert into events values " +
                    "(1, " + baseNanos + ", 'pending', 100.0), " +
                    "(2, " + (baseNanos + 123_456_789L) + ", 'pending', 200.0), " + // +123.456789ms
                    "(3, " + (baseNanos + 500_000_000L) + ", 'completed', 300.0), " + // +500ms
                    "(4, " + (baseNanos + 1_000_000_000L) + ", 'pending', 400.0), " + // +1s
                    "(5, " + (baseNanos + 1_500_000_123L) + ", 'failed', 500.0)"); // +1.500000123s

            update("update events set status = 'processed' " +
                    "where ts_nano between " + baseNanos + " and " + (baseNanos + 600_000_000L));

            assertQueryNoLeakCheck("""
                            id\tts_nano\tstatus\tvalue
                            1\t2020-01-01T00:00:00.000000000Z\tprocessed\t100.0
                            2\t2020-01-01T00:00:00.123456789Z\tprocessed\t200.0
                            3\t2020-01-01T00:00:00.500000000Z\tprocessed\t300.0
                            4\t2020-01-01T00:00:01.000000000Z\tpending\t400.0
                            5\t2020-01-01T00:00:01.500000123Z\tfailed\t500.0
                            """,
                    "select * from events", "ts_nano", true, true);
        });
    }

    @Test
    public void testVarcharToIpv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(case when x = 1 then null else rnd_ipv4() end as varchar) as v," +
                    " cast(null as ipv4) as ip " +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY" + (walEnabled ? " WAL" : ""));

            update("UPDATE up SET ip = v");

            String data = """
                    ts\tv\tip
                    1970-01-01T00:00:00.000000Z\t\t
                    1970-01-01T00:00:01.000000Z\t187.139.150.80\t187.139.150.80
                    1970-01-01T00:00:02.000000Z\t18.206.96.238\t18.206.96.238
                    1970-01-01T00:00:03.000000Z\t92.80.211.65\t92.80.211.65
                    1970-01-01T00:00:04.000000Z\t212.159.205.29\t212.159.205.29
                    """;
            assertSql(data, "up");

            update("UPDATE up set v = 'abc'");
            update("UPDATE up set v = ip");
            assertSql(data, "up");
        });
    }

    private void applyUpdate(UpdateOperation updateOperation) {
        updateOperation.withContext(sqlExecutionContext);
        try (TableWriter tableWriter = getWriter(updateOperation.getTableToken())) {
            updateOperation.apply(tableWriter, false);
        }
    }

    private void createTablesToJoin(String createTableSql) throws SqlException {
        execute(createTableSql);

        execute("create table down1 (s symbol index, y int)" + (walEnabled ? " WAL" : ""));
        execute("insert into down1 values ('a', 1)");
        execute("insert into down1 values ('a', 2)");
        execute("insert into down1 values ('b', 3)");
        execute("insert into down1 values ('b', 4)");
        execute("insert into down1 values (null, 5)");
        execute("insert into down1 values (null, 6)");

        execute("create table  down2 (s symbol index, y long)" + (walEnabled ? " WAL" : ""));
        execute("insert into down2 values ('a', 100)");
        execute("insert into down2 values ('b', 300)");
        execute("insert into down2 values (null, 500)");

        // Check what will be in JOIN between down1 and down2
        assertSql(
                """
                        sm\ts
                        101\ta
                        102\ta
                        303\tb
                        304\tb
                        505\t
                        506\t
                        """,
                "select down1.y + down2.y AS sm, down1.s FROM down1 JOIN down2 ON down1.s = down2.s"
        );
    }

    private void testInsertAfterFailed(boolean closeWriter) throws Exception {
        //this test makes sense for non-WAL tables only, WAL tables behave differently
        Assume.assumeFalse(walEnabled);

        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, "x.d.1") && Utf8s.containsAscii(name, "1970-01-03")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                }
            };
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 24*60*60*1000000L) ts," +
                            " cast(x as int) v," +
                            " cast(x as int) x," +
                            " cast(x as int) z" +
                            " from long_sequence(5))" +
                            " timestamp(ts) partition by DAY"
            );

            assertException(
                    "UPDATE up SET x = 1",
                    0,
                    "could not open read-write"
            );

            if (closeWriter) {
                engine.releaseInactive();
            }

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t1\t1\t1
                            1970-01-02T00:00:00.000000Z\t2\t2\t2
                            1970-01-03T00:00:00.000000Z\t3\t3\t3
                            1970-01-04T00:00:00.000000Z\t4\t4\t4
                            1970-01-05T00:00:00.000000Z\t5\t5\t5
                            """,
                    "up"
            );

            execute("INSERT INTO up VALUES('1970-01-01T00:00:05.000000Z', 10.0, 10.0, 10.0)");
            execute("INSERT INTO up VALUES('1970-01-01T00:00:06.000000Z', 100.0, 100.0, 100.0)");

            assertSql(
                    """
                            ts\tv\tx\tz
                            1970-01-01T00:00:00.000000Z\t1\t1\t1
                            1970-01-01T00:00:05.000000Z\t10\t10\t10
                            1970-01-01T00:00:06.000000Z\t100\t100\t100
                            1970-01-02T00:00:00.000000Z\t2\t2\t2
                            1970-01-03T00:00:00.000000Z\t3\t3\t3
                            1970-01-04T00:00:00.000000Z\t4\t4\t4
                            1970-01-05T00:00:00.000000Z\t5\t5\t5
                            """,
                    "up"
            );
        });
    }

    private void testSymbol_UpdateWithExistingValue(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""));

            update("update up set symCol = 'VTJ' where symCol = 'WCP'");
            assertSql(
                    """
                            symCol\tts\tx
                            VTJ\t1970-01-01T00:00:00.000000Z\t1
                            VTJ\t1970-01-01T00:00:01.000000Z\t2
                            VTJ\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            try (
                    RecordCursorFactory factory = select("up where symCol = 'VTJ'");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(indexed, cursor.isUsingIndex());
                println(factory, cursor);
            }
            TestUtils.assertEquals(
                    """
                            symCol\tts\tx
                            VTJ\t1970-01-01T00:00:00.000000Z\t1
                            VTJ\t1970-01-01T00:00:01.000000Z\t2
                            VTJ\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            """,
                    sink
            );

            assertSql("symCol\tts\tx\n", "up where symCol = 'WCP'");
        });
    }

    private void testSymbolsReplacedDistinct(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""));

            update("update up set symCol = 'ABC' where symCol = 'WCP'");
            assertSql(
                    """
                            symCol\tts\tx
                            ABC\t1970-01-01T00:00:00.000000Z\t1
                            ABC\t1970-01-01T00:00:01.000000Z\t2
                            ABC\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            assertQuery(
                    """
                            symCol
                            
                            ABC
                            VTJ
                            """,
                    "select distinct symCol from up order by symCol",
                    null,
                    true,
                    true
            );

            assertSql(
                    """
                            symCol\tcount
                            \t1
                            ABC\t3
                            VTJ\t1
                            """,
                    "select symCol, count() from up order by symCol"
            );
        });
    }

    private void testSymbols_UpdateNull(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""));

            update("update up set symCol = 'ABC' where symCol is null");
            assertSql(
                    """
                            symCol\tts\tx
                            WCP\t1970-01-01T00:00:00.000000Z\t1
                            WCP\t1970-01-01T00:00:01.000000Z\t2
                            WCP\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            ABC\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            assertSql(
                    """
                            symCol\tts\tx
                            ABC\t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up where symCol = 'ABC'"
            );

            assertSql(
                    "symCol\tts\tx\n",
                    "up where symCol is null"
            );
        });
    }

    private void testSymbols_UpdateWithNewValue(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table up as" +
                    " (select rnd_symbol(3,3,3,3) as symCol, timestamp_sequence(0, 1000000) ts," +
                    " x" +
                    " from long_sequence(5))" + (indexed ? ", index(symCol)" : "") + " timestamp(ts)" +
                    (walEnabled ? " partition by DAY WAL" : ""));

            update("update up set symCol = 'ABC' where symCol = 'WCP'");
            assertSql(
                    """
                            symCol\tts\tx
                            ABC\t1970-01-01T00:00:00.000000Z\t1
                            ABC\t1970-01-01T00:00:01.000000Z\t2
                            ABC\t1970-01-01T00:00:02.000000Z\t3
                            VTJ\t1970-01-01T00:00:03.000000Z\t4
                            \t1970-01-01T00:00:04.000000Z\t5
                            """,
                    "up"
            );

            try (
                    RecordCursorFactory factory = select("up where symCol = 'ABC'");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(indexed, cursor.isUsingIndex());
                println(factory, cursor);
            }
            TestUtils.assertEquals(
                    """
                            symCol\tts\tx
                            ABC\t1970-01-01T00:00:00.000000Z\t1
                            ABC\t1970-01-01T00:00:01.000000Z\t2
                            ABC\t1970-01-01T00:00:02.000000Z\t3
                            """,
                    sink
            );

            assertSql("symCol\tts\tx\n", "up where symCol = 'WCP'");
        });
    }

    private void testUpdateAsyncMode(Consumer<TableWriter> writerConsumer, String errorMsg, String expectedData) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table up as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " x" +
                            " from long_sequence(5))" +
                            " timestamp(ts)"
            );

            CyclicBarrier lockBarrier = new CyclicBarrier(2);
            AtomicBoolean updateFlag = new AtomicBoolean();

            final Thread th = new Thread(() -> {
                try (TableWriter tableWriter = getWriter("up")) {
                    lockBarrier.await(); // table is locked
                    while (!updateFlag.get()) { // update is on writer async cmd queue
                        Os.sleep(1);
                    }
                    writerConsumer.accept(tableWriter);
                    tableWriter.tick();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    Assert.fail();
                } finally {
                    Path.clearThreadLocals();
                }
            });
            th.start();

            lockBarrier.await(); // table is locked

            try {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery cq = compiler.compile("UPDATE up SET x = 123 WHERE x > 1 and x < 4", sqlExecutionContext);
                    try (OperationFuture fut = cq.execute(eventSubSequence)) {
                        Assert.assertEquals(OperationFuture.QUERY_NO_RESPONSE, fut.getStatus());
                        Assert.assertEquals(0, fut.getAffectedRowsCount());
                        updateFlag.set(true); // update is on writer async cmd queue

                        if (errorMsg == null) {
                            fut.await(10 * Micros.SECOND_MILLIS); // 10 seconds timeout
                            Assert.assertEquals(OperationFuture.QUERY_COMPLETE, fut.getStatus());
                            Assert.assertEquals(2, fut.getAffectedRowsCount());
                        } else {
                            try {
                                fut.await(10 * Micros.SECOND_MILLIS); // 10 seconds timeout
                                Assert.fail("Expected exception missing");
                            } catch (TableReferenceOutOfDateException | SqlException e) {
                                Assert.assertEquals(errorMsg, e.getMessage());
                                Assert.assertEquals(0, fut.getAffectedRowsCount());
                            }
                        }
                    }
                }
            } finally {
                updateFlag.set(true);
                th.join();
            }

            assertSql(expectedData, "up");
        });
    }

    @Override
    protected void assertSql(CharSequence expected, CharSequence sql) throws SqlException {
        if (walEnabled) {
            drainWalQueue();
        }
        super.assertSql(expected, sql);
    }

    @Override
    protected long update(CharSequence updateSql) throws SqlException {
        try {
            if (walEnabled) {
                circuitBreaker.setTimeout(1);
            }
            return super.update(updateSql);
        } finally {
            circuitBreaker.setTimeout(DEFAULT_CIRCUIT_BREAKER_TIMEOUT);
        }
    }
}
