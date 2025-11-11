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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.CreateTableOperationFuture;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

@SuppressWarnings("SameParameterValue")
public class CreateTableTest extends AbstractCairoTest {

    @Test
    public void testCreateNaNColumn() throws Exception {
        assertException(
                "create table a as (select NaN x)",
                0,
                "cannot create NULL-type column, please use type cast, e.g. x::type"
        );
    }

    @Test
    public void testCreateNullColumn() throws Exception {
        assertException(
                "create table a as (select null x)",
                0,
                "cannot create NULL-type column, please use type cast, e.g. x::type"
        );
    }

    @Test
    public void testCreateTableArrayWithMismatchedBrackets() throws Exception {
        assertMemoryLeak(() -> {
            assertException("create table x (arr double[);", 26, "syntax error at column type definition, expected array type: 'DOUBLE[]...', but found: 'double[)'");
            assertException("create table x (arr double[][);", 28, "syntax error at column type definition, expected array type: 'DOUBLE[][]...', but found: 'double[][)'");
            assertException("create table x (arr double]);", 16, "arr has an unmatched `]` - were you trying to define an array?");
            assertException("create table x (arr double[]]);", 16, "arr has an unmatched `]` - were you trying to define an array?");
        });
    }

    @Test
    public void testCreateTableAsSelectIndexSupportedColumnTypeAfterCast() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n",
                "select * from tab",
                "CREATE TABLE tab AS (" +
                        "SELECT CAST(x as SYMBOL) AS x FROM long_sequence(1)" +
                        "), INDEX(x)",
                null,
                true,
                true
        );
    }

    @Test
    public void testCreateTableAsSelectIndexSupportedColumnTypeAfterCast2() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n",
                "select * from tab",
                "CREATE TABLE tab AS (" +
                        "SELECT CAST(x as STRING) AS x FROM long_sequence(1)" +
                        "), CAST(x as SYMBOL), INDEX(x)",
                null,
                true,
                true
        );
    }

    @Test
    public void testCreateTableAsSelectIndexUnsupportedColumnType() throws Exception {
        assertUnsupportedIndexType(
                "CREATE TABLE tab AS (" +
                        "SELECT x FROM long_sequence(1)" +
                        "), INDEX(x)",
                0
        );
    }

    @Test
    public void testCreateTableAsSelectIndexUnsupportedColumnTypeAfterCast() throws Exception {
        assertUnsupportedIndexType(
                "CREATE TABLE tab AS (" +
                        "SELECT CAST(x as STRING) x FROM long_sequence(1)" +
                        "), INDEX(x)",
                0
        );
    }

    @Test
    public void testCreateTableAsSelectIndexUnsupportedColumnTypeAfterCast2() throws Exception {
        assertUnsupportedIndexType(
                "CREATE TABLE tab AS (" +
                        "SELECT CAST(x as SYMBOL) x FROM long_sequence(1)" +
                        "), CAST(x as STRING), INDEX(x)",
                0
        );
    }

    @Test
    public void testCreateTableAsSelectInheritsColumnIndex() throws Exception {
        execute("create table old(s string,sym symbol index, ts timestamp)");
        execute("create table new as (select * from old), index(s), cast(s as symbol), cast(ts as date)");
        assertSql(
                "s\tsym\tts\n",
                "select * from new"
        );

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumn() throws Exception {
        execute("create table old(s string,l long, ts timestamp)");
        execute("create table new as (select * from old), cast(s as symbol), index(s)");
        assertSql("s\tl\tts\n", "new");
        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumnV2() throws Exception {
        execute("create table old(s string,l long, ts timestamp)");
        execute("create table new as (select * from old), index(s), cast(s as symbol)");
        assertSql("s\tl\tts\n", "new");
        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumnV3() throws Exception {
        execute("create table old(s string,l long, ts timestamp)");
        execute("create table new as (select * from old), cast(s as symbol), index(s)");
        assertSql("s\tl\tts\n", "new");
        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndex_v2() throws Exception {
        execute("create table old(s symbol,l long, ts timestamp)");
        execute("create table new as (select * from old), index(s), cast(l as int)");
        assertSql("s\tl\tts\n", "new");
        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndSeparateIndex() throws Exception {
        execute("create table old(s symbol,l long, ts timestamp)");
        execute("create table new as (select * from old), cast(l as int), index(s)");
        assertSql("s\tl\tts\n", "new");
        assertColumnsIndexed("new", "s");
    }

    @Test(expected = SqlException.class)
    public void testCreateTableAsSelectWithCastSymbolToStringAndIndexOnIt() throws Exception {
        execute("create table old(s symbol,l long, ts timestamp)");
        execute("create table new as (select * from old), index(s), cast(s as string)");
        assertSql("s\tl\tts\n", "new");
    }

    @Test(expected = SqlException.class)
    public void testCreateTableAsSelectWithIndexOnSymbolCastedToString() throws Exception {
        execute("create table old(s symbol,l long, ts timestamp)");
        execute("create table new as (select * from old), cast(s as string), index(s)");
        assertSql("s\tl\tts\n", "new");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleCasts() throws Exception {
        execute("create table old(s symbol,l long, ts timestamp)");
        execute("create table new as (select * from old), cast(s as string), cast(l as long), cast(ts as date)");
        assertSql("s\tl\tts\n", "new");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleIndexes() throws Exception {
        execute("create table old(s1 symbol,s2 symbol, s3 symbol)");
        execute("create table new as (select * from old), index(s1), index(s2), index(s3)");
        assertSql("s1\ts2\ts3\n", "new");
        assertColumnsIndexed("new", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexes() throws Exception {
        execute("create table old(s string,sym symbol, ts timestamp)");
        execute("create table new as (select * from old), cast(s as symbol), index(s), cast(ts as date), index(sym), cast(sym as symbol)");
        assertSql("s\tsym\tts\n", "new");
        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexesV2() throws Exception {
        execute("create table old(s string,sym symbol, ts timestamp)");
        execute("create table new as (select * from old), cast(s as symbol), index(s), cast(ts as date), index(sym), cast(sym as symbol)");
        assertSql("s\tsym\tts\n", "select * from new");
        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexesV3() throws Exception {
        execute("create table old(s string,sym symbol, ts timestamp)");
        execute("create table new as (select * from old), index(s), cast(s as symbol), cast(ts as date), index(sym), cast(sym as symbol)");
        assertSql("s\tsym\tts\n", "select * from new");
        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithNoIndex() throws Exception {
        execute("create table old(s1 symbol)");
        execute("create table new as (select * from old)");
        assertSql("s1\n", "select * from new");
    }

    @Test
    public void testCreateTableAsSelectWithOneCast() throws Exception {
        execute("create table old(s1 symbol,s2 symbol, s3 symbol)");
        execute("create table new as (select * from old), cast(s1 as string)");
        assertSql("s1\ts2\ts3\n", "select * from new");
    }

    @Test
    public void testCreateTableAsSelectWithOneIndex() throws Exception {
        execute("create table old(s1 symbol,s2 symbol, s3 symbol)");
        execute("create table new as (select * from old), index(s1)");
        assertSql("s1\ts2\ts3\n", "select * from new");
        assertColumnsIndexed("new", "s1");
    }

    @Test
    public void testCreateTableFromLikeTableWithIndex() throws Exception {
        execute("create table tab (s symbol), index(s)");
        execute("create table x (like tab)");
        assertSql("s\n", "select * from x");
        assertColumnsIndexed("x", "s");
    }

    @Test
    public void testCreateTableFromLikeTableWithMultipleIndices() throws Exception {
        execute("create table tab (s1 symbol, s2 symbol, s3 symbol), index(s1), index(s2), index(s3)");
        execute("create table x(like tab)");
        assertSql("s1\ts2\ts3\n", "select * from x");
        assertColumnsIndexed("x", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableFromLikeTableWithNoIndex() throws Exception {
        execute("create table y (s1 symbol)");
        execute("create table tab (like y)");
        assertSql("s1\n", "select * from tab");
    }

    @Test
    public void testCreateTableFromLikeTableWithPartition() throws Exception {
        execute(
                "create table x (" +
                        "a INT," +
                        "t timestamp) timestamp(t) partition by MONTH"
        );
        execute("create table tab (like x)");
        assertSql("a\tt\n", "select * from tab");
        assertPartitionAndTimestamp();
    }

    @Test
    public void testCreateTableIfNotExistParallel() throws Throwable {
        assertMemoryLeak(() -> {
            int threadCount = 2;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);

            ObjList<Thread> threads = new ObjList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (
                                SqlCompiler compiler = engine.getSqlCompiler();
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                CompiledQuery query = compiler.query().$("create table if not exists tab").$(j).$(" (x int)")
                                        .compile(executionContext);
                                try (Operation op = query.getOperation()) {
                                    try (OperationFuture fut = op.execute(executionContext, null)) {
                                        final TableToken token = ((CreateTableOperationFuture) fut).getTableToken();
                                        assertNotNull(token);
                                        assertEquals("tab" + j, token.getTableName());
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        LOG.error().$("Error in thread").$(e).$();
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(i).start();
            }

            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException("One or more parallel tasks failed", ref.get());
            }
        });
    }

    @Test
    public void testCreateTableIfNotExistParallelWal() throws Throwable {
        assertMemoryLeak(() -> {
            int threadCount = 2;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);

            ObjList<Thread> threads = new ObjList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                execute("create table if not exists tab" + j + " (x int, ts timestamp) timestamp(ts) partition by YEAR WAL", executionContext);
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(i).start();
            }

            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }

            assertEquals(tableCount, getTablesInRegistrySize());
        });
    }

    @Test
    public void testCreateTableIfNotExistsExistingLikeAndDestinationTable() throws Exception {
        execute("create table x (s1 symbol)");
        execute("create table y (s2 symbol)");
        execute("create table if not exists x (like y)");
        assertSql("s1\n", "select * from x");
    }

    @Test
    public void testCreateTableIfNotExistsExistingLikeTable() throws Exception {
        execute("create table y (s2 symbol)");
        execute("create table if not exists x (like y)");
        assertSql("s2\n", "select * from x");
    }

    @Test
    public void testCreateTableLikeTableAllColumnTypes() throws Exception {
        String[][] columnTypes = new String[][]{
                {"a", "INT"},
                {"b", "BYTE"},
                {"c", "SHORT"},
                {"d", "LONG"},
                {"e", "FLOAT"},
                {"f", "DOUBLE"},
                {"g", "DATE"},
                {"h", "BINARY"},
                {"t", "TIMESTAMP"},
                {"n", "TIMESTAMP_NS"},
                {"x", "SYMBOL"},
                {"z", "STRING"},
                {"y", "BOOLEAN"},
                {"l", "LONG256"},
                {"u", "UUID"},
                {"gh1", "GEOHASH(7c)"},
                {"gh2", "GEOHASH(4b)"}
        };

        execute("create table x (" + getColumnDefinitions(columnTypes) + ")");
        execute("create table tab (like x)");
        assertSql("a\tb\tc\td\te\tf\tg\th\tt\tn\tx\tz\ty\tl\tu\tgh1\tgh2\n", "tab");
        assertColumnTypes(columnTypes);
    }

    @Test
    public void testCreateTableLikeTableNotPresent() throws Exception {
        String likeTableName = "y";
        assertException(
                "create table x (like " + likeTableName + ")",
                21,
                "table does not exist [table=" + likeTableName + "]"
        );
    }

    @Test
    public void testCreateTableLikeTableWithCachedSymbol() throws Exception {
        testCreateTableLikeTableWithCachedSymbol(true);
    }

    @Test
    public void testCreateTableLikeTableWithDedup() throws Exception {
        execute(
                "CREATE TABLE foo (" +
                        "ts TIMESTAMP," +
                        "a INT," +
                        "b STRING" +
                        ") " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL " +
                        "DEDUP UPSERT KEYS(ts, a)"
        );
        execute("create table foo_clone ( like foo)");
        assertSql(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                        "a\tINT\tfalse\t0\tfalse\t0\t0\tfalse\ttrue\n" +
                        "b\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n",
                "SHOW COLUMNS FROM foo_clone"
        );
    }

    @Test
    public void testCreateTableLikeTableWithIndexBlockCapacity() throws Exception {
        int indexBlockCapacity = 128;
        execute(
                "create table x (" +
                        "a INT," +
                        "y SYMBOL NOCACHE INDEX CAPACITY " + indexBlockCapacity + "," +
                        "t timestamp) timestamp(t) partition by MONTH"
        );
        execute("create table tab ( like x)");

        assertSql("a\ty\tt\n", "tab");
        assertSymbolParameters(new SymbolParameters(null, false, true, indexBlockCapacity));
    }

    @Test
    public void testCreateTableLikeTableWithMaxUncommittedRowsAndO3MaxLag() throws Exception {
        int maxUncommittedRows = 20;
        int o3MaxLag = 200;
        execute(
                "create table y (s2 symbol, ts TIMESTAMP) timestamp(ts)" +
                        " PARTITION BY DAY" +
                        " WITH maxUncommittedRows = " + maxUncommittedRows + ", o3MaxLag = " + o3MaxLag + "us");
        execute("create table x (like y)");
        assertSql("s2\tts\n", "select * from x");
        assertWithClauseParameters(maxUncommittedRows, o3MaxLag);
    }

    @Test
    public void testCreateTableLikeTableWithNotCachedSymbol() throws Exception {
        testCreateTableLikeTableWithCachedSymbol(false);
    }

    @Test
    public void testCreateTableLikeTableWithSymbolCapacity() throws Exception {
        int symbolCapacity = 128;

        execute(
                "create table x (" +
                        "a INT," +
                        "y SYMBOL CAPACITY " + symbolCapacity + " NOCACHE," +
                        "t timestamp) timestamp(t) partition by MONTH"
        );
        execute("create table tab ( like x)");
        assertSql("a\ty\tt\n", "select * from tab");
        assertSymbolParameters(new SymbolParameters(symbolCapacity, false, false, null));
    }

    @Test
    public void testCreateTableLikeTableWithWALDisabled() throws Exception {
        createTableLike(false);
    }

    @Test
    public void testCreateTableLikeTableWithWALEnabled() throws Exception {
        createTableLike(true);
    }

    @Test
    public void testCreateTableParallel() throws Throwable {
        assertMemoryLeak(() -> {
            int threadCount = 2;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);

            ObjList<Thread> threads = new ObjList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                int threadId = i;
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        for (int j = 0; j < tableCount; j++) {
                            execute("create table tab" + (threadId * tableCount + j) + " (x int)");
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(i).start();
            }

            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }
        });
    }

    @Test
    public void testCreateTableParallelWal() throws Throwable {
        assertMemoryLeak(() -> {
            int threadCount = 2;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);

            ObjList<Thread> threads = new ObjList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    execute("create table tab" + j + " (x int, ts timestamp) timestamp(ts) partition by YEAR WAL", executionContext);
                                } catch (SqlException e) {
                                    TestUtils.assertEquals("table already exists", e.getFlyweightMessage());
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(i).start();
            }

            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }

            assertEquals(tableCount, getTablesInRegistrySize());
        });
    }

    @Test
    public void testCreateTableWithArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (arr double[]);");
            assertSql("ddl\n" +
                            "CREATE TABLE 'x' ( \n" +
                            "\tarr DOUBLE[]\n" +
                            ")\n" +
                            "WITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n",
                    "show create table x;");
        });
    }

    @Test
    public void testCreateTableWithIndex() throws Exception {
        execute("create table tab (s symbol), index(s)");
        assertSql("s\n", "select * from tab");
        assertColumnsIndexed("tab", "s");
    }

    @Test
    public void testCreateTableWithInvalidArrayType() throws Exception {
        assertMemoryLeak(() -> assertException("create table x (ts timestamp, arr varchar[]);", 34, "unsupported array element type [type=VARCHAR]"));
    }

    @Test
    public void testCreateTableWithMultipleIndexes() throws Exception {
        execute("create table tab (s1 symbol, s2 symbol, s3 symbol), index(s1), index(s2), index(s3)");
        assertSql("s1\ts2\ts3\n", "select * from tab");
        assertColumnsIndexed("tab", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableWithNoIndex() throws Exception {
        execute("create table tab (s symbol) ");
        assertSql("s\n", "select * from tab");
    }

    @Test
    public void testCreateTableWithTimestampNSColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ns timestamp_ns, s symbol) timestamp(ns) partition by DAY WAL;");
            assertSql("ddl\n" +
                            "CREATE TABLE 'x' ( \n" +
                            "\tns TIMESTAMP_NS,\n" +
                            "\ts SYMBOL CAPACITY 128 CACHE\n" +
                            ") timestamp(ns) PARTITION BY DAY WAL\n" +
                            "WITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n",
                    "show create table x;");
            execute("create table y (like x);");
            assertSql("ns\ts\n", "select * from x");

            try (TableReader reader = engine.getReader("x")) {
                assertEquals(PartitionBy.DAY, reader.getPartitionedBy());
                assertEquals(0, reader.getMetadata().getTimestampIndex());
            }

            assertSql("ddl\n" +
                            "CREATE TABLE 'y' ( \n" +
                            "\tns TIMESTAMP_NS,\n" +
                            "\ts SYMBOL CAPACITY 128 CACHE\n" +
                            ") timestamp(ns) PARTITION BY DAY WAL\n" +
                            "WITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n",
                    "show create table y;");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ns\tTIMESTAMP_NS\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\tfalse\n"
                    ,
                    "SHOW COLUMNS FROM y"
            );

            execute(
                    "CREATE TABLE z (" +
                            "ns TIMESTAMP_NS," +
                            "a INT," +
                            "b STRING" +
                            ") " +
                            "TIMESTAMP(ns) PARTITION BY DAY WAL " +
                            "DEDUP UPSERT KEYS(ns, a)"
            );
            assertSql("ddl\n" +
                            "CREATE TABLE 'z' ( \n" +
                            "\tns TIMESTAMP_NS,\n" +
                            "\ta INT,\n" +
                            "\tb STRING\n" +
                            ") timestamp(ns) PARTITION BY DAY WAL\n" +
                            "WITH maxUncommittedRows=1000, o3MaxLag=300000000us\n" +
                            "DEDUP UPSERT KEYS(ns,a);\n",
                    "show create table z;");
        });
    }

    @Test
    public void testCreateWalAndNonWalTablesParallel() throws Throwable {
        assertMemoryLeak(() -> {
            int threadCount = 3;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(2 * threadCount);

            ObjList<Thread> threads = new ObjList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    execute("create table tab" + j + " (x int)", executionContext);
                                    execute("drop table tab" + j, executionContext);
                                } catch (SqlException e) {
                                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                                    Os.pause();
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(2 * i).start();

                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    execute("create table tab" + j + " (x int, ts timestamp) timestamp(ts) Partition by DAY WAL ", executionContext);
                                    execute("drop table tab" + j, executionContext);
                                } catch (SqlException e) {
                                    TestUtils.assertContains(e.getFlyweightMessage(), "table already exists");
                                    Os.pause();
                                }
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.get(2 * i + 1).start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }
        });
    }

    private static int getTablesInRegistrySize() {
        ObjHashSet<TableToken> bucket = new ObjHashSet<>();
        engine.getTableTokens(bucket, true);
        return bucket.size();
    }

    private void assertColumnTypes(String[][] columnTypes) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("tab")) {
                TableReaderMetadata metadata = reader.getMetadata();
                for (int i = 0; i < columnTypes.length; i++) {
                    String[] arr = columnTypes[i];
                    assertEquals(arr[0], metadata.getColumnName(i));
                    assertEquals(arr[1], ColumnType.nameOf(metadata.getColumnType(i)));
                }
            }
        });
    }

    private void assertColumnsIndexed(String tableName, String... columnNames) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader r = engine.getReader(tableName)) {
                TableReaderMetadata metadata = r.getMetadata();
                IntList indexed = new IntList();
                indexed.setPos(metadata.getColumnCount());

                for (String columnName : columnNames) {
                    int i = metadata.getColumnIndex(columnName);
                    indexed.setQuick(i, 1);

                    assertTrue("Column " + columnName + " should be indexed!", metadata.isColumnIndexed(i));
                }

                for (int i = 0, len = indexed.size(); i < len; i++) {
                    if (indexed.getQuick(i) == 0) {
                        String columnName = metadata.getColumnName(i);
                        assertFalse("Column " + columnName + " shouldn't be indexed!", metadata.isColumnIndexed(i));
                    }
                }
            }
        });
    }

    private void assertPartitionAndTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("tab")) {
                assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
                assertEquals(1, reader.getMetadata().getTimestampIndex());
            }
        });
    }

    private void assertSymbolParameters(SymbolParameters parameters) throws Exception {
        engine.clear();
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("tab")) {
                if (parameters.symbolCapacity != null) {
                    assertEquals(parameters.symbolCapacity.intValue(), reader.getSymbolMapReader(1).getSymbolCapacity());
                }
                assertEquals(parameters.isCached, reader.getSymbolMapReader(1).isCached());
                assertEquals(parameters.isIndexed, reader.getMetadata().isColumnIndexed(1));
                if (parameters.indexBlockCapacity != null) {
                    assertEquals(parameters.indexBlockCapacity.intValue(), reader.getMetadata().getIndexValueBlockCapacity(1));
                }
            }
        });
    }

    private void assertUnsupportedIndexType(String sql, int position) throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute(sql, sqlExecutionContext);
                fail();
            } catch (SqlException e) {
                assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "indexes are supported only for SYMBOL columns: x");
            }
        });
    }

    private void assertWalEnabled(boolean isWalEnabled) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("x")) {
                assertEquals(isWalEnabled, reader.getMetadata().isWalEnabled());
            }
        });
    }

    private void assertWithClauseParameters(int maxUncommittedRows, int o3MaxLag) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("x")) {
                assertEquals(o3MaxLag, reader.getMetadata().getO3MaxLag());
                assertEquals(maxUncommittedRows, reader.getMetadata().getMaxUncommittedRows());
            }
        });
    }

    private void createTableLike(boolean isWalEnabled) throws Exception {
        String walParameterValue = isWalEnabled ? "WAL" : "BYPASS WAL";
        execute("create table y (s2 symbol, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY " + walParameterValue);
        execute("create table x (like y)");
        assertSql("s2\tts\n", "select * from x");
        assertWalEnabled(isWalEnabled);
    }

    private String getColumnDefinitions(String[][] columnTypes) {
        StringBuilder result = new StringBuilder();
        for (String[] arr : columnTypes) {
            result.append(arr[0]).append(" ").append(arr[1]).append(",");
        }
        result = new StringBuilder(result.substring(0, result.length() - 1));
        return result.toString();
    }

    private void testCreateTableLikeTableWithCachedSymbol(boolean isSymbolCached) throws Exception {
        String symbolCacheParameterValue = isSymbolCached ? "CACHE" : "NOCACHE";

        execute(
                "create table x (" +
                        "a INT," +
                        "y SYMBOL " + symbolCacheParameterValue + "," +
                        "t timestamp) timestamp(t) partition by MONTH"
        );
        execute("create table tab ( like x)");
        assertSql("a\ty\tt\n", "select * from tab");
        SymbolParameters parameters = new SymbolParameters(null, isSymbolCached, false, null);
        assertSymbolParameters(parameters);
    }

    private static class SymbolParameters {
        private final Integer indexBlockCapacity;
        private final boolean isCached;
        private final boolean isIndexed;
        private final Integer symbolCapacity;

        SymbolParameters(Integer symbolCapacity, boolean isCached, boolean isIndexed, Integer indexBlockCapacity) {
            this.symbolCapacity = symbolCapacity;
            this.isCached = isCached;
            this.isIndexed = isIndexed;
            this.indexBlockCapacity = indexBlockCapacity;
        }
    }
}
