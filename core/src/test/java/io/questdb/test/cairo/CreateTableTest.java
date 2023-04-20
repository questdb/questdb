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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test interactions between cast and index clauses in CREATE TABLE and CREATE TABLE AS SELECT statements .
 */
@SuppressWarnings("SameParameterValue")
public class CreateTableTest extends AbstractGriffinTest {

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
        assertFailure(
                "CREATE TABLE tab AS (" +
                        "SELECT x FROM long_sequence(1)" +
                        "), INDEX(x)",
                0
        );
    }

    @Test
    public void testCreateTableAsSelectIndexUnsupportedColumnTypeAfterCast() throws Exception {
        assertFailure(
                "CREATE TABLE tab AS (" +
                        "SELECT CAST(x as STRING) x FROM long_sequence(1)" +
                        "), INDEX(x)",
                0
        );
    }

    @Test
    public void testCreateTableAsSelectIndexUnsupportedColumnTypeAfterCast2() throws Exception {
        assertFailure(
                "CREATE TABLE tab AS (" +
                        "SELECT CAST(x as SYMBOL) x FROM long_sequence(1)" +
                        "), CAST(x as STRING), INDEX(x)",
                82
        );
    }

    @Test
    public void testCreateTableAsSelectInheritsColumnIndex() throws Exception {
        assertCompile("create table old(s string,sym symbol index, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as symbol), cast(ts as date)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumn() throws Exception {
        assertCompile("create table old(s string,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumnV2() throws Exception {
        assertCompile("create table old(s string,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as symbol)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndexOnTheSameColumnV3() throws Exception {
        assertCompile("create table old(s string,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndIndex_v2() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(l as int)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test
    public void testCreateTableAsSelectWithCastAndSeparateIndex() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(l as int), index(s)", null);

        assertColumnsIndexed("new", "s");
    }

    @Test(expected = SqlException.class)
    public void testCreateTableAsSelectWithCastSymbolToStringAndIndexOnIt() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as string)", null);
    }

    @Test(expected = SqlException.class)
    public void testCreateTableAsSelectWithIndexOnSymbolCastedToString() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as string), index(s)", null);
    }

    @Test
    public void testCreateTableAsSelectWithMultipleCasts() throws Exception {
        assertCompile("create table old(s symbol,l long, ts timestamp)");
        assertQuery("s\tl\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as string), cast(l as long), cast(ts as date)", null);
    }

    @Test
    public void testCreateTableAsSelectWithMultipleIndexes() throws Exception {
        assertCompile("create table old(s1 symbol,s2 symbol, s3 symbol)");
        assertQuery("s1\ts2\ts3\n", "select * from new", "create table new as (select * from old), index(s1), index(s2), index(s3)", null);

        assertColumnsIndexed("new", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexes() throws Exception {
        assertCompile("create table old(s string,sym symbol, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s), cast(ts as date), index(sym), cast(sym as symbol)", null);

        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexesV2() throws Exception {
        assertCompile("create table old(s string,sym symbol, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), cast(s as symbol), index(s), cast(ts as date), index(sym), cast(sym as symbol)", null);

        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithMultipleInterleavedCastAndIndexesV3() throws Exception {
        assertCompile("create table old(s string,sym symbol, ts timestamp)");
        assertQuery("s\tsym\tts\n", "select * from new",
                "create table new as (select * from old), index(s), cast(s as symbol), cast(ts as date), index(sym), cast(sym as symbol)", null);

        assertColumnsIndexed("new", "s", "sym");
    }

    @Test
    public void testCreateTableAsSelectWithNoIndex() throws Exception {
        assertCompile("create table old(s1 symbol)");
        assertQuery("s1\n", "select * from new", "create table new as (select * from old)", null);
    }

    @Test
    public void testCreateTableAsSelectWithOneCast() throws Exception {
        assertCompile("create table old(s1 symbol,s2 symbol, s3 symbol)");
        assertQuery("s1\ts2\ts3\n", "select * from new", "create table new as (select * from old), cast(s1 as string)", null);
    }

    @Test
    public void testCreateTableAsSelectWithOneIndex() throws Exception {
        assertCompile("create table old(s1 symbol,s2 symbol, s3 symbol)");
        assertQuery("s1\ts2\ts3\n", "select * from new", "create table new as (select * from old), index(s1)", null);

        assertColumnsIndexed("new", "s1");
    }

    @Test
    public void testCreateTableFromLikeTableWithIndex() throws Exception {
        assertCompile("create table tab (s symbol), index(s)");
        assertQuery("s\n", "select * from x", "create table x (like tab)", null);
        assertColumnsIndexed("x", "s");
    }

    @Test
    public void testCreateTableFromLikeTableWithMultipleIndices() throws Exception {
        assertCompile("create table tab (s1 symbol, s2 symbol, s3 symbol), index(s1), index(s2), index(s3)");
        assertQuery("s1\ts2\ts3\n", "select * from x", "create table x(like tab)", null);
        assertColumnsIndexed("x", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableFromLikeTableWithNoIndex() throws Exception {
        assertCompile("create table y (s1 symbol)");
        assertQuery("s1\n", "select * from tab", "create table tab (like y)", null);
    }

    @Test
    public void testCreateTableFromLikeTableWithPartition() throws Exception {
        assertCompile("create table x (" +
                "a INT," +
                "t timestamp) timestamp(t) partition by MONTH");
        assertQuery("a\tt\n", "select * from tab", "create table tab (like x)", "t");
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
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                compiler.compile("create table if not exists tab" + j + " (x int)", executionContext);
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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                compiler.compile("create table if not exists tab" + j + " (x int, ts timestamp) timestamp(ts) partition by YEAR WAL", executionContext);
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

            Assert.assertEquals(tableCount, getTablesInRegistrySize());
        });
    }

    @Test
    public void testCreateTableIfNotExistsExistingLikeAndDestinationTable() throws Exception {
        assertCompile("create table x (s1 symbol)");
        assertCompile("create table y (s2 symbol)");
        assertQuery("s1\n", "select * from x", "create table if not exists x (like y)", null);
    }

    @Test
    public void testCreateTableIfNotExistsExistingLikeTable() throws Exception {
        assertCompile("create table y (s2 symbol)");
        assertQuery("s2\n", "select * from x", "create table if not exists x (like y)", null);
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
                {"x", "SYMBOL"},
                {"z", "STRING"},
                {"y", "BOOLEAN"},
                {"l", "LONG256"},
                {"u", "UUID"},
                {"gh1", "GEOHASH(7c)"},
                {"gh2", "GEOHASH(4b)"}
        };

        assertCompile("create table x (" +
                getColumnDefinitions(columnTypes) + ")"
        );
        assertQuery("a\tb\tc\td\te\tf\tg\th\tt\tx\tz\ty\tl\tu\tgh1\tgh2\n", "select * from tab", "create table tab (like x)", null);
        assertColumnTypes(columnTypes);

    }

    @Test
    public void testCreateTableLikeTableNotPresent() throws Exception {
        String likeTableName = "y";
        try {
            assertQuery("s1\n", "select * from x", "create table x (like " + likeTableName + ")", null);
        } catch (SqlException se) {
            TestUtils.assertContains(se.getFlyweightMessage(), "table does not exist [table=" + likeTableName + "]");
        }
    }

    @Test
    public void testCreateTableLikeTableWithCachedSymbol() throws Exception {
        testCreateTableLikeTableWithCachedSymbol(true);
    }

    @Test
    public void testCreateTableLikeTableWithIndexBlockCapacity() throws Exception {
        int indexBlockCapacity = 128;
        assertCompile("create table x (" +
                "a INT," +
                "y SYMBOL NOCACHE INDEX CAPACITY " + indexBlockCapacity + "," +
                "t timestamp) timestamp(t) partition by MONTH");
        assertQuery("a\ty\tt\n", "select * from tab", "create table tab ( like x)", "t");
        SymbolParameters parameters = new SymbolParameters(null, false, true, indexBlockCapacity);
        assertSymbolParameters(parameters);
    }

    @Test
    public void testCreateTableLikeTableWithMaxUncommittedRowsAndO3MaxLag() throws Exception {
        int maxUncommittedRows = 20;
        int o3MaxLag = 200;
        assertCompile("create table y (s2 symbol, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY WITH maxUncommittedRows = " + maxUncommittedRows + ", o3MaxLag = " + o3MaxLag + "us");
        assertQuery("s2\tts\n", "select * from x", "create table x (like y)", "ts");
        assertWithClauseParameters(maxUncommittedRows, o3MaxLag);
    }

    @Test
    public void testCreateTableLikeTableWithNotCachedSymbol() throws Exception {
        testCreateTableLikeTableWithCachedSymbol(false);
    }

    @Test
    public void testCreateTableLikeTableWithSymbolCapacity() throws Exception {
        int symbolCapacity = 128;

        assertCompile("create table x (" +
                "a INT," +
                "y SYMBOL CAPACITY " + symbolCapacity + " NOCACHE," +
                "t timestamp) timestamp(t) partition by MONTH");
        assertQuery("a\ty\tt\n", "select * from tab", "create table tab ( like x)", "t");
        SymbolParameters parameters = new SymbolParameters(symbolCapacity, false, false, null);
        assertSymbolParameters(parameters);
    }

    @Test
    public void testCreateTableLikeTableWithWALDisabled() throws Exception {
        createWALNonWal(false);
    }

    @Test
    public void testCreateTableLikeTableWithWALEnabled() throws Exception {
        createWALNonWal(true);
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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                compiler.compile("create table tab" + (threadId * tableCount + j) + " (x int)", executionContext);
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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    compiler.compile("create table tab" + j + " (x int, ts timestamp) timestamp(ts) partition by YEAR WAL", executionContext);
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

            Assert.assertEquals(tableCount, getTablesInRegistrySize());
        });
    }

    @Test
    public void testCreateTableWithIndex() throws Exception {
        assertQuery("s\n", "select * from tab", "create table tab (s symbol), index(s)", null);

        assertColumnsIndexed("tab", "s");
    }

    @Test
    public void testCreateTableWithMultipleIndexes() throws Exception {
        assertQuery("s1\ts2\ts3\n", "select * from tab", "create table tab (s1 symbol, s2 symbol, s3 symbol), index(s1), index(s2), index(s3)", null);

        assertColumnsIndexed("tab", "s1", "s2", "s3");
    }

    @Test
    public void testCreateTableWithNoIndex() throws Exception {
        assertQuery("s\n", "select * from tab", "create table tab (s symbol) ", null);
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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    compiler.compile("create table tab" + j + " (x int)", executionContext);
                                    compiler.compile("drop table tab" + j, executionContext);
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
                        try (
                                SqlCompiler compiler = new SqlCompiler(engine);
                                SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                        ) {
                            for (int j = 0; j < tableCount; j++) {
                                try {
                                    compiler.compile("create table tab" + j + " (x int, ts timestamp) timestamp(ts) Partition by DAY WAL ", executionContext);
                                    compiler.compile("drop table tab" + j, executionContext);
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
                    Assert.assertEquals(arr[0], metadata.getColumnName(i));
                    Assert.assertEquals(arr[1], ColumnType.nameOf(metadata.getColumnType(i)));
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

                    Assert.assertTrue("Column " + columnName + " should be indexed!", metadata.isColumnIndexed(i));
                }

                for (int i = 0, len = indexed.size(); i < len; i++) {
                    if (indexed.getQuick(i) == 0) {
                        String columnName = metadata.getColumnName(i);
                        Assert.assertFalse("Column " + columnName + " shouldn't be indexed!", metadata.isColumnIndexed(i));
                    }
                }
            }
        });
    }

    private void assertFailure(String sql, int position) throws Exception {
        assertMemoryLeak(() -> {
            try {
                compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "indexes are supported only for SYMBOL columns: x");
            }
        });
    }

    private void assertPartitionAndTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
                Assert.assertEquals(1, reader.getMetadata().getTimestampIndex());
            }
        });
    }

    private void assertSymbolParameters(SymbolParameters parameters) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("tab")) {
                if (parameters.symbolCapacity != null) {
                    Assert.assertEquals(parameters.symbolCapacity.intValue(), reader.getSymbolMapReader(1).getSymbolCapacity());
                }
                Assert.assertEquals(parameters.isCached, reader.getSymbolMapReader(1).isCached());
                Assert.assertEquals(parameters.isIndexed, reader.getMetadata().isColumnIndexed(1));
                if (parameters.indexBlockCapacity != null) {
                    Assert.assertEquals(parameters.indexBlockCapacity.intValue(), reader.getMetadata().getIndexValueBlockCapacity(1));
                }
            }
        });
    }

    private void assertWalEnabled(boolean isWalEnabled) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("x")) {
                Assert.assertEquals(isWalEnabled, reader.getMetadata().isWalEnabled());
            }
        });
    }

    private void assertWithClauseParameters(int maxUncommittedRows, int o3MaxLag) throws Exception {
        assertMemoryLeak(() -> {
            try (TableReader reader = engine.getReader("x")) {
                Assert.assertEquals(o3MaxLag, reader.getMetadata().getO3MaxLag());
                Assert.assertEquals(maxUncommittedRows, reader.getMetadata().getMaxUncommittedRows());
            }
        });
    }

    private void createWALNonWal(boolean isWalEnabled) throws Exception {
        String walParameterValue = isWalEnabled ? "WAL" : "BYPASS WAL";

        assertCompile("create table y (s2 symbol, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY " + walParameterValue);
        assertQuery("s2\tts\n", "select * from x", "create table x (like y)", "ts");
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

        assertCompile("create table x (" +
                "a INT," +
                "y SYMBOL " + symbolCacheParameterValue + "," +
                "t timestamp) timestamp(t) partition by MONTH");
        assertQuery("a\ty\tt\n", "select * from tab", "create table tab ( like x)", "t");
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

