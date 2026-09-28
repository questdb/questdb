/*+*****************************************************************************
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

package io.questdb.test.cairo.mig;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.mig.Mig1002;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class Mig1002Test extends AbstractCairoTest {
    private static final int LARGE_SYMBOL_COUNT = 10_000;

    @Test
    public void testKeepsLargeSymbolMapWhenFlagAlreadySet() throws Exception {
        assertMemoryLeak(() -> {
            createLargeSymbolMapTable();
            Assert.assertTrue(containsNullValue("t"));
            final long offsetFileLength = offsetFileLength();
            runMig1002("t");
            Assert.assertTrue(containsNullValue("t"));
            Assert.assertEquals(offsetFileLength, offsetFileLength());
            assertLargeSymbolMapReadable();
        });
    }

    @Test
    public void testKeepsNullFlagUnsetWithoutColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, d SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 'A', 'x'), ('2024-01-06T00:00:00Z', 'B', 'y')");
            execute("ALTER TABLE t DROP COLUMN d");
            Assert.assertFalse(containsNullValue("t"));
            runMig1002("t");
            Assert.assertFalse(containsNullValue("t"));
            assertQuery("SELECT s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("s\nA\nB\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfColumnAbsentFromOlderPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-06T00:00:00Z', 2)");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL");
            execute("INSERT INTO t VALUES ('2024-01-07T00:00:00Z', 3, 'A')");
            unsetNullFlag("t");
            runMig1002("t");
            Assert.assertTrue(containsNullValue("t"));
            assertQuery("SELECT x, s FROM t LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n3\tA\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfConvertedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stale (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO stale VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-05T01:00:00Z', 2)");
            execute("ALTER TABLE stale ADD COLUMN s STRING");
            execute("INSERT INTO stale VALUES ('2024-01-06T00:00:00Z', 3, 'A'), ('2024-01-01T00:00:00Z', 4, 'B')");
            execute("ALTER TABLE stale ALTER COLUMN s TYPE SYMBOL");
            execute("CREATE TABLE keys (k STRING)");
            execute("INSERT INTO keys VALUES ('A'), ('B')");
            Assert.assertTrue(containsNullValue("stale"));
            unsetNullFlag("stale");
            runMig1002("stale");
            Assert.assertTrue(containsNullValue("stale"));

            final String[] predicates = {
                    "",
                    "WHERE s = 'A' OR s = 'B' ",
                    "WHERE s IN (SELECT k FROM keys) ",
                    "WHERE s NOT IN ('A', 'B') ",
                    "WHERE s = 'B' OR s = NULL ",
            };
            final String[] expected = {
                    "x\ts\n4\tB\n2\t\n3\tA\n",
                    "x\ts\n4\tB\n3\tA\n",
                    "x\ts\n4\tB\n3\tA\n",
                    "x\ts\n2\t\n",
                    "x\ts\n4\tB\n2\t\n",
            };
            for (int i = 0; i < predicates.length; i++) {
                assertQuery("SELECT x, s FROM stale " + predicates[i] + "LATEST ON ts PARTITION BY s")
                        .noLeakCheck().inferRandomAccess().sizeMayVary().returns(expected[i]);
            }
        });
    }

    @Test
    public void testRepairsNullFlagOfConvertedColumnInNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stale (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY NONE");
            execute("INSERT INTO stale VALUES ('2024-01-05T00:00:00Z', 1), ('2024-01-05T01:00:00Z', 2)");
            execute("ALTER TABLE stale ADD COLUMN s STRING");
            execute("INSERT INTO stale VALUES ('2024-01-06T00:00:00Z', 3, 'A'), ('2024-01-07T00:00:00Z', 4, 'B')");
            execute("ALTER TABLE stale ALTER COLUMN s TYPE SYMBOL");
            unsetNullFlag("stale");
            runMig1002("stale");
            Assert.assertTrue(containsNullValue("stale"));
            assertQuery("SELECT x, s FROM stale LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n3\tA\n4\tB\n");
            assertQuery("SELECT x, s FROM stale WHERE s NOT IN ('A', 'B') LATEST ON ts PARTITION BY s")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().returns("x\ts\n2\t\n");
        });
    }

    @Test
    public void testRepairsNullFlagOfLargeSymbolMap() throws Exception {
        assertMemoryLeak(() -> {
            createLargeSymbolMapTable();
            unsetNullFlag("t");
            final long offsetFileLength = offsetFileLength();
            runMig1002("t");
            Assert.assertTrue(containsNullValue("t"));
            Assert.assertEquals(offsetFileLength, offsetFileLength());
            assertLargeSymbolMapReadable();
        });
    }

    private static boolean containsNullValue(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getSymbolMapReader(reader.getMetadata().getColumnIndex("s")).containsNullValue();
        }
    }

    private static void createLargeSymbolMapTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES ('2024-01-05T00:00:00Z', 0)");
        execute("ALTER TABLE t ADD COLUMN s SYMBOL");
        execute("INSERT INTO t SELECT '2024-01-06T00:00:00Z'::TIMESTAMP + x, x::INT, 'sym' || x FROM long_sequence("
                + LARGE_SYMBOL_COUNT + ")");
    }

    private static long offsetFileLength() {
        engine.releaseAllWriters();
        try (
                TableReader reader = engine.getReader(engine.verifyTableName("t"));
                Path path = new Path().of(configuration.getDbRoot()).concat(reader.getTableToken())
        ) {
            final int columnIndex = reader.getMetadata().getColumnIndex("s");
            final long columnNameTxn = reader.getColumnVersionReader().getSymbolTableNameTxn(columnIndex);
            return configuration.getFilesFacade().length(TableUtils.offsetFileName(path, "s", columnNameTxn));
        }
    }

    private static void runMig1002(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final FilesFacade ff = configuration.getFilesFacade();
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();
        final long tempMem = Unsafe.malloc(1024, MemoryTag.NATIVE_MIG_MMAP);
        try (
                MemoryMARW rwMem = Vm.getCMARWInstance();
                MemoryARW tempVirtualMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                Path tablePath = new Path().of(configuration.getDbRoot()).concat(token).slash();
                Path tablePath2 = new Path().of(configuration.getDbRoot()).concat(token).slash()
        ) {
            final MigrationContext ctx = new MigrationContext(engine, tempMem, 1024, tempVirtualMem, rwMem);
            ctx.of(tablePath, tablePath2, -1);
            Mig1002.migrate(ctx);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }

    private static void unsetNullFlag(String tableName) {
        try (TableWriter writer = getWriter(tableName)) {
            writer.getSymbolMapWriter(writer.getMetadata().getColumnIndex("s")).updateNullFlag(false);
        }
        engine.releaseAllReaders();
        Assert.assertFalse(containsNullValue(tableName));
    }

    private void assertLargeSymbolMapReadable() throws Exception {
        assertQuery("SELECT count() FROM (SELECT * FROM t LATEST ON ts PARTITION BY s)")
                .noLeakCheck().inferRandomAccess().expectSize().returns("count\n" + (LARGE_SYMBOL_COUNT + 1) + "\n");
        assertQuery("SELECT x, s FROM t WHERE s IN ('sym1', 'sym" + LARGE_SYMBOL_COUNT + "')")
                .noLeakCheck().inferRandomAccess().returns("x\ts\n1\tsym1\n" + LARGE_SYMBOL_COUNT + "\tsym" + LARGE_SYMBOL_COUNT + "\n");
    }
}
