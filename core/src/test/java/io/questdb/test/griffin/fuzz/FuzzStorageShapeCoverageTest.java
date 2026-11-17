/*+*****************************************************************************
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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.HashJoinPayloadLayout;
import io.questdb.test.griffin.ParallelHashJoinTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the storage shapes that {@link FuzzTableFactory} gives the fuzzer's primary tables (see
 * {@link FuzzStorage}): over the seeds below, the factory makes non-WAL and non-partitioned tables,
 * tables with column tops, in join keys among other columns, in STRING and VARCHAR columns of native
 * partitions and in Parquet partitions, tables that dropped a column, and tables whose sym dictionary
 * holds far more texts than their rows, whose SYMBOL key a join that probes them translates through
 * the workers' own caches. Every such table holds the rows of its shadow, a plain partitioned WAL table
 * with every column from its first row, and the parallel hash join over them, as probe and as build,
 * with the build's payload read at its rows and from a copy, returns the ordinary plan's rows, in
 * order, and the rows of the same join over the shadows.
 */
public class FuzzStorageShapeCoverageTest extends AbstractCairoTest {
    private static final String[] JOIN_KEYS = {"k", "sym", "lk", "vk"};
    // The seeds this test tries, at most, to see every storage shape; it stops at the first seed by
    // which it saw them all.
    private static final int MAX_SEEDS = 48;

    @Test
    public void testParallelHashJoinOverEveryStorageShape() throws Exception {
        assertMemoryLeak(() -> {
            int columnTopKeys = 0;
            int parquetColumnTopPayloads = 0;
            int nonPartitionedBuilds = 0;
            int nonWalProbes = 0;
            int droppedColumns = 0;
            int nativeVarSizeColumnTopPayloads = 0;
            int paddedSymbolKeyProbes = 0;
            int seed = 0;
            while (seed < MAX_SEEDS && (columnTopKeys == 0 || parquetColumnTopPayloads == 0 || nonPartitionedBuilds == 0
                    || nonWalProbes == 0 || droppedColumns == 0 || nativeVarSizeColumnTopPayloads == 0 || paddedSymbolKeyProbes == 0)) {
                seed++;
                final Rnd rnd = new Rnd(0x5707, seed);
                final FuzzConfig config = new FuzzConfig(rnd);
                Assert.assertTrue(config.isStorageAxesEnabled());
                final FuzzTableFactory factory = new FuzzTableFactory(config);
                final ObjList<FuzzTable> tables = new ObjList<>();
                for (int i = 0; i < config.getNumTables(); i++) {
                    final FuzzTable table = factory.create(rnd, "s" + seed + "_t" + i, AbstractCairoTest::execute, AbstractCairoTest::drainWalQueue);
                    assertStorage(table);
                    tables.add(table);
                }
                for (int p = 0, n = tables.size(); p < n; p++) {
                    for (int b = 0; b < n; b++) {
                        if (p == b) {
                            continue;
                        }
                        final FuzzTable probe = tables.getQuick(p);
                        final FuzzTable build = tables.getQuick(b);
                        for (String key : JOIN_KEYS) {
                            // The probe reads the build's payload at its rows, and from a copy.
                            for (HashJoinPayloadLayout layout : HashJoinPayloadLayout.values()) {
                                layout.apply(node1.getConfigurationOverrides());
                                for (String join : new String[]{" JOIN ", " LEFT JOIN "}) {
                                    assertJoin(probe, build, join, key);
                                }
                            }
                            if (probe.getStorage().isLate(key) || build.getStorage().isLate(key)) {
                                columnTopKeys++;
                            }
                        }
                        if (build.getStorage().hasColumnTops() && build.getParquetMode() != FuzzTableFactory.ParquetMode.NONE) {
                            parquetColumnTopPayloads++;
                        }
                        if (build.getParquetMode() != FuzzTableFactory.ParquetMode.ALL && hasLateVarSizeColumn(build)) {
                            nativeVarSizeColumnTopPayloads++;
                        }
                        if (!build.getStorage().isPartitioned()) {
                            nonPartitionedBuilds++;
                        }
                        if (!probe.getStorage().isWal()) {
                            nonWalProbes++;
                        }
                        if (probe.getStorage().hasDroppedColumn() || build.getStorage().hasDroppedColumn()) {
                            droppedColumns++;
                        }
                        if (probe.getStorage().isJoinKeyDictionaryPadded()) {
                            assertSlotCachedProbe(probe, build);
                            paddedSymbolKeyProbes++;
                        }
                    }
                }
            }
            final String counts = "seeds " + seed + ", column top keys " + columnTopKeys + ", Parquet column top payloads " + parquetColumnTopPayloads
                    + ", non-partitioned builds " + nonPartitionedBuilds + ", non-WAL probes " + nonWalProbes + ", dropped columns " + droppedColumns
                    + ", native var-size column top payloads " + nativeVarSizeColumnTopPayloads + ", padded SYMBOL key probes " + paddedSymbolKeyProbes;
            LOG.info().$("fuzz storage coverage: ").$(counts).$();
            Assert.assertTrue(counts, columnTopKeys > 0);
            Assert.assertTrue(counts, parquetColumnTopPayloads > 0);
            Assert.assertTrue(counts, nonPartitionedBuilds > 0);
            Assert.assertTrue(counts, nonWalProbes > 0);
            Assert.assertTrue(counts, droppedColumns > 0);
            Assert.assertTrue(counts, nativeVarSizeColumnTopPayloads > 0);
            Assert.assertTrue(counts, paddedSymbolKeyProbes > 0);
        });
    }

    // A projection of whether each STRING and VARCHAR column of the table, under the alias, is NULL and
    // whether it is empty. Each reads the value its own way: IS NULL asks a VARCHAR's size, = '' reads
    // the value itself.
    private static String nullFlags(FuzzTable table, String alias) {
        final StringBuilder flags = new StringBuilder();
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            final String name = table.getColumn(i).getName();
            final String ddl = table.getColumn(i).getType().getDdl();
            if (ddl.equals("VARCHAR") || ddl.equals("STRING")) {
                flags.append(", ").append(alias).append('.').append(name).append(" IS NULL ")
                        .append(alias).append('_').append(name).append("_is_null");
                flags.append(", ").append(alias).append('.').append(name).append(" = '' ")
                        .append(alias).append('_').append(name).append("_is_empty");
            }
        }
        return flags.toString();
    }

    // Whether a STRING or VARCHAR column of the table is a column top in some of its rows.
    private static boolean hasLateVarSizeColumn(FuzzTable table) {
        for (int i = 0, n = table.getColumnCount(); i < n; i++) {
            final FuzzColumn column = table.getColumn(i);
            final String ddl = column.getType().getDdl();
            if (table.getStorage().isLate(column.getName()) && (ddl.equals("VARCHAR") || ddl.equals("STRING"))) {
                return true;
            }
        }
        return false;
    }

    // The join of the two tables runs the parallel hash join, whose rows equal the ordinary plan's, in
    // order, and the same join's over the shadows. The printed rows show a NULL and an empty STRING or
    // VARCHAR alike, so each such column of either table adds flags of its NULLs and of its empty
    // values: a column top that the operator read as an empty value differs there.
    private static void assertJoin(FuzzTable probe, FuzzTable build, String join, String key) throws Exception {
        final String select = "SELECT *" + nullFlags(probe, "p") + nullFlags(build, "b") + " FROM ";
        final String sql = select + probe.getName() + " p" + join + build.getName() + " b ON p." + key + " = b." + key;
        ParallelHashJoinTestUtils.assertParallelHashJoin(sql, sqlExecutionContext);
        final String shadowSql = select + probe.getShadow().getName() + " p" + join + build.getShadow().getName() + " b ON p." + key + " = b." + key;
        TestUtils.assertEquals(sql, ParallelHashJoinTestUtils.ordinary(shadowSql, sqlExecutionContext), ParallelHashJoinTestUtils.ordinary(sql, sqlExecutionContext));
    }

    // A LEFT join on sym that probes a table of a padded dictionary, whose rows are far fewer than one
    // per 16 of its texts: the join translates the key through the slots' own caches, and returns the
    // ordinary plan's rows.
    private static void assertSlotCachedProbe(FuzzTable probe, FuzzTable build) throws Exception {
        final String sql = "SELECT * FROM " + probe.getName() + " p LEFT JOIN " + build.getName() + " b ON p.sym = b.sym";
        final String expected = ParallelHashJoinTestUtils.ordinary(sql, sqlExecutionContext);
        final boolean wasEnabled = sqlExecutionContext.isParallelHashJoinEnabled();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = ParallelHashJoinTestUtils.find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.assertTrue(sql, join.getAtom().getStage(0).isSymbolKeySlotCached(0));
            }
            TestUtils.assertEquals(sql, expected, ParallelHashJoinTestUtils.result(factory, sqlExecutionContext));
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(wasEnabled);
        }
    }

    // The table has the storage shape that the factory reports, and its shadow's rows.
    private void assertStorage(FuzzTable table) throws Exception {
        final FuzzStorage storage = table.getStorage();
        assertQuery("SELECT walEnabled, partitionBy FROM tables() WHERE table_name = '" + table.getName() + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("walEnabled\tpartitionBy\n" + storage.isWal() + '\t' + (storage.isPartitioned() ? "DAY" : "NONE") + '\n');
        // The late columns come last, in the shadow as well, and the dropped column is gone.
        final ObjList<FuzzColumn> columns = table.getColumns();
        final ObjList<FuzzColumn> shadowColumns = table.getShadow().getColumns();
        Assert.assertEquals(columns.size(), shadowColumns.size());
        final int lateCount = storage.lateColumns().size();
        try (
                RecordCursorFactory primary = select("SELECT * FROM " + table.getName());
                RecordCursorFactory shadow = select("SELECT * FROM " + table.getShadow().getName())
        ) {
            Assert.assertEquals(columns.size(), primary.getMetadata().getColumnCount());
            for (int i = 0, n = columns.size(); i < n; i++) {
                final String name = columns.getQuick(i).getName();
                Assert.assertEquals(name, primary.getMetadata().getColumnName(i));
                Assert.assertEquals(name, shadow.getMetadata().getColumnName(i));
                Assert.assertEquals(name, i >= n - lateCount, storage.isLate(name));
            }
            Assert.assertEquals(-1, primary.getMetadata().getColumnIndexQuiet(FuzzStorage.DROPPED_COLUMN));
            Assert.assertEquals(
                    table.getName(),
                    ParallelHashJoinTestUtils.result(shadow, sqlExecutionContext),
                    ParallelHashJoinTestUtils.result(primary, sqlExecutionContext)
            );
        }
    }
}
