/*******************************************************************************
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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A table stamps {@link ColumnType#MAX_STORAGE_VERSION} into {@code _meta} at
 * {@link TableUtils#META_OFFSET_VERSION} the moment it gains its first composite partition, and restores
 * {@link ColumnType#VERSION} once the last composite partition is folded back to plain. This is what lets
 * an operator try composite partitions and still downgrade: a binary that only knows {@link
 * ColumnType#VERSION} refuses a table stamped {@code MAX_STORAGE_VERSION} instead of misreading its
 * composite pieces as flat data, while a table that never went composite (or was compacted back) opens
 * unchanged.
 */
public class CompositeStorageVersionTest extends AbstractCairoTest {

    @Test
    public void testDdlKeepsElevatedVersionWhileComposite() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeMiddleDay();
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());

            // A full _meta rewrite (ADD COLUMN) must reproduce the elevated version, not reset it to VERSION.
            execute("ALTER TABLE x ADD COLUMN extra INT");
            drainWalQueue();
            Assert.assertTrue("ADD COLUMN folded the composite partition away", isComposite("2024-01-01"));
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
        });
    }

    @Test
    public void testDropOfTheOnlyCompositePartitionRestoresBaseVersion() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeMiddleDay();
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());

            // 2024-01-01 is the only composite partition; dropping it takes the in-memory count to zero,
            // and the drop's commit downgrades _meta back to the base version.
            execute("ALTER TABLE x DROP PARTITION LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());
        });
    }

    @Test
    public void testPlainTableStaysAtBaseVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                    " FROM long_sequence(10_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            Assert.assertFalse(isComposite("2024-01-01"));
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());
        });
    }

    @Test
    public void testUpgradeOnComposite() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeMiddleDay();
            Assert.assertTrue(isComposite("2024-01-01"));
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
        });
    }

    @Test
    public void testVersionDropsWhenFoldedBackToPlain() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeMiddleDay();
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());

            // Turning merge-append off folds every composite partition back to plain at writer open.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            engine.releaseInactive();
            engine.releaseAllWriters();
            engine.getWriter(engine.verifyTableName("x"), "test").close();

            Assert.assertFalse("a composite partition survived the fold", isComposite("2024-01-01"));
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());
        });
    }

    /**
     * A composite day that is NOT the last partition. Built with the flag ON, the test-suite default.
     */
    private static void createCompositeMiddleDay() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();

        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x SELECT cast(x AS int) + 300_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
        Assert.assertTrue("fixture produced no composite partition", isComposite("2024-01-01"));
    }

    private static boolean isComposite(String day) {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    /**
     * Reads the storage version straight off the {@code _meta} file on disk, to prove the in-place write
     * landed rather than trusting an in-memory cache.
     */
    private static int metaStorageVersion() {
        final TableToken tt = engine.verifyTableName("x");
        try (
                MemoryMR mem = Vm.getCMRInstance();
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(tt).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(TableUtils.META_OFFSET_VERSION);
        }
    }
}
