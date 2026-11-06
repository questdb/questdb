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

package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Opens tables written by the released QuestDB 10.0.1 binary whose live {@code _txn} record carries -1 at offset
 * 116. 10.0.1 never writes {@code [116,128)}. Its allocator moved the live area 16 bytes down over an older
 * record, and the low word of that record's unset lagMinTimestamp ({@code Long.MAX_VALUE}) stayed at offset 116.
 * <p>
 * The fixture {@code resources/txn-released/stale-gap-10.0.1.zip} is that binary's own output, written by these
 * statements, each applied before the next:
 * <pre>
 * CREATE TABLE stale_gap_wal (s1 SYMBOL, s2 SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * INSERT INTO stale_gap_wal VALUES ('a', 'b', 1, '2024-01-01T00:00:00.000000Z');
 * INSERT INTO stale_gap_wal VALUES ('a', 'b', 2, '2024-01-01T00:00:01.000000Z');
 * INSERT INTO stale_gap_wal VALUES ('a', 'b', 3, '2024-01-01T00:00:02.000000Z');
 * ALTER TABLE stale_gap_wal DROP COLUMN s1;
 * ALTER TABLE stale_gap_wal DROP COLUMN s2;
 * INSERT INTO stale_gap_wal VALUES (100, '2024-01-01T01:00:00.000000Z');
 *
 * CREATE TABLE stale_gap_bypass (s1 SYMBOL, s2 SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL;
 * ALTER TABLE stale_gap_bypass DROP COLUMN s1;
 * ALTER TABLE stale_gap_bypass DROP COLUMN s2;
 * INSERT INTO stale_gap_bypass (x, ts) VALUES (3, '2024-01-01T23:00:00.000000Z');
 * </pre>
 * The fixture leaves out the WAL segments: 10.0.1 applied every WAL transaction before it shut down.
 */
public class TxnReleasedGapFixtureTest extends AbstractCairoTest {

    @Test
    public void testReleasedNonWalTableTakesInsert() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture();
            assertReleasedGap("stale_gap_bypass");
            assertQuery("SELECT count(), sum(x), max(ts) FROM stale_gap_bypass")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmax
                            1\t3\t2024-01-01T23:00:00.000000Z
                            """);

            execute("INSERT INTO stale_gap_bypass (x, ts) VALUES (1000, '2024-01-01T12:00:00.000000Z')");
            assertQuery("SELECT count(), sum(x), max(ts) FROM stale_gap_bypass")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmax
                            2\t1003\t2024-01-01T23:00:00.000000Z
                            """);
            assertStampedLayout("stale_gap_bypass");
        });
    }

    @Test
    public void testReleasedWalTableReadsAndApplies() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture();
            assertReleasedGap("stale_gap_wal");
            assertQuery("SELECT count(), sum(x), max(ts) FROM stale_gap_wal")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmax
                            4\t106\t2024-01-01T01:00:00.000000Z
                            """);

            execute("INSERT INTO stale_gap_wal VALUES (1000, '2024-01-01T12:00:00.000000Z')");
            drainWalQueue();
            Assert.assertFalse(
                    "the apply job must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("stale_gap_wal"))
            );
            assertQuery("SELECT count(), sum(x), max(ts) FROM stale_gap_wal")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmax
                            5\t1106\t2024-01-01T12:00:00.000000Z
                            """);
            assertStampedLayout("stale_gap_wal");
        });
    }

    // Fails when a regenerated fixture no longer holds what it exists to test: a live record without stamps whose
    // legacy geometry slot is negative.
    private static void assertReleasedGap(String tableName) {
        Assert.assertTrue(
                "fixture: the live record's legacy geometry slot must be negative",
                TxnCorruptionUtils.readLiveAreaLegacyGeometrySlot(engine, tableName) < 0
        );
        Assert.assertEquals("fixture: the live record must carry no checksum stamp",
                0, TxnCorruptionUtils.readLiveAreaChecksumStamp(engine, tableName));
        Assert.assertEquals("fixture: the live record must carry no geometry stamp",
                0, TxnCorruptionUtils.readLiveAreaGeometryStamp(engine, tableName));
    }

    // The first commit moves the table to the stamped layout and carries the -1 forward as geometry version 0.
    private static void assertStampedLayout(String tableName) {
        final long liveTxn = TxnCorruptionUtils.readLiveAreaTxn(engine, tableName);
        Assert.assertEquals(
                (int) liveTxn ^ TableUtils.TX_BODY_CHECKSUM_STAMP_XOR,
                TxnCorruptionUtils.readLiveAreaChecksumStamp(engine, tableName)
        );
        Assert.assertEquals(
                (int) liveTxn ^ TableUtils.TX_GEOMETRY_VERSION_STAMP_XOR,
                TxnCorruptionUtils.readLiveAreaGeometryStamp(engine, tableName)
        );
        final TableToken token = engine.verifyTableName(tableName);
        engine.releaseInactive();
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertEquals(liveTxn, reader.getTxFile().getTxn());
            Assert.assertEquals(0, reader.getTxFile().getGeometryVersion());
        }
    }

    // Unpacks the archived table directories into the db root and lets the registry discover them from their _name
    // files. The fixture's entries are rooted at the table directories.
    private static void loadFixture() throws Exception {
        final byte[] buffer = new byte[64 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = TxnReleasedGapFixtureTest.class.getResourceAsStream("/txn-released/stale-gap-10.0.1.zip")) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (ze.isDirectory()) {
                        Assert.assertTrue(dest.isDirectory() || dest.mkdirs());
                    } else {
                        final File dir = dest.getParentFile();
                        Assert.assertTrue(dir.isDirectory() || dir.mkdirs());
                        try (FileOutputStream fos = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer, 0, buffer.length)) > 0) {
                                fos.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();
    }
}
