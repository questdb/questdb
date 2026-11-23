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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Parquet partitions that QuestDB 10.2.0 updated in place. The fixtures in
 * {@code resources/parquet-stale-ascii-flag} are tables the released 10.2.0 wrote with row groups of 12 rows:
 * <pre>
 * CREATE TABLE t (ts TIMESTAMP, v1 VARCHAR, v2 VARCHAR) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL;
 * INSERT INTO t SELECT '2022-02-25'::TIMESTAMP + (x - 1) * 60_000_000L, 'a' || x, 'b' || x FROM long_sequence(5);
 * INSERT INTO t SELECT '2022-02-25T01:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'c' || x, 'd' || x FROM long_sequence(40);
 * INSERT INTO t SELECT '2022-02-25T02:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'e' || x, 'ü' || x FROM long_sequence(40);
 * </pre>
 * each INSERT applied before the next. The second one rewrote the partition and flagged both columns ASCII in the
 * {@code _pm} header; the third appended row groups in place. In {@code ascii_flipped} it brought the non-ASCII
 * values above, which 10.2.0 then read as ASCII: {@code sum(length(v2))} returned 272 instead of 232. In
 * {@code ascii_kept} its v2 values are {@code 'f' || x}, so the header flags are still true, but nothing in
 * {@code _pm} tells the two tables apart.
 * <p>
 * {@code stale_ascii_934} is a table the released 9.3.4 wrote with row groups of 10 rows:
 * <pre>
 * CREATE TABLE stale_ascii (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * INSERT INTO stale_ascii SELECT '2024-01-01T00:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'a' || x FROM long_sequence(30);
 * INSERT INTO stale_ascii VALUES ('2024-01-02T00:00:00', 'next_day');
 * ALTER TABLE stale_ascii CONVERT PARTITION TO PARQUET LIST '2024-01-01';
 * INSERT INTO stale_ascii VALUES ('2024-01-01T00:02:30', 'é_m1_rg0'), ('2024-01-01T00:12:30', 'ü_m1_rg1');
 * </pre>
 * The last INSERT merged the two rows into row groups 0 and 1 in place, and 9.3.4 kept {@code "ascii": true} for v
 * in the Parquet footer; 9.3.4 has no {@code _pm}. {@code stale_ascii_934_upgraded_by_1020} is the same table after
 * the released 10.2.0 opened it: its upgrade built {@code _pm} from that footer and flagged v ASCII in the header,
 * and 10.2.0 returns {@code length('é_m1_rg0')} as 9.
 */
public class StaleParquetAsciiFlagTest extends AbstractCairoTest {
    private static final String QUERY = "SELECT ts, v1, length(v1) len1, upper(v1) up1, v2, length(v2) len2, upper(v2) up2 FROM ";
    private static final String STALE_FOOTER_ROWS = "SELECT ts, v, length(v) len, upper(v) up FROM stale_ascii WHERE ts IN ('2024-01-01T00:02:30', '2024-01-01T00:12:30')";
    private static final String STALE_FOOTER_SUMMARY = "SELECT count() c, sum(length(v)) len, sum(length_bytes(v)) bytes FROM stale_ascii WHERE ts < '2024-01-02'";

    @Test
    public void testFlippedColumnReadsNoValueAsAscii() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("ascii_flipped~8", "'ü'");
            assertSqlCursors(QUERY + "y", QUERY + "ascii_flipped");
            assertQuery("SELECT count() c, sum(length(v1)) len1, sum(length(v2)) len2 FROM ascii_flipped")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            c\tlen1\tlen2
                            85\t232\t232
                            """);
            assertQuery(QUERY + "ascii_flipped WHERE ts IN ('2022-02-25T01:39:00', '2022-02-25T02:00:00', '2022-02-25T02:39:00')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv1\tlen1\tup1\tv2\tlen2\tup2
                            2022-02-25T01:39:00.000000Z\tc40\t3\tC40\td40\t3\tD40
                            2022-02-25T02:00:00.000000Z\te1\t2\tE1\tü1\t2\tÜ1
                            2022-02-25T02:39:00.000000Z\te40\t3\tE40\tü40\t3\tÜ40
                            """);
            // the header flags of the partition prove nothing, for either column
            assertAsciiFlaggedValueCounts("ascii_flipped", 0, 0);
        });
    }

    @Test
    public void testFlippedColumnStaysCorrectAfterInPlaceAppend() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("ascii_flipped~8", "'ü'");
            final long nameTxn = getPartitionNameTxn("ascii_flipped");
            appendAsciiRows("ascii_flipped");
            Assert.assertEquals("the append must update the parquet partition in place", nameTxn, getPartitionNameTxn("ascii_flipped"));

            assertSqlCursors(QUERY + "y", QUERY + "ascii_flipped");
            // The update cannot vouch for the header: v2 holds non-ASCII values. The chunks it wrote
            // carry their own flags, and its values of v2 are ASCII too.
            assertAsciiFlaggedValueCounts("ascii_flipped", 40, 40);
        });
    }

    @Test
    public void testFlippedColumnStaysCorrectAfterRewrite() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("ascii_flipped~8", "'ü'");
            final long nameTxn = getPartitionNameTxn("ascii_flipped");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, 0);
            appendAsciiRows("ascii_flipped");
            Assert.assertNotEquals("the append must rewrite the parquet partition", nameTxn, getPartitionNameTxn("ascii_flipped"));

            assertSqlCursors(QUERY + "y", QUERY + "ascii_flipped");
            // The rewrite flags the row groups it writes, the 40 new rows. The row groups it copies keep the
            // chunk flags of the old _pm, and 10.2.0 wrote none; the flag of a whole column, in the old
            // header or in the parquet footer, flags no chunk.
            assertAsciiFlaggedValueCounts("ascii_flipped", 40, 40);
        });
    }

    @Test
    public void testKeptColumnsGetAsciiFlagsBackOnInPlaceAppend() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("ascii_kept~9", "'f'");
            assertSqlCursors(QUERY + "y", QUERY + "ascii_kept");
            // an update in place by 10.2.0 does not say whether the header flags still hold
            assertAsciiFlaggedValueCounts("ascii_kept", 0, 0);

            final long nameTxn = getPartitionNameTxn("ascii_kept");
            appendAsciiRows("ascii_kept");
            Assert.assertEquals("the append must update the parquet partition in place", nameTxn, getPartitionNameTxn("ascii_kept"));

            assertSqlCursors(QUERY + "y", QUERY + "ascii_kept");
            // this update knows from the parquet footer that both columns are ASCII and says so
            assertAsciiFlaggedValueCounts("ascii_kept", 125, 125);
        });
    }

    @Test
    public void testStaleFooterFlagOf934IsDroppedByRewrite() throws Exception {
        assertMemoryLeak(() -> {
            unzipFixture("stale_ascii_934_upgraded_by_1020");
            engine.reloadTableNames();
            engine.reconcileTableNameRegistryState();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
            // A _pm of a single snapshot: every release trusts its header, and so does this one until the
            // partition is next rewritten. length() counts the bytes of the two non-ASCII values.
            assertStaleFooterSummary(32, 99, 99);

            // ADD COLUMN, then a row into row group 1: the rewrite merges row group 1, which holds 'ü_m1_rg1',
            // and copies row group 0, which holds 'é_m1_rg0'
            long nameTxn = getPartitionNameTxn("stale_ascii");
            execute("ALTER TABLE stale_ascii ADD COLUMN c INT");
            execute("INSERT INTO stale_ascii (ts, v, c) VALUES ('2024-01-01T00:15:30', 'o3_a1', 1)");
            drainWalQueue();
            Assert.assertNotEquals("the insert must rewrite the parquet partition", nameTxn, getPartitionNameTxn("stale_ascii"));
            assertStaleFooterRowsCorrect();
            assertStaleFooterSummary(33, 102, 104);

            // a second rewrite copies both row groups that hold the non-ASCII values
            nameTxn = getPartitionNameTxn("stale_ascii");
            execute("ALTER TABLE stale_ascii ADD COLUMN d INT");
            execute("INSERT INTO stale_ascii (ts, v, c, d) VALUES ('2024-01-01T00:26:30', 'o3_a2', 2, 2)");
            drainWalQueue();
            Assert.assertNotEquals("the insert must rewrite the parquet partition", nameTxn, getPartitionNameTxn("stale_ascii"));
            assertStaleFooterRowsCorrect();
            assertStaleFooterSummary(34, 107, 109);
        });
    }

    @Test
    public void testStaleFooterFlagOf934IsDroppedByUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            unzipFixture("stale_ascii_934");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();
            // the upgrade of a 9.3.4 database runs the migrations from 427 on; Mig941 builds _pm from the footer
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            engine.reloadTableNames();
            engine.reconcileTableNameRegistryState();
            assertStaleFooterRowsCorrect();
            assertStaleFooterSummary(32, 97, 99);
        });
    }

    private static long getPartitionNameTxn(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    // Unpacks the table directory into the db root, lets the registry discover it from its _name file, and
    // creates y: the same rows in a native partition, where every value carries its own ASCII flag.
    private static void loadFixture(String tableDirName, String lastV2Prefix) throws Exception {
        unzipFixture(tableDirName);
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();

        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 12);
        execute("CREATE TABLE y (ts TIMESTAMP, v1 VARCHAR, v2 VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO y SELECT '2022-02-25'::TIMESTAMP + (x - 1) * 60_000_000L, 'a' || x, 'b' || x FROM long_sequence(5)");
        execute("INSERT INTO y SELECT '2022-02-25T01:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'c' || x, 'd' || x FROM long_sequence(40)");
        execute("INSERT INTO y SELECT '2022-02-25T02:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'e' || x, " + lastV2Prefix + " || x FROM long_sequence(40)");
        drainWalQueue();
    }

    // Unpacks the zip of the fixture into the db root.
    private static void unzipFixture(String zipName) throws Exception {
        final byte[] buffer = new byte[1024 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = StaleParquetAsciiFlagTest.class.getResourceAsStream("/parquet-stale-ascii-flag/" + zipName + ".zip")) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (!ze.isDirectory()) {
                        final File dir = dest.getParentFile();
                        if (!dir.exists()) {
                            Assert.assertTrue(dir.mkdirs());
                        }
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
    }

    private void appendAsciiRows(String tableName) throws Exception {
        final String rows = " SELECT '2022-02-25T03:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'g' || x, 'h' || x FROM long_sequence(40)";
        execute("INSERT INTO " + tableName + rows);
        execute("INSERT INTO y" + rows);
        drainWalQueue();
    }

    // The two non-ASCII values of stale_ascii, each 8 characters in 9 bytes, read right.
    private void assertStaleFooterRowsCorrect() throws Exception {
        assertQuery(STALE_FOOTER_ROWS)
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        ts\tv\tlen\tup
                        2024-01-01T00:02:30.000000Z\té_m1_rg0\t8\tÉ_M1_RG0
                        2024-01-01T00:12:30.000000Z\tü_m1_rg1\t8\tÜ_M1_RG1
                        """);
    }

    // 'a1'..'a30' hold 81 characters in as many bytes; the two non-ASCII values 16 characters in 18 bytes; 'o3_a1'
    // and 'o3_a2' 5 each.
    private void assertStaleFooterSummary(int count, int length, int lengthBytes) throws Exception {
        assertQuery(STALE_FOOTER_SUMMARY)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("c\tlen\tbytes\n" + count + "\t" + length + "\t" + lengthBytes + "\n");
    }

    // Counts the values of v1 and of v2 that a scan of the table returns flagged ASCII.
    private void assertAsciiFlaggedValueCounts(String tableName, int expectedV1Count, int expectedV2Count) throws Exception {
        int v1Count = 0;
        int v2Count = 0;
        try (
                RecordCursorFactory factory = select("SELECT v1, v2 FROM " + tableName);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final Utf8Sequence v1 = record.getVarcharA(0);
                final Utf8Sequence v2 = record.getVarcharA(1);
                if (v1.isAscii()) {
                    v1Count++;
                }
                if (v2.isAscii()) {
                    v2Count++;
                }
            }
        }
        Assert.assertEquals("v1", expectedV1Count, v1Count);
        Assert.assertEquals("v2", expectedV2Count, v2Count);
    }
}
