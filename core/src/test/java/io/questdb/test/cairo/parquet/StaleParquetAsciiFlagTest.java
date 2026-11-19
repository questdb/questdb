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
import io.questdb.cairo.TableReader;
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
 */
public class StaleParquetAsciiFlagTest extends AbstractCairoTest {
    private static final String QUERY = "SELECT ts, v1, length(v1) len1, upper(v1) up1, v2, length(v2) len2, upper(v2) up2 FROM ";

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
            // The update cannot vouch for the header: v2 holds non-ASCII values. The v1 chunks it wrote
            // carry their own flag; it does not scan new values of a column the parquet footer already
            // calls non-ASCII.
            assertAsciiFlaggedValueCounts("ascii_flipped", 40, 0);
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
            // the rewrite builds _pm from the parquet footer, where 10.2.0 kept the flags right
            assertAsciiFlaggedValueCounts("ascii_flipped", 125, 0);
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

    private static long getPartitionNameTxn(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    // Unpacks the table directory into the db root, lets the registry discover it from its _name file, and
    // creates y: the same rows in a native partition, where every value carries its own ASCII flag.
    private static void loadFixture(String tableDirName, String lastV2Prefix) throws Exception {
        final byte[] buffer = new byte[1024 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = StaleParquetAsciiFlagTest.class.getResourceAsStream("/parquet-stale-ascii-flag/" + tableDirName + ".zip")) {
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
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();

        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 12);
        execute("CREATE TABLE y (ts TIMESTAMP, v1 VARCHAR, v2 VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO y SELECT '2022-02-25'::TIMESTAMP + (x - 1) * 60_000_000L, 'a' || x, 'b' || x FROM long_sequence(5)");
        execute("INSERT INTO y SELECT '2022-02-25T01:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'c' || x, 'd' || x FROM long_sequence(40)");
        execute("INSERT INTO y SELECT '2022-02-25T02:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'e' || x, " + lastV2Prefix + " || x FROM long_sequence(40)");
        drainWalQueue();
    }

    private void appendAsciiRows(String tableName) throws Exception {
        final String rows = " SELECT '2022-02-25T03:00:00'::TIMESTAMP + (x - 1) * 60_000_000L, 'g' || x, 'h' || x FROM long_sequence(40)";
        execute("INSERT INTO " + tableName + rows);
        execute("INSERT INTO y" + rows);
        drainWalQueue();
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
