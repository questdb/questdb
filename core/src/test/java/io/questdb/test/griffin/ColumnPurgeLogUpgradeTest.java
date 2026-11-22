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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * The column purge job on sys.column_versions_purge_log as a version before partition_timestamp_raw left it. Such
 * a version saved the partition timestamp in partition_timestamp only, converted to microseconds.
 * <p>
 * The fixture {@code column-purge-log/legacy_purge_log.zip} holds the directories of t_us, t_ns and the purge log
 * that this code wrote before it saved partition_timestamp_raw, with the column purge retry delay at a day,
 * cairo.o3.partition.split.min.size at 1 byte and both O3 split limits at 20:
 * <pre>
 * CREATE TABLE t_us (ts TIMESTAMP, x LONG, c LONG, vc VARCHAR, s SYMBOL INDEX TYPE BITMAP, p SYMBOL INDEX TYPE POSTING)
 *     TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL;
 * -- one row in 2024-01-01, 2024-01-02 and 2024-01-04, one each hour from 01:00 to 10:00 in 2024-01-03
 * INSERT INTO t_us VALUES ('2024-01-01T02:00:00.000000Z', 1, 10, 'v1', 'A', 'P'), ...,
 *     ('2024-01-04T02:00:00.000000Z', 13, 130, 'v13', 'A', 'P');
 * -- splits 2024-01-03 after its 09:00 row into 2024-01-03T090000-000001.1
 * INSERT INTO t_us VALUES ('2024-01-03T09:30:00.000000Z', 14, 140, 'v14', 'B', 'Q');
 * -- the same for t_ns, with ts TIMESTAMP_NS, whose split partition is 2024-01-03T090000-000000001.1
 * -- with a reader open on both tables, so that the purges wait in the log:
 * ALTER TABLE t_us DROP COLUMN c; ALTER TABLE t_us DROP COLUMN vc; ALTER TABLE t_us DROP COLUMN s;
 * ALTER TABLE t_us DROP COLUMN p;
 * -- the same for t_ns, then one run of the column purge job, which saved the purges in its log
 * </pre>
 */
public class ColumnPurgeLogUpgradeTest extends AbstractCairoTest {
    private static final String[] DROPPED_COLUMNS = {"c", "vc", "s", "p"};

    /**
     * The fixture {@code column-purge-log/legacy_purge_log_same_microsecond.zip} holds the directories of t_same_us
     * and the purge log that the purge job wrote before it saved partition_timestamp_raw, with the column purge retry
     * delay at a day, cairo.o3.partition.split.min.size at 1 byte and both O3 split limits at 20:
     * <pre>
     * CREATE TABLE t_same_us (ts TIMESTAMP_NS, v LONG, c2 LONG, s SYMBOL INDEX TYPE POSTING)
     *     TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL;
     * -- 100 rows from 2022-02-27T00:00, the last at 00:59:59.999999999, 20 rows from 01:00:00.000000010 to
     * -- 01:00:00.000000200 and one at 02:00, c2 = v and s = 'a' in each; then O3 rows at 01:00, which splits the
     * -- day into 2022-02-27T01.6, at 01:00:00.000000500, which splits that partition into
     * -- 2022-02-27T010000-000000201.6, and at 01:00:00.000000005 and 01:00:00.000000300 in one commit, which
     * -- rewrites both partitions under name txn 6
     * -- with a reader open, so that the purges wait in the log:
     * UPDATE t_same_us SET c2 = 0, s = 'b' WHERE ts >= '2022-02-27T01:00:00.000000201Z';
     * -- then one run of the column purge job, which saved the purges of c2 and s in its log, at 01:00 and name
     * -- txn 6
     * </pre>
     */
    @Test
    public void testReplayOfLogWithoutRawPartitionTimestampInSameMicrosecond() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("/column-purge-log/legacy_purge_log_same_microsecond.zip");
            final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("t_same_us").getDirName());
            final File firstDir = new File(tableDir, "2022-02-27T01.6");
            final File secondDir = new File(tableDir, "2022-02-27T010000-000000201.6");
            Assert.assertEquals("c2.d s.d s.pk s.pv.0 s.pv.1 ts.d v.d", fileNames(firstDir));
            Assert.assertEquals("c2.d c2.d.7 s.d s.d.7 s.pk s.pk.7 s.pv.0 s.pv.1 s.pv.7.0 s.pv.7.1 ts.d v.d", fileNames(secondDir));
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
            }
            // The log names the second partition by its microsecond, which starts the first partition too, and by
            // their common name txn. The replay purges the versions the update replaced in the second partition, and
            // keeps those of the first, which it reads.
            Assert.assertEquals("c2.d s.d s.pk s.pv.0 s.pv.1 ts.d v.d", fileNames(firstDir));
            Assert.assertEquals("c2.d.7 s.d.7 s.pk.7 s.pv.7.0 s.pv.7.1 ts.d v.d", fileNames(secondDir));
            // c2 is v and s is 'a' outside the update, 1..100, 201..220, 400, 600 and 300 at 02:00
            assertQuery("SELECT s, count(), sum(c2) FROM t_same_us ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tcount\tsum
                            a\t122\t10260
                            b\t3\t0
                            """);
            assertQuery("SELECT count() FROM t_same_us WHERE s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Index")
                    .returns("""
                            count
                            122
                            """);
        });
    }

    @Test
    public void testReplayOfLogWithoutRawPartitionTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture("/column-purge-log/legacy_purge_log.zip");
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                // the job added partition_timestamp_raw to the log, replayed the rows and truncated the log
                Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                assertQuery("SELECT * FROM '" + purgeJob.getLogTableName() + "'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\ttable_name\tcolumn_name\ttable_id\ttruncate_version\tcolumnType\ttable_partition_by\t"
                                + "updated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\tcompleted\tpartition_timestamp_raw\n");
            }
            // every partition of t_us, the split one too, and the symbol maps of s and p in the table directory
            Assert.assertEquals("", droppedColumnFiles("t_us"));
            // The rows of the split partition of t_ns lost the nanosecond of its timestamp, 09:00:00.000000001: the
            // replay located the partition in a snapshot of the table. The rows name the symbol maps with the root
            // partition marker in microseconds.
            Assert.assertEquals("", droppedColumnFiles("t_ns"));

            final String rows = """
                    ts\tx
                    2024-01-01T02:00:00.000000Z\t1
                    2024-01-02T02:00:00.000000Z\t2
                    2024-01-03T01:00:00.000000Z\t3
                    2024-01-03T02:00:00.000000Z\t4
                    2024-01-03T03:00:00.000000Z\t5
                    2024-01-03T04:00:00.000000Z\t6
                    2024-01-03T05:00:00.000000Z\t7
                    2024-01-03T06:00:00.000000Z\t8
                    2024-01-03T07:00:00.000000Z\t9
                    2024-01-03T08:00:00.000000Z\t10
                    2024-01-03T09:00:00.000000Z\t11
                    2024-01-03T09:30:00.000000Z\t14
                    2024-01-03T10:00:00.000000Z\t12
                    2024-01-04T02:00:00.000000Z\t13
                    """;
            assertQuery("SELECT * FROM t_us")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(rows);
            assertQuery("SELECT * FROM t_ns")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(replaceTimestampSuffix(rows, TIMESTAMP_NS_TYPE_NAME));
        });
    }

    // the files of the dropped columns in the table directory of tableName and in its partition directories
    private static String droppedColumnFiles(String tableName) {
        final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName(tableName).getDirName());
        final TreeSet<String> files = new TreeSet<>();
        final File[] entries = tableDir.listFiles();
        Assert.assertNotNull(tableDir.toString(), entries);
        for (File entry : entries) {
            final File[] partitionFiles = entry.isDirectory() ? entry.listFiles() : new File[]{entry};
            Assert.assertNotNull(entry.toString(), partitionFiles);
            for (File file : partitionFiles) {
                for (String column : DROPPED_COLUMNS) {
                    if (file.getName().startsWith(column + '.')) {
                        files.add(entry.isDirectory() ? entry.getName() + '/' + file.getName() : file.getName());
                    }
                }
            }
        }
        return String.join(" ", files);
    }

    // the names of the files in dir, sorted and space-separated
    private static String fileNames(File dir) {
        final String[] names = dir.list();
        Assert.assertNotNull(dir.toString(), names);
        Arrays.sort(names);
        return String.join(" ", names);
    }

    private static void loadFixture(String resource) throws Exception {
        final byte[] buffer = new byte[64 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = ColumnPurgeLogUpgradeTest.class.getResourceAsStream(resource)) {
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
