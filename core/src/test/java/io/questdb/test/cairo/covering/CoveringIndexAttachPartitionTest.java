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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.IndexBwdNullReader;
import io.questdb.cairo.idx.IndexFwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.griffin.AbstractAlterTableAttachPartitionTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ATTACH PARTITION is the second producer of a column top, and it does not look like the first.
 * {@code ALTER TABLE ADD COLUMN} writes a {@code COL_TOP_DEFAULT_PARTITION} record saying when
 * the column arrived; attaching a directory that holds no file for a column the table has had
 * since {@code CREATE TABLE} writes only a per-partition top ({@code TableWriter.attachPrepare()}
 * calls {@code upsertColumnTop}, and {@code ColumnVersionWriter.overrideColumnVersions()} copies
 * nothing else). So {@code getColumnTopPartitionTimestamp()} answers {@code Long.MIN_VALUE} for a
 * column that does carry a top.
 * <p>
 * A covering scan for the NULL key has no posting to decode over such a partition, so it must run
 * its backup plan; {@code force_use_covering} must see the promise broken and throw. Trusting
 * {@code Long.MIN_VALUE} as "no top anywhere" instead makes both silently drop the attached
 * partition's rows.
 */
public class CoveringIndexAttachPartitionTest extends AbstractAlterTableAttachPartitionTest {

    @Test
    public void testAttachedPartitionMissingIndexedColumnServesNullKey() throws Exception {
        assertMemoryLeak(() -> {
            attachPartitionWithoutIndexedColumn();

            // 2024-01-01 came from the attach and holds no sym at all; 2024-01-02's second row
            // carries an explicit NULL above the top. All three match, and the covering factory
            // has to defer to its backup to say so.
            final String sql = "SELECT ts, sym, val FROM dst_cov_attach WHERE sym = null";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            2024-01-02T01:00:00.000000Z\t\t40.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));

            // The IN-list site reaches the same rows, plus the non-NULL key's own row.
            final String inSql = "SELECT ts, sym, val FROM dst_cov_attach WHERE sym IN (null, 'A')";
            assertSqlCursors(inSql, inSql.replace("SELECT ", "SELECT /*+ no_covering */ "));

            // A non-NULL key needs no backup: every row it can match lives above the top.
            assertQuery("SELECT ts, sym, val FROM dst_cov_attach WHERE sym = 'A'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .expectSize()
                    .withPlanNotContaining("backup: true")
                    .returns("ts\tsym\tval\n2024-01-02T00:00:00.000000Z\tA\t30.0\n");
        });
    }

    @Test
    public void testAttachedPartitionMissingIndexedColumnThrowsUnderForceHint() throws Exception {
        assertMemoryLeak(() -> {
            attachPartitionWithoutIndexedColumn();
            assertThrowsForcedNullKey("SELECT /*+ force_use_covering */ ts, sym, val FROM dst_cov_attach WHERE sym = null");
            assertThrowsForcedNullKey("SELECT /*+ force_use_covering */ ts, sym, val FROM dst_cov_attach WHERE sym IN (null, 'A')");
        });
    }

    @Test
    public void testAttachRemovesCoveringIndexFilesOfNameTxnZeroDroppedWhileDetached() throws Exception {
        // s is the first column added to the empty table, so its column name txn is 0, and its .pc
        // files carry the token 0 that txn NONE also writes. 2024-01-01 leaves with the covering index
        // of s; the index is dropped while the partition is detached. ATTACH removes every index file
        // of s that the partition brings back, the .pc files included.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_att (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE t_att ADD COLUMN s SYMBOL");
            execute("ALTER TABLE t_att ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (val)");
            execute("""
                    INSERT INTO t_att VALUES
                    ('2024-01-01T00:00:00', 10.0, 'A'),
                    ('2024-01-01T01:00:00', 20.0, 'B'),
                    ('2024-01-02T00:00:00', 30.0, 'A')
                    """);
            try (TableReader reader = engine.getReader("t_att")) {
                final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("s"));
                Assert.assertEquals(0, reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex));
            }
            execute("ALTER TABLE t_att DETACH PARTITION LIST '2024-01-01'");
            execute("ALTER TABLE t_att ALTER COLUMN s DROP INDEX");
            final TableToken token = engine.verifyTableName("t_att");
            path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").put(TableUtils.DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            // s.pc0 for val and s.pc1 for ts, each s.pc<N>.0[.<covered txn>].<seal>
            final String[] coverFiles = listIndexFilesOfS(other.toString(), "s.pc");
            Assert.assertEquals(Arrays.toString(coverFiles), 2, coverFiles.length);
            for (String name : coverFiles) {
                Assert.assertTrue(name, name.matches("s\\.pc[01]\\.0(\\.[0-9]+)?\\.[0-9]+"));
            }

            execute("ALTER TABLE t_att ATTACH PARTITION LIST '2024-01-01'");

            final File[] partitionDirs = new File(configuration.getDbRoot().toString(), token.getDirName())
                    .listFiles((_, name) -> name.matches("2024-01-01(\\.[0-9]+)?"));
            Assert.assertNotNull(partitionDirs);
            Assert.assertEquals(Arrays.toString(partitionDirs), 1, partitionDirs.length);
            Assert.assertEquals("[]", Arrays.toString(listIndexFilesOfS(partitionDirs[0].getPath(), "s.p")));
            assertQuery("SELECT ts, val, s FROM t_att WHERE s = 'A'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanNotContaining("Index")
                    .returns("""
                            ts\tval\ts
                            2024-01-01T00:00:00.000000Z\t10.0\tA
                            2024-01-02T00:00:00.000000Z\t30.0\tA
                            """);
        });
    }

    @Test
    public void testAttachRefusesUnreadablePostingKeyFileOfNameTxnZeroWithValueFiles() throws Exception {
        // s is the first column added to the empty table, so its column name txn is 0. ATTACH cannot
        // read the live seal of s from s.pk.0 twice, while the header of the file is intact and the
        // partition holds s.pv.0.<seal>: it refuses the partition rather than drop the sealed data, and
        // attaches it once the key file reads again.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger keyFileOpens = new AtomicInteger();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isArmed.get() && Utf8s.endsWithAscii(name, Files.SEPARATOR + "s.pk.0") && keyFileOpens.getAndIncrement() < 2) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t_att (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE t_att ADD COLUMN s SYMBOL");
            execute("ALTER TABLE t_att ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (val)");
            execute("""
                    INSERT INTO t_att VALUES
                    ('2024-01-01T00:00:00', 10.0, 'A'),
                    ('2024-01-01T01:00:00', 20.0, 'B'),
                    ('2024-01-02T00:00:00', 30.0, 'A')
                    """);
            execute("ALTER TABLE t_att DETACH PARTITION LIST '2024-01-01'");
            final TableToken token = engine.verifyTableName("t_att");
            path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").put(TableUtils.DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            Assert.assertTrue(listIndexFilesOfS(other.toString(), "s.pv.0.").length > 0);
            // without the detached _meta, ATTACH validates the partition files against the table
            Assert.assertTrue(new File(other.toString(), TableUtils.META_FILE_NAME).delete());

            isArmed.set(true);
            try {
                execute("ALTER TABLE t_att ATTACH PARTITION LIST '2024-01-01'");
                Assert.fail("ATTACH must refuse a partition whose posting key file cannot be read");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not read live sealTxn from posting key file but .pv files exist");
                TestUtils.assertContains(e.getFlyweightMessage(), "s.pk.0");
            } finally {
                isArmed.set(false);
            }
            // two failed reads of the live seal, then the header check, which reads
            Assert.assertEquals(3, keyFileOpens.get());

            execute("ALTER TABLE t_att ATTACH PARTITION LIST '2024-01-01'");
            assertQuery("SELECT ts, val, s FROM t_att WHERE s = 'A'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            ts\tval\ts
                            2024-01-01T00:00:00.000000Z\t10.0\tA
                            2024-01-02T00:00:00.000000Z\t30.0\tA
                            """);
        });
    }

    @Test
    public void testStandInIndexReaderServesPartitionThatLostTheColumn() throws Exception {
        // A pooled reader holds a real index reader for 2024-01-01; detaching that partition
        // and attaching a same-dated one without sym leaves the partition count and the slot
        // mapping untouched, so the reader has to answer with a stand-in afterwards.
        // What this does NOT pin is getIndexReader()'s losing-direction swap: the attach
        // changes the partition name txn, so the reload closes the partition and drops the
        // cached reader before the swap can see it. Narrowing that condition to the gaining
        // direction alone leaves this test green. It pins the answer, not the mechanism.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dst_cov_swap (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO dst_cov_swap VALUES
                    ('2024-01-01T00:00:00', 10.0, 'A'),
                    ('2024-01-01T01:00:00', 20.0, 'B'),
                    ('2024-01-02T00:00:00', 30.0, 'A')
                    """);
            createAttachableSource("src_cov_swap", "dst_cov_swap");

            try (TableReader reader = engine.getReader("dst_cov_swap")) {
                final int symIndex = reader.getMetadata().getColumnIndex("sym");
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(2, reader.openPartition(0));
                for (int direction : new int[]{IndexReader.DIR_FORWARD, IndexReader.DIR_BACKWARD}) {
                    final IndexReader real = reader.getIndexReader(0, symIndex, direction);
                    Assert.assertFalse(
                            "partition 0 holds sym, so it must get a real reader, direction=" + direction,
                            real instanceof IndexFwdNullReader || real instanceof IndexBwdNullReader
                    );
                }

                execute("ALTER TABLE dst_cov_swap DETACH PARTITION LIST '2024-01-01'");
                attachSourcePartition("src_cov_swap", "dst_cov_swap");
                Assert.assertTrue(reader.reload());
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(2, reader.openPartition(0));
                Assert.assertTrue(reader.getIndexReader(0, symIndex, IndexReader.DIR_FORWARD) instanceof IndexFwdNullReader);
                Assert.assertTrue(reader.getIndexReader(0, symIndex, IndexReader.DIR_BACKWARD) instanceof IndexBwdNullReader);
            }
        });
    }

    private static String[] listIndexFilesOfS(String dir, String prefix) {
        final String[] names = new File(dir).list((_, name) -> name.startsWith(prefix) && !(prefix.equals("s.pc") && name.startsWith("s.pci")));
        Assert.assertNotNull(dir, names);
        Arrays.sort(names);
        return names;
    }

    private static void assertThrowsForcedNullKey(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // drain
            }
            Assert.fail("expected a CairoException naming the force_use_covering hint");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "force_use_covering");
        }
    }

    /**
     * Builds {@code dst_cov_attach} with a covering index on a column declared in CREATE TABLE,
     * then attaches a partition copied from a table that stops one column short of it.
     */
    private void attachPartitionWithoutIndexedColumn() throws Exception {
        prepareAttachableSourcePartition();
        attachSourcePartition("src_cov_attach", "dst_cov_attach");
    }

    /**
     * Copies {@code srcName}'s 2024-01-01 partition into {@code dstName}'s attachable slot and
     * attaches it. Leaves any open reader alone, so a caller can hold one across the attach.
     */
    private void attachSourcePartition(String srcName, String dstName) throws Exception {
        copyPartitionAndMetadata(
                configuration.getDbRoot(),
                engine.verifyTableName(srcName),
                "2024-01-01",
                configuration.getDbRoot(),
                engine.verifyTableName(dstName).getDirName(),
                "2024-01-01",
                configuration.getAttachPartitionSuffix()
        );
        execute("ALTER TABLE " + dstName + " ATTACH PARTITION LIST '2024-01-01'");
    }

    /**
     * Creates {@code srcName} carrying {@code dstName}'s table id -- ATTACH refuses a partition
     * from a foreign table -- and one column short of it, so the directory it hands over holds
     * no file for the indexed column.
     */
    private void createAttachableSource(String srcName, String dstName) throws Exception {
        final int dstTableId;
        try (TableReader reader = engine.getReader(dstName)) {
            dstTableId = reader.getMetadata().getTableId();
        }
        final TableModel src = new TableModel(configuration, srcName, PartitionBy.DAY)
                .timestamp("ts")
                .col("val", ColumnType.DOUBLE)
                .noWal();
        TestUtils.createTable(engine, src, dstTableId);
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0),
                ('2024-01-01T01:00:00', 20.0)
                """.formatted(srcName));
        engine.releaseAllWriters();
    }

    private void prepareAttachableSourcePartition() throws Exception {
        execute("CREATE TABLE dst_cov_attach (ts TIMESTAMP, val DOUBLE,"
                + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO dst_cov_attach VALUES
                ('2024-01-02T00:00:00', 30.0, 'A'),
                ('2024-01-02T01:00:00', 40.0, NULL)
                """);
        createAttachableSource("src_cov_attach", "dst_cov_attach");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }
}
