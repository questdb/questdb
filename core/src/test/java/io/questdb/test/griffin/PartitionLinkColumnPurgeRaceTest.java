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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;

/**
 * DETACH PARTITION and FORCE REINDEX PARQUET hard-link a partition directory entry by entry. The column purge job
 * removes superseded column files from that directory on its own thread, so a file can vanish between the listing
 * and its link. The command must skip it rather than fail, and a WAL table must not suspend.
 */
public class PartitionLinkColumnPurgeRaceTest extends AbstractCairoTest {
    // the file name whose link runs the purge job first, null when disarmed
    private final AtomicReference<String> purgeBeforeLinking = new AtomicReference<>();
    private final AtomicReference<Throwable> purgeFailure = new AtomicReference<>();
    private ColumnPurgeJob purgeJob;
    private volatile boolean hasPurgedLinkSource;
    private long purgeMicros;

    @Override
    @Before
    public void setUp() {
        purgeMicros = 0;
        hasPurgedLinkSource = false;
        purgeBeforeLinking.set(null);
        purgeFailure.set(null);
        setCurrentMicros(0);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        super.setUp();
    }

    @Test
    public void testDetachSkipsColumnFilePurgedDuringLink() throws Exception {
        assertDetachSkipsColumnFilePurgedDuringLink(false);
    }

    @Test
    public void testDetachSkipsColumnFilePurgedDuringLinkWal() throws Exception {
        assertDetachSkipsColumnFilePurgedDuringLink(true);
    }

    @Test
    public void testForceReindexSkipsIndexFilePurgedDuringLink() throws Exception {
        assertMemoryLeak(purgingFacade(), () -> {
            try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                purgeJob = job;
                execute("""
                        CREATE TABLE pq (
                            id INT, ts TIMESTAMP, sym SYMBOL INDEX TYPE BITMAP, tag SYMBOL INDEX TYPE BITMAP, v LONG
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""");
                insertRows();
                execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                // a query keeps the index of sym in use, so its old files wait for the purge job
                try (
                        RecordCursorFactory factory = select("SELECT * FROM pq WHERE sym = 's1'");
                        RecordCursor ignored = factory.getCursor(sqlExecutionContext)
                ) {
                    execute("ALTER TABLE pq ALTER COLUMN sym DROP INDEX");
                }
                engine.releaseAllReaders();

                // the index of tag makes FORCE REINDEX link the partition directory into a new version
                purgeBeforeLinking.set("sym.k");
                execute("ALTER TABLE pq FORCE REINDEX PARQUET");
                assertPurgedDuringLink();
                assertNoPurgedFile(engine.verifyTableName("pq"), "sym.k");

                assertQuery("SELECT id, sym, tag FROM pq WHERE tag = 't1' AND ts < '2024-01-02'").noLeakCheck().returns("""
                        id\tsym\ttag
                        1\ts1\tt1
                        4\ts1\tt1
                        7\ts1\tt1
                        10\ts1\tt1
                        """);
            } finally {
                purgeJob = null;
            }
        });
    }

    private void assertDetachSkipsColumnFilePurgedDuringLink(boolean isWal) throws Exception {
        assertMemoryLeak(purgingFacade(), () -> {
            try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                purgeJob = job;
                execute("CREATE TABLE pq (id INT, ts TIMESTAMP, sym SYMBOL INDEX TYPE BITMAP, tag SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY"
                        + (isWal ? " WAL" : " BYPASS WAL"));
                insertRows();
                drainWalQueue();
                // a query keeps the old version of v in use, so its file waits for the purge job
                try (
                        RecordCursorFactory factory = select("SELECT * FROM pq");
                        RecordCursor ignored = factory.getCursor(sqlExecutionContext)
                ) {
                    execute("UPDATE pq SET v = v + 100 WHERE ts < '2024-01-02'");
                    drainWalQueue();
                }
                engine.releaseAllReaders();
                // Under the ADAPTIVE commit mode, a save point keeps the column versions it names until a later
                // one replaces it. One more commit lets the purge job remove the old version of v.
                execute("INSERT INTO pq VALUES (37, '2024-01-03T23:00:00.000000Z', 's1', 't1', 37)");
                drainWalQueue();

                purgeBeforeLinking.set("v.d");
                execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                final TableToken token = engine.verifyTableName("pq");
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                assertPurgedDuringLink();
                assertQuery("SELECT count() FROM pq").noLeakCheck().noRandomAccess().expectSize().returns("count\n25\n");

                // the detached copy holds the current column version only, and attaches back
                assertNoPurgedFile(token, "v.d");
                try (Path path = new Path(); Path other = new Path()) {
                    path.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").put(DETACHED_DIR_MARKER).$();
                    other.of(configuration.getDbRoot()).concat(token).concat("2024-01-01").put(configuration.getAttachPartitionSuffix()).$();
                    Assert.assertEquals(0, engine.getConfiguration().getFilesFacade().rename(path.$(), other.$()));
                }
                execute("ALTER TABLE pq ATTACH PARTITION LIST '2024-01-01'");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                assertQuery("SELECT id, v FROM pq WHERE ts < '2024-01-02' AND id <= 3").noLeakCheck().returns("""
                        id\tv
                        1\t101
                        2\t102
                        3\t103
                        """);
                assertQuery("SELECT count(), sum(v) FROM pq").noLeakCheck().noRandomAccess().expectSize()
                        // 1..37 plus 100 for each of the 12 rows of 2024-01-01
                        .returns("count\tsum\n37\t1903\n");
            } finally {
                purgeJob = null;
            }
        });
    }

    private void assertNoPurgedFile(TableToken token, String fileName) throws IOException {
        // no directory of the partition, the live one, a new version or the detached copy, holds the purged file
        final java.nio.file.Path tableDir = java.nio.file.Path.of(configuration.getDbRoot(), token.getDirName());
        int partitionDirs = 0;
        try (DirectoryStream<java.nio.file.Path> dirs = java.nio.file.Files.newDirectoryStream(tableDir, "2024-01-01*")) {
            for (java.nio.file.Path dir : dirs) {
                partitionDirs++;
                Assert.assertFalse(dir.resolve(fileName).toString(), java.nio.file.Files.exists(dir.resolve(fileName)));
            }
        }
        Assert.assertTrue(partitionDirs > 0);
    }

    private void assertPurgedDuringLink() {
        final Throwable failure = purgeFailure.get();
        if (failure != null) {
            throw new AssertionError("purge job failed", failure);
        }
        Assert.assertNull("the command did not link the file the purge job removes", purgeBeforeLinking.get());
        Assert.assertTrue("the purge job did not remove the file before its link", hasPurgedLinkSource);
    }

    private void insertRows() throws Exception {
        // 12 rows a day, ids 1..36
        execute("""
                INSERT INTO pq
                SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 's' || (x % 3), 't' || (x % 3), x
                FROM long_sequence(36)""");
    }

    private FilesFacade purgingFacade() {
        return new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                final String fileName = purgeBeforeLinking.get();
                if (
                        fileName != null
                                && Utf8s.containsAscii(src, "2024-01-01")
                                && Utf8s.endsWithAscii(src, Files.SEPARATOR + fileName)
                                && purgeBeforeLinking.compareAndSet(fileName, null)
                ) {
                    runPurgeJob();
                    hasPurgedLinkSource = !exists(src);
                }
                return super.hardLink(src, hardLink);
            }
        };
    }

    private void runPurgeJob() {
        // Another thread, as in production: the command holds thread-local paths the job would reuse
        final Thread thread = new Thread(() -> {
            try {
                engine.releaseInactive();
                for (int i = 0; i < 2; i++) {
                    purgeMicros += 1_000;
                    setCurrentMicros(purgeMicros);
                    purgeJob.run();
                }
            } catch (Throwable th) {
                purgeFailure.set(th);
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
