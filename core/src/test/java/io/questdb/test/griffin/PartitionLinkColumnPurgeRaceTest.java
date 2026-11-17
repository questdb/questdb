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
import io.questdb.std.FindVisitor;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;

/**
 * DETACH PARTITION and FORCE REINDEX PARQUET hard-link a partition directory entry by entry. The column purge job
 * removes superseded column files from that directory on its own thread, so a file can vanish between the listing
 * and its link. The command must skip it rather than fail, and a WAL table must not suspend.
 */
public class PartitionLinkColumnPurgeRaceTest extends AbstractCairoTest {
    // how many links of the partition go before the one that runs the purge job first, -1 when disarmed
    private final AtomicInteger linksBeforePurge = new AtomicInteger(-1);
    // the file name whose link runs the purge job first, null when disarmed
    private final AtomicReference<String> purgeBeforeLinking = new AtomicReference<>();
    private final AtomicReference<Throwable> purgeFailure = new AtomicReference<>();
    // the links whose source the purge job removed before the walk reached them
    private final AtomicInteger vanishedLinkSources = new AtomicInteger();
    private ColumnPurgeJob purgeJob;
    private volatile boolean hasPurgedLinkSource;
    private volatile boolean hasPurgedDuringWalk;
    // the partition whose links linksBeforePurge counts
    private volatile String linkedDay;
    private long purgeMicros;

    @Override
    @Before
    public void setUp() {
        purgeMicros = 0;
        hasPurgedLinkSource = false;
        hasPurgedDuringWalk = false;
        linkedDay = "2024-01-01";
        linksBeforePurge.set(-1);
        vanishedLinkSources.set(0);
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
    public void testDetachAttachRoundsPurgeAtRandomLink() throws Exception {
        assertDetachAttachRoundsPurgeAtRandomLink(false);
    }

    @Test
    public void testDetachAttachRoundsPurgeAtRandomLinkWal() throws Exception {
        assertDetachAttachRoundsPurgeAtRandomLink(true);
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

    // the column name txn a file name ends with, -1 for none
    private static long nameTxnOf(String fileName) {
        try {
            return Long.parseLong(fileName.substring(fileName.lastIndexOf('.') + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Rounds of a DETACH and an ATTACH of a day whose column, variable-size and index files an UPDATE superseded while
     * a query was open. The DETACH runs the purge job at a random link of the walk; in the first round before the first
     * link, so that every superseded file the purge job removes vanishes before its link. Each day goes round twice:
     * the second time its directory also holds the versions the first round's DETACH linked before the purge job
     * removed them, which nothing purges, and the attach must still read the current version of every column.
     */
    private void assertDetachAttachRoundsPurgeAtRandomLink(boolean isWal) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(purgingFacade(), () -> {
            try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                purgeJob = job;
                // the last day takes writes and is never detached
                final int detachedDayCount = 3;
                execute("CREATE TABLE pq (id INT, ts TIMESTAMP, sym SYMBOL INDEX TYPE BITMAP, tag SYMBOL, v LONG, note VARCHAR) TIMESTAMP(ts) PARTITION BY DAY"
                        + (isWal ? " WAL" : " BYPASS WAL"));
                // 12 rows a day
                execute("INSERT INTO pq SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 's' || (x % 3), 't' || (x % 3), x, 'n' || (x % 3)"
                        + " FROM long_sequence(" + 12 * (detachedDayCount + 1) + ")");
                drainWalQueue();
                final TableToken token = engine.verifyTableName("pq");
                // what each day's rows hold: the UPDATEs that ran on it, and the values they set, null for the inserted
                final int[] updateCounts = new int[detachedDayCount];
                final String[] syms = new String[detachedDayCount];
                final String[] tags = new String[detachedDayCount];
                final String[] notes = new String[detachedDayCount];
                int skippedLinks = 0;
                for (int round = 0; round < 2 * detachedDayCount; round++) {
                    final int dayIndex = round % detachedDayCount;
                    final String day = "2024-01-0" + (dayIndex + 1);
                    final String dayFilter = "ts IN '" + day + "'";
                    final StringSink update = new StringSink();
                    update.put("UPDATE pq SET v = v + 1");
                    if (rnd.nextBoolean()) {
                        syms[dayIndex] = "s" + (10 + round);
                        update.put(", sym = '").put(syms[dayIndex]).put('\'');
                    }
                    if (rnd.nextBoolean()) {
                        tags[dayIndex] = "t" + (10 + round);
                        update.put(", tag = '").put(tags[dayIndex]).put('\'');
                    }
                    if (rnd.nextBoolean()) {
                        notes[dayIndex] = "n" + (10 + round);
                        update.put(", note = '").put(notes[dayIndex]).put('\'');
                    }
                    update.put(" WHERE ").put(dayFilter);
                    updateCounts[dayIndex]++;
                    // a query keeps the old versions in use, so that their files wait for the purge job
                    try (
                            RecordCursorFactory factory = select("SELECT * FROM pq");
                            RecordCursor ignored = factory.getCursor(sqlExecutionContext)
                    ) {
                        execute(update);
                        drainWalQueue();
                    }
                    engine.releaseAllReaders();
                    // a save point of the ADAPTIVE commit mode keeps the versions it names until a later one replaces it
                    execute("INSERT INTO pq VALUES (" + (1000 + round) + ", '2024-01-04T23:00:00.000000Z', 's1', 't1', 0, 'n1')");
                    drainWalQueue();
                    releaseSavePointHolds("pq");

                    hasPurgedDuringWalk = false;
                    linkedDay = day;
                    linksBeforePurge.set(round == 0 ? 0 : rnd.nextInt(8));
                    execute("ALTER TABLE pq DETACH PARTITION LIST '" + day + "'");
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                    final Throwable failure = purgeFailure.get();
                    if (failure != null) {
                        throw new AssertionError("purge job failed", failure);
                    }
                    // the walk is shorter than the drawn link when the purge job did not run
                    linksBeforePurge.set(-1);
                    if (round == 0) {
                        Assert.assertTrue(hasPurgedDuringWalk);
                        Assert.assertTrue("no superseded file vanished before its link", vanishedLinkSources.get() > 0);
                    }
                    skippedLinks += vanishedLinkSources.getAndSet(0);
                    assertQuery("SELECT count() FROM pq WHERE " + dayFilter).noLeakCheck().noRandomAccess().expectSize()
                            .returns("count\n0\n");

                    try (Path path = new Path(); Path other = new Path()) {
                        path.of(configuration.getDbRoot()).concat(token).concat(day).put(DETACHED_DIR_MARKER).$();
                        other.of(configuration.getDbRoot()).concat(token).concat(day).put(configuration.getAttachPartitionSuffix()).$();
                        Assert.assertEquals(0, engine.getConfiguration().getFilesFacade().rename(path.$(), other.$()));
                    }
                    execute("ALTER TABLE pq ATTACH PARTITION LIST '" + day + "'");
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                    // under the ADAPTIVE commit mode, the attach keeps its source until save points cover it, and the
                    // next round of the day renames another detached copy to that name
                    releaseSavePointHolds("pq");

                    // the attached day holds the current version of every column
                    final StringBuilder expected = new StringBuilder("id\tv\tsym\ttag\tnote\n");
                    for (int id = 12 * dayIndex + 1, hi = 12 * dayIndex + 12; id <= hi; id++) {
                        expected.append(id).append('\t').append(id + updateCounts[dayIndex])
                                .append('\t').append(syms[dayIndex] != null ? syms[dayIndex] : "s" + (id % 3))
                                .append('\t').append(tags[dayIndex] != null ? tags[dayIndex] : "t" + (id % 3))
                                .append('\t').append(notes[dayIndex] != null ? notes[dayIndex] : "n" + (id % 3))
                                .append('\n');
                    }
                    assertQuery("SELECT id, v, sym, tag, note FROM pq WHERE " + dayFilter).noLeakCheck().returns(expected.toString());
                    // and its index finds the rows
                    final String sym = syms[dayIndex] != null ? syms[dayIndex] : "s1";
                    assertQuery("SELECT count() FROM pq WHERE sym = '" + sym + "' AND " + dayFilter).noLeakCheck().noRandomAccess().expectSize()
                            .returns("count\n" + (syms[dayIndex] != null ? 12 : 4) + "\n");
                }
                LOG.info().$("links skipped after the purge removed their source [count=").$(skippedLinks).I$();
                // every UPDATE added 1 to the 12 rows of its day, and the extra rows hold 0
                final int rowCount = 12 * (detachedDayCount + 1);
                assertQuery("SELECT count(), sum(v) FROM pq").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\tsum\n" + (rowCount + 2 * detachedDayCount) + "\t" + ((long) rowCount * (rowCount + 1) / 2 + 12L * 2 * detachedDayCount) + "\n");
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
                if (Utf8s.containsAscii(src, linkedDay)) {
                    final int linksBefore = linksBeforePurge.get();
                    if (linksBefore >= 0 && linksBeforePurge.compareAndSet(linksBefore, linksBefore - 1) && linksBefore == 0) {
                        runPurgeJob();
                        hasPurgedDuringWalk = true;
                    }
                    if (hasPurgedDuringWalk && !exists(src)) {
                        vanishedLinkSources.incrementAndGet();
                    }
                }
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

            @Override
            public void iterateDir(LPSZ path, FindVisitor func) {
                if (!Utf8s.containsAscii(path, linkedDay)) {
                    super.iterateDir(path, func);
                    return;
                }
                // the newest column versions first, so that a walk that keeps the version it lists last keeps the oldest
                final ArrayList<String> names = new ArrayList<>();
                final HashMap<String, Integer> types = new HashMap<>();
                final DirectUtf8StringZ listed = new DirectUtf8StringZ();
                super.iterateDir(path, (pUtf8NameZ, type) -> {
                    final String name = Utf8s.toString(listed.of(pUtf8NameZ));
                    names.add(name);
                    types.put(name, type);
                });
                names.sort(Comparator.comparingLong(PartitionLinkColumnPurgeRaceTest::nameTxnOf).reversed());
                try (Path name = new Path()) {
                    for (int i = 0, n = names.size(); i < n; i++) {
                        func.onFind(name.of(names.get(i)).$().ptr(), types.get(names.get(i)));
                    }
                }
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
