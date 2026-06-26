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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.questdb.cairo.ErrorTag.*;
import static io.questdb.std.Files.SEPARATOR;

public class WalTableListFunctionFactoryTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
    }

    @Test
    public void testMemoryPressureIndicator() throws Exception {
        assertMemoryLeak(() -> {
            TableSequencerAPI tableSequencerAPI = engine.getTableSequencerAPI();

            createTable("A", true);
            engine.awaitTable("A", 30, TimeUnit.SECONDS);

            TableToken token = engine.getTableTokenIfExists("A");
            Assert.assertNotNull(token);

            var pressureControl = tableSequencerAPI.getTxnTracker(token).getMemPressureControl();
            assertMemoryPressureLevel(0);

            int parallelism = pressureControl.getMemoryPressureRegulationValue();
            pressureControl.updateInflightPartitions(parallelism);
            pressureControl.onOutOfMemory();

            // Memory pressure level should be 1 after the first OOM event - it indicates that the table is under memory pressure
            // and reducing parallelism
            assertMemoryPressureLevel(1);

            do {
                parallelism = pressureControl.getMemoryPressureRegulationValue();
                pressureControl.updateInflightPartitions(parallelism);
                pressureControl.onOutOfMemory();
            } while (pressureControl.getMemoryPressureLevel() == 1);

            // eventually memory pressure level should be 2 after the second OOM event - it indicates that the table is under memory pressure
            // and is applying backoff
            assertMemoryPressureLevel(2);


            // now let's simulate reducing memory pressure
            parallelism = pressureControl.getMemoryPressureRegulationValue();
            pressureControl.updateInflightPartitions(parallelism);
            pressureControl.onEnoughMemory();

            // after a first successful O3 merge memory pressure level should be 1 - still reducing parallelism
            // but no longer applying backoff
            assertMemoryPressureLevel(1);

            do {
                parallelism = pressureControl.getMemoryPressureRegulationValue();
                pressureControl.updateInflightPartitions(parallelism);
                pressureControl.onEnoughMemory();
            } while (pressureControl.getMemoryPressureLevel() == 1);

            // eventually the memory pressure should be 0 - no memory pressure at all
            assertMemoryPressureLevel(0);
        });
    }

    @Test
    public void testNotInitialized() throws Exception {
        assertMemoryLeak(() -> {
            createTable("B", true);
            createTable("C", true);
            assertQuery("wal_tables()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\tcommitMode\tdurableEpochSeqTxn\twalRetentionTxn\trecoveryIncarnation
                            B\tfalse\t0\t0\t0\t\t\t0\tnosync\t0\t0\t0
                            C\tfalse\t0\t0\t0\t\t\t0\tnosync\t0\t0\t0
                            """);
        });
    }

    @Test
    public void testWalTablesQueryCache() throws Exception {
        assertMemoryLeak(() -> {
            createTable("A", false);
            createTable("B", true);
            createTable("C", true);

            try (RecordCursorFactory factory = select("wal_tables()")) {
                // RecordCursorFactory could be cached in QueryCache and reused
                // so let's run the query few times using the same factory
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        println(factory, cursor);
                        TestUtils.assertEquals("""
                                name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\tcommitMode\tdurableEpochSeqTxn\twalRetentionTxn\trecoveryIncarnation
                                B\tfalse\t0\t0\t0\t\t\t0\tnosync\t0\t0\t0
                                C\tfalse\t0\t0\t0\t\t\t0\tnosync\t0\t0\t0
                                """, sink);
                    }
                }
            }
        });
    }

    @Test
    public void testWalTablesSelectAll() throws Exception {
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int attempt = 0;

            @Override
            public int errno() {
                return 888;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "x.d.1") && attempt++ == 0) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(filesFacade, () -> {
            createTable("A", false);
            createTable("B", true);
            createTable("C", true);
            createTable("D", true);

            execute("insert into B values (1, 'A', '2022-12-05T01', 'B')");
            execute("update B set x = 101");
            execute("insert into B values (2, 'C', '2022-12-05T02', 'D')");
            execute("insert into C values (1, 'A', '2022-12-05T01', 'B')");
            execute("insert into C values (2, 'C', '2022-12-05T02', 'D')");
            execute("insert into D values (1, 'A', '2022-12-05T01', 'B')");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("B")));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("C")));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("D")));

            assertQuery("wal_tables() order by name")
                    .noLeakCheck()
                    .returns("name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\tcommitMode\tdurableEpochSeqTxn\twalRetentionTxn\trecoveryIncarnation\n" +
                            "B\ttrue\t1\t0\t3\t\tcould not open read-write [file=" + root + SEPARATOR + "B~2" + SEPARATOR + "2022-12-05" + SEPARATOR + "x.d.1]\t0\tnosync\t0\t0\t0\n" +
                            "C\tfalse\t2\t0\t2\t\t\t0\tnosync\t0\t0\t0\n" +
                            "D\tfalse\t1\t0\t1\t\t\t0\tnosync\t0\t0\t0\n");

            assertQuery("select name, suspended, writerTxn from wal_tables() order by name")
                    .noLeakCheck()
                    .returns("""
                            name\tsuspended\twriterTxn
                            B\ttrue\t1
                            C\tfalse\t2
                            D\tfalse\t1
                            """);

            assertQuery("select name, suspended, writerTxn from wal_tables() where name = 'B'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tsuspended\twriterTxn
                            B\ttrue\t1
                            """);
        });
    }

    @Test
    public void testWalTablesSuspendedWithErrorCode() throws Exception {
        testWalTablesSuspendedWithError("alter table B suspend wal with " + (Os.isWindows() ? 112 : 28) + ", 'Out of disk space'", DISK_FULL, "Out of disk space");
        testWalTablesSuspendedWithError("alter table B suspend wal with " + (Os.isWindows() ? 8 : 12) + ", 'Out of memory'", OUT_OF_MMAP_AREAS, "Out of memory");
        testWalTablesSuspendedWithError("alter table B suspend wal with " + (Os.isWindows() ? 4 : 24) + ", 'Too many open file handlers'", TOO_MANY_OPEN_FILES, "Too many open file handlers");
    }

    @Test
    public void testWalTablesSuspendedWithErrorTag() throws Exception {
        testWalTablesSuspendedWithError("alter table B suspend wal with 'DISK FULL', 'test error message 1'", DISK_FULL, "test error message 1");
        testWalTablesSuspendedWithError("alter table B suspend wal with 'OUT OF MMAP AREAS', 'test error message 2'", OUT_OF_MMAP_AREAS, "test error message 2");
        testWalTablesSuspendedWithError("alter table B suspend wal with 'OUT OF MEMORY', 'test error message 3'", OUT_OF_MEMORY, "test error message 3");
        testWalTablesSuspendedWithError("alter table B suspend wal with 'TOO MANY OPEN FILES', 'test error message 4'", TOO_MANY_OPEN_FILES, "test error message 4");
        testWalTablesSuspendedWithError("alter table B suspend wal with '', 'test error message 5'", NONE, "test error message 5");
        testWalTablesSuspendedWithError("alter table B suspend wal", NONE, "");
    }

    /**
     * Plan 4 — adaptive observability columns: TDD (RED -> GREEN).
     * <p>
     * Asserts that wal_tables() exposes the four new columns with correct values:
     * <ul>
     *   <li>{@code commitMode} — reflects the engine's configured commit mode (default = "nosync").</li>
     *   <li>{@code durableEpochSeqTxn} — read from the per-table {@link SeqTxnTracker}.</li>
     *   <li>{@code walRetentionTxn} — same as durableEpochSeqTxn (the adaptive WAL floor).</li>
     *   <li>{@code recoveryIncarnation} — incremented by {@link io.questdb.cairo.RecoveryCoordinator}
     *       on a successful epoch restore; bumping via {@link SeqTxnTracker#bumpRecoveryIncarnation()}
     *       directly verifies the counter is surfaced correctly by wal_tables().</li>
     * </ul>
     */
    @Test
    public void testAdaptiveObservabilityColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Default commit mode for AbstractCairoTest is NOSYNC.
            createTable("T1", true);
            drainWalQueue();

            final TableToken token = engine.verifyTableName("T1");

            // --- commitMode = nosync (default) ---
            assertQuery("select name, commitMode from wal_tables() where name = 'T1'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tcommitMode
                            T1\tnosync
                            """);

            // --- durableEpochSeqTxn / walRetentionTxn: 0 by default ---
            assertQuery("select name, durableEpochSeqTxn, walRetentionTxn from wal_tables() where name = 'T1'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tdurableEpochSeqTxn\twalRetentionTxn
                            T1\t0\t0
                            """);

            // --- recoveryIncarnation: 0 initially ---
            assertQuery("select name, recoveryIncarnation from wal_tables() where name = 'T1'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\trecoveryIncarnation
                            T1\t0
                            """);

            // Advance durableEpochSeqTxn on the tracker directly (simulates an epoch commit) and
            // bump recoveryIncarnation (simulates a recovery restore for this table).
            SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            tracker.setDurableEpochSeqTxn(5L);
            tracker.bumpRecoveryIncarnation();

            // --- durableEpochSeqTxn and walRetentionTxn both reflect the updated epoch ---
            assertQuery("select name, durableEpochSeqTxn, walRetentionTxn from wal_tables() where name = 'T1'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tdurableEpochSeqTxn\twalRetentionTxn
                            T1\t5\t5
                            """);

            // --- recoveryIncarnation = 1 after one bump ---
            assertQuery("select name, recoveryIncarnation from wal_tables() where name = 'T1'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\trecoveryIncarnation
                            T1\t1
                            """);

            // Bump again to confirm it increments correctly.
            tracker.bumpRecoveryIncarnation();
            assertQuery("select name, recoveryIncarnation from wal_tables() where name = 'T1'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\trecoveryIncarnation
                            T1\t2
                            """);
        });
    }

    private void assertMemoryPressureLevel(int expectedMemoryPressureLevel) throws Exception {
        assertQuery("select memoryPressure from wal_tables() where name = '" + "A" + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("memoryPressure\n" +
                        expectedMemoryPressureLevel + "\n");
    }

    private void createTable(final String tableName, boolean isWal) throws SqlException {
        execute("create table " + tableName + " (" +
                "x long," +
                "sym symbol," +
                "ts timestamp," +
                "sym2 symbol" +
                ") timestamp(ts) partition by DAY" + (isWal ? " WAL" : ""));
    }

    private void dropTable(final String tableName) throws SqlException {
        execute("drop table " + tableName);
    }

    private void testWalTablesSuspendedWithError(String suspendSql, ErrorTag expectedErrorTag, String expectedErrorMessage) throws Exception {
        assertMemoryLeak(() -> {
            createTable("A", false);
            createTable("B", true);

            execute("insert into A values (1, 'A', '2022-12-05T01', 'A')");
            execute("insert into B values (2, 'A', '2022-12-05T01', 'B')");

            drainWalQueue();
            execute(suspendSql);

            execute("insert into B values (3, 'C', '2022-12-05T02', 'D')");

            drainWalQueue();

            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("B")));

            assertQuery("wal_tables()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\tcommitMode\tdurableEpochSeqTxn\twalRetentionTxn\trecoveryIncarnation\n" +
                            "B\ttrue\t1\t0\t2\t" + expectedErrorTag.text() + "\t" + expectedErrorMessage + "\t0\tnosync\t0\t0\t0\n");

            execute("alter table B resume wal");

            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("B")));

            assertQuery("wal_tables()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\tcommitMode\tdurableEpochSeqTxn\twalRetentionTxn\trecoveryIncarnation
                            B\tfalse\t2\t0\t2\t\t\t0\tnosync\t0\t0\t0
                            """);

            dropTable("A");
            dropTable("B");
        });
    }
}
