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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A failed RESUME WAL leaves a suspended table's suspension as it was (reason, write denial, gauge) and reports
 * the failure itself; only a failed save point is reported as such.
 */
public class WalResumeFailureTest extends AbstractCairoTest {
    // EMFILE on Linux: the lock fault reports it, so the errno the caller sees is known.
    private static final int LOCK_FAULT_ERRNO = 24;
    private static final String SUSPENDED_TABLES = "questdb_suspended_tables";

    @Test
    public void testResumeFromTxnLockFailureKeepsSuspensionAdaptive() throws Exception {
        assertLockFailureKeepsSuspension("adaptive", CommitMode.ADAPTIVE);
    }

    @Test
    public void testResumeFromTxnLockFailureKeepsSuspensionNosync() throws Exception {
        assertLockFailureKeepsSuspension("nosync", CommitMode.NOSYNC);
    }

    @Test
    public void testResumeFromTxnSavePointFailureKeepsSuspension() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final AtomicBoolean isFailing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getResolvedCommitMode());
            final TableToken token = createSuspendedTableWithSkippableTxn();
            try (TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(1);
            }
            isFailing.set(true);
            try {
                execute("alter table t resume wal from txn 3");
                Assert.fail("the save point cannot be taken, so the resume must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "could not take the durable epoch the table owes, the table stays suspended; retry RESUME WAL [table=t, reason=");
            } finally {
                isFailing.set(false);
            }
            assertSuspensionKept(token);

            execute("alter table t resume wal from txn 3");
            drainWalQueue();
            assertResumed(token);
        });
    }

    @Test
    public void testResumeFromTxnTxnFileFailureKeepsSuspension() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final TableFileFault fault = new TableFileFault("/_txn", CairoException.ERRNO_FILE_DOES_NOT_EXIST);
        assertMemoryLeak(fault, () -> {
            Assert.assertEquals(CommitMode.NOSYNC, engine.getResolvedCommitMode());
            final TableToken token = createSuspendedTableWithSkippableTxn();
            engine.releaseAllWriters();
            fault.arm(token);
            try {
                execute("alter table t resume wal from txn 3");
                Assert.fail("the writer cannot open, so the resume must fail");
            } catch (CairoException e) {
                Assert.assertEquals("errno", CairoException.TABLE_DOES_NOT_EXIST, e.getErrno());
                Assert.assertFalse("critical", e.isCritical());
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=t]");
            } finally {
                fault.disarm();
            }
            Assert.assertNotNull("the fault must fire", fault.failedFile.get());
            assertSuspensionKept(token);

            execute("alter table t resume wal from txn 3");
            drainWalQueue();
            assertResumed(token);
        });
    }

    @Test
    public void testResumeOfRunningTableSuspendsOnce() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final TableFileFault fault = new TableFileFault(".lock", LOCK_FAULT_ERRNO);
        assertMemoryLeak(fault, () -> {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getResolvedCommitMode());
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t values (1, '2024-01-01T00')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            // Boot recovery arms this for a table whose last direct change still needs its save point; the
            // table runs meanwhile, and a plain RESUME WAL takes the writer to complete it.
            engine.getTableSequencerAPI().getTxnTracker(token).setDurableEpochCompletionRequired(true);
            engine.releaseAllWriters();
            assertSuspendedTables(0);
            fault.arm(token);
            try {
                execute("alter table t resume wal");
                Assert.fail("the writer cannot open, so the resume must fail");
            } catch (CairoException e) {
                Assert.assertEquals("errno", LOCK_FAULT_ERRNO, e.getErrno());
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot lock table");
            } finally {
                fault.disarm();
            }
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("select suspended, errorTag, errorMessage like 'cannot lock table: %' isLockFailure from wal_tables() where name = 't'")
                    .noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary()
                    .returns("suspended\terrorTag\tisLockFailure\ntrue\t" + ErrorTag.resolveTag(LOCK_FAULT_ERRNO).text() + "\ttrue\n");
            assertSuspendedTables(1);

            execute("alter table t resume wal");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertSuspendedTables(0);
        });
    }

    private static void assertSuspendedTables(long expected) {
        Assert.assertEquals(SUSPENDED_TABLES, expected, TestUtils.getMetricValue(engine, SUSPENDED_TABLES));
    }

    // One transaction applied, then a SUSPEND WAL with a reason, then transactions 2 and 3 left to apply.
    private static TableToken createSuspendedTableWithSkippableTxn() throws Exception {
        execute("create table t (x long, ts timestamp) timestamp(ts) partition by day wal");
        execute("insert into t values (1, '2024-01-01T00')");
        drainWalQueue();
        final TableToken token = engine.verifyTableName("t");
        execute("alter table t suspend wal with 'DISK FULL', 'original operator reason'");
        execute("insert into t values (2, '2024-01-01T01')");
        execute("insert into t values (3, '2024-01-01T02')");
        drainWalQueue();
        assertSuspendedTables(1);
        return token;
    }

    private void assertLockFailureKeepsSuspension(String mode, int commitMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
        final TableFileFault fault = new TableFileFault(".lock", LOCK_FAULT_ERRNO);
        assertMemoryLeak(fault, () -> {
            Assert.assertEquals(commitMode, engine.getResolvedCommitMode());
            final TableToken token = createSuspendedTableWithSkippableTxn();
            engine.releaseAllWriters();
            fault.arm(token);
            try {
                execute("alter table t resume wal from txn 3");
                Assert.fail("the writer cannot open, so the resume must fail");
            } catch (CairoException e) {
                Assert.assertEquals("errno", LOCK_FAULT_ERRNO, e.getErrno());
                Assert.assertTrue("critical", e.isCritical());
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot lock table");
                Assert.assertFalse(Chars.contains(e.getFlyweightMessage(), "durable epoch"));
            } finally {
                fault.disarm();
            }
            Assert.assertNotNull("the fault must fire", fault.failedFile.get());
            assertSuspensionKept(token);

            execute("alter table t resume wal from txn 3");
            drainWalQueue();
            assertResumed(token);
        });
    }

    private void assertResumed(TableToken token) throws Exception {
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        assertSuspendedTables(0);
        assertQuery("select x from t")
                .noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary()
                .returns("x\n1\n3\n");
    }

    private void assertSuspensionKept(TableToken token) throws Exception {
        Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
        assertQuery("select suspended, writerTxn, sequencerTxn, errorTag, errorMessage from wal_tables() where name = 't'")
                .noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary()
                .returns("suspended\twriterTxn\tsequencerTxn\terrorTag\terrorMessage\ntrue\t1\t3\tDISK FULL\toriginal operator reason\n");
        assertSuspendedTables(1);
        Assert.assertTrue("SUSPEND WAL must stay in force", engine.isWalApplySuspended(token));
    }

    /**
     * Fails the first openRW of one file in the table's own directory (not in its WAL or sequencer), and reports
     * the given errno for it.
     */
    private static class TableFileFault extends TestFilesFacadeImpl {
        private final AtomicReference<String> dirName = new AtomicReference<>();
        private final int errno;
        private final AtomicReference<String> failedFile = new AtomicReference<>();
        private final String suffix;
        private volatile boolean isArmed;

        TableFileFault(String suffix, int errno) {
            this.suffix = suffix;
            this.errno = errno;
        }

        @Override
        public int errno() {
            return isArmed && failedFile.get() != null ? errno : super.errno();
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final String dir = dirName.get();
            if (isArmed && dir != null && failedFile.get() == null
                    && Utf8s.containsAscii(name, dir) && Utf8s.endsWithAscii(name, suffix)
                    && !Utf8s.containsAscii(name, "/wal") && !Utf8s.containsAscii(name, "/txn_seq")) {
                failedFile.set(Utf8s.toString(name));
                return -1;
            }
            return super.openRW(name, opts);
        }

        void arm(TableToken token) {
            dirName.set(token.getDirName());
            failedFile.set(null);
            isArmed = true;
        }

        void disarm() {
            isArmed = false;
        }
    }
}
