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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Crash consistency for the {@code _txn} body checksum and the stamp that names its record.
 * <p>
 * The stamp claims that a checksum exists, separately from the checksummed bytes themselves, so a crash can
 * land between the two writes. A stamp durable ahead of the checksum it promises would make the
 * already-written record read as torn, and the reader would condemn intact data.
 * <p>
 * The test sweeps every durability op rather than one chosen point, and each iteration asserts that the
 * injected crash fired. The bar is asymmetric, matching the crash model: fewer rows or a missing table after
 * a crash is acceptable (rollback); a healthy artifact reported as corrupt is not.
 */
public class ChecksumArtifactsCrashTest extends AbstractCrashConsistencyTest {

    @Test
    public void testTxnCapabilityCrashSweepNeverCondemnsAnIntactTxn() throws Exception {
        // The artifacts under test are written in every mode, but NOSYNC issues no durability op to crash at.
        // Under a nosync sweep, sweep SYNC, the closest grade that syncs; every other sweep keeps its own mode.
        if (engine.getConfiguration().getCommitMode() == CommitMode.NOSYNC) {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        }
        final int ops = countOps("txnprobe");
        // An async sync makes no msync call, so the sweep has no MS_ASYNC points. Those points made nothing durable
        // in the crash model: a crash right after one left the durable state of a crash right after the op before
        // it. An ADAPTIVE commit of this table is five ops.
        Assert.assertTrue("expected a real durability-op sequence to sweep, got " + ops, ops >= 5);

        for (int crashAt = 1; crashAt <= ops; crashAt++) {
            final int point = crashAt;
            runWithCrashFacade(() -> {
                final String t = "txncap" + point;
                execute("create table " + t + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into " + t + " values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                markDurableBaseline();

                crashFf.armCrashAt(point);
                try {
                    execute("insert into " + t + " values ('2024-01-02T00:00:00.000000Z', 2)");
                    drainWalQueue();
                } catch (CrashSimulationError | CairoException e) {
                    // expected at most crash points
                }
                Assert.assertFalse("crash armed at op " + point + " never fired", crashFf.isCrashArmed());
                crashAndReopen();

                assertTableReadableOrGone(t, point);
            });
        }
    }

    @Test
    public void testTornChecksumStampDegradesToLegacy() throws Exception {
        // Tear the checksum and the stamp that names its record. Losing them must cost detection (the record
        // falls back to "unstamped, unverified"), never availability: condemning a record whose checksum
        // metadata is gone would take out a healthy table.
        runWithCrashFacade(() -> {
            execute("create table txntorn (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txntorn values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("txntorn");
            markDurableBaseline();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                // [116,128) of area A: the 8 checksum bytes plus the 4-byte stamp.
                crashFf.tornTail(path.$(), TableUtils.TX_BASE_HEADER_SIZE + TableUtils.TX_OFFSET_BODY_CHECKSUM_64, 12);
            }
            crashAndReopen();

            assertTableReadableOrGone("txntorn", -1);
        });
    }

    /**
     * Asserts that after a crash the table either reads or is gone. It must never survive and then be
     * rejected by a checksum verdict, which would mean intact bytes condemned.
     */
    private void assertTableReadableOrGone(String tableName, int crashPoint) {
        try {
            engine.verifyTableName(tableName);
        } catch (CairoException e) {
            return; // table did not survive: acceptable crash outcome
        }
        try {
            engine.getTableMetadata(engine.verifyTableName(tableName)).close();
            // Reading the sequencer is what exercises the txnlog checks; metadata alone would not.
            try (io.questdb.cairo.wal.seq.TransactionLogCursor cursor =
                         engine.getTableSequencerAPI().getCursor(engine.verifyTableName(tableName), 1)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
            }
        } catch (CairoException | CairoError e) {
            final String msg = String.valueOf(e.getMessage());
            if (msg.contains("checksum mismatch")
                    || msg.contains("torn sequencer txnlog record")
                    || msg.contains("body length is impossible")) {
                Assert.fail("crash at op " + crashPoint + " condemned an intact artifact: " + msg);
            }
            // other failures are ordinary crash-recovery outcomes, outside the scope of this check
        }
    }

    private int countOps(String tableName) throws Exception {
        final int[] ops = new int[1];
        runWithCrashFacade(() -> {
            execute("create table " + tableName + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            markDurableBaseline();
            final int before = crashFf.durabilityOpCount();
            execute("insert into " + tableName + " values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            ops[0] = crashFf.durabilityOpCount() - before;
        });
        return ops[0];
    }
}
