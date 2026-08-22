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

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Crash consistency for the two capability-marked checksum artifacts this branch added:
 * the {@code _txn} body-checksum capability marker, and the V1 sequencer's additive
 * {@code _txnlog.c} CRC sidecar.
 * <p>
 * Both share one hazard. Each publishes a claim ("from this point a checksum exists") separately
 * from the checksummed bytes themselves, so a crash can land between the two writes. Get it wrong in
 * the unsafe direction and the reader condemns intact data:
 * <ul>
 *   <li>{@code _txn}: a capability marker durable AHEAD of the checksum it promises makes the
 *       already-written record read as torn.</li>
 *   <li>{@code _txnlog.c}: a header advertising a txn whose CRC never landed makes an intact record
 *       read as "absent/torn beyond the durable frontier".</li>
 * </ul>
 * Each is swept at EVERY durability op rather than at one chosen point, and each iteration asserts
 * the injected crash actually fired -- a sweep whose injection never fires proves nothing.
 * <p>
 * The bar is deliberately asymmetric, matching the crash model: FEWER rows or a missing table after a
 * crash is acceptable (rollback); a healthy artifact reported as corrupt is not.
 */
public class ChecksumArtifactsCrashTest extends AbstractCrashConsistencyTest {

    @Test
    public void testTxnCapabilityCrashSweepNeverCondemnsAnIntactTxn() throws Exception {
        final int ops = countOps("txnprobe");
        Assert.assertTrue("expected a real durability-op sequence to sweep, got " + ops, ops >= 8);

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
    public void testTornTxnCapabilityMarkerDegradesToLegacy() throws Exception {
        // Tear the capability marker itself. Losing it must cost DETECTION (records fall back to
        // "legacy, unverified"), never availability -- the alternative, condemning every record whose
        // checksum the file no longer promises, would take out a healthy table.
        runWithCrashFacade(() -> {
            execute("create table txntorn (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txntorn values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("txntorn");
            markDurableBaseline();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
                crashFf.tornTail(path.$(), TableUtils.TX_BASE_OFFSET_CAPABILITY_MAGIC_64, 16);
            }
            crashAndReopen();

            assertTableReadableOrGone("txntorn", -1);
        });
    }

    /**
     * Bar: after a crash the table either reads, or is gone. What it must never do is survive and then
     * be rejected by one of the new checksum verdicts, which would mean intact bytes condemned.
     */
    private void assertTableReadableOrGone(String tableName, int crashPoint) {
        try {
            engine.verifyTableName(tableName);
        } catch (CairoException e) {
            return; // table did not survive: acceptable crash outcome
        }
        try {
            engine.getTableMetadata(engine.verifyTableName(tableName)).close();
            // Reading the sequencer is what exercises the txnlog CRC path; metadata alone would not.
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
                    || msg.contains("absent/torn sequencer txnlog record")
                    || msg.contains("body length is impossible")) {
                Assert.fail("crash at op " + crashPoint + " condemned an intact artifact: " + msg);
            }
            // other failures are pre-existing recovery outcomes, not this feature's concern
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
