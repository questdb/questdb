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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Crash consistency for the V1 sequencer's additive {@code _txnlog.c} CRC sidecar.
 * <p>
 * The sidecar and the txnlog header are SEPARATE files, so a crash can make one durable and not the
 * other. The dangerous direction is a header advertising a txn whose CRC never landed: the record is
 * intact, but a reader that treats "no CRC at or beyond the watermark" as torn condemns it. That is a
 * false alarm on healthy data, which is worse than not checking at all.
 * <p>
 * V1 is no longer the default, so the format is pinned in {@link #setUp()} -- under V2 there is no
 * sidecar at all and every assertion here would pass for the wrong reason.
 */
public class TxnLogCrcSidecarCrashTest extends AbstractCrashConsistencyTest {

    @Override
    public void setUp() {
        // Must be set BEFORE the engine is built: setting it inside a test method is too late, and the
        // tables come out V2. The assertIsV1 precondition below exists because that actually happened.
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
        super.setUp();
    }

    @Test
    public void testV1SidecarCrashSweepNeverCondemnsAnIntactRecord() throws Exception {
        // V1 only: the sidecar exists solely for the format whose records have no room for a CRC.
        final int ops = countOps("v1probe");
        Assert.assertTrue("expected a real durability-op sequence to sweep, got " + ops, ops >= 8);

        for (int crashAt = 1; crashAt <= ops; crashAt++) {
            final int point = crashAt;
            runWithCrashFacade(() -> {
                final String t = "v1crc" + point;
                execute("create table " + t + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("insert into " + t + " values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                assertIsV1(t);
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
    public void testTornSidecarTailDegradesToLegacy() throws Exception {
        // The sidecar body is append-only, so a torn TAIL must not invalidate the prefix, and must not
        // condemn the records it covers.
        runWithCrashFacade(() -> {
            execute("create table v1torn (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 5; i++) {
                execute("insert into v1torn values ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
            }
            drainWalQueue();
            assertIsV1("v1torn");

            final TableToken token = engine.verifyTableName("v1torn");
            markDurableBaseline();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot())
                        .concat(token)
                        .concat(WalUtils.SEQ_DIR)
                        .concat(WalUtils.TXNLOG_CRC_FILE_NAME);
                // Zero from the third entry onward: header and the first entries survive.
                crashFf.tornTail(path.$(), 24 + 2 * 8, 64);
            }
            crashAndReopen();

            assertTableReadableOrGone("v1torn", -1);
        });
    }

    private void assertIsV1(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(token)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_FILE_NAME);
            final long fd = crashFf.openRO(path.$());
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertEquals("this sweep requires the V1 txnlog",
                        WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1, crashFf.readNonNegativeInt(fd, 0));
            } finally {
                crashFf.close(fd);
            }
        }
    }

    /**
     * Bar: after a crash the table either reads, or is gone. What it must never do is survive and then
     * be rejected by a CRC verdict, which would mean intact bytes condemned.
     */
    private void assertTableReadableOrGone(String tableName, int crashPoint) {
        try {
            engine.verifyTableName(tableName);
        } catch (CairoException e) {
            return;
        }
        try {
            engine.getTableMetadata(engine.verifyTableName(tableName)).close();
            try (io.questdb.cairo.wal.seq.TransactionLogCursor cursor =
                         engine.getTableSequencerAPI().getCursor(engine.verifyTableName(tableName), 1)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
            }
        } catch (CairoException | CairoError e) {
            final String msg = String.valueOf(e.getMessage());
            if (msg.contains("torn sequencer txnlog record")
                    || msg.contains("absent/torn sequencer txnlog record")) {
                Assert.fail("crash at op " + crashPoint + " condemned an intact record: " + msg);
            }
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
