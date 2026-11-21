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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives the purge job's external consumer floor with random floors, segment rolls, commits on several writers,
 * applies and sweeps, and checks after every sweep that every segment and every sequencer part holding a
 * transaction above the highest hold any sweep has seen still exists.
 */
public class WalPurgeJobFloorFuzzTest extends AbstractCairoTest {

    @Test
    public void testRandomFloorsRollsAndSweepsHoldWhatTheConsumerNeeds() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync"); // mode-independent: adaptive keeps the WAL until a second save point
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // 0 is the V1 log, the default, which has no sequencer parts
            final int partSize = rnd.nextInt(9);
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partSize);
            final String tableName = testName.getMethodName();
            execute("CREATE TABLE " + tableName + " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tableToken = engine.verifyTableName(tableName);
            final long interval = engine.getConfiguration().getWalPurgeInterval() * 1000; // ms to us
            setCurrentMicros(interval + 1);

            final AtomicLong floor = new AtomicLong(Long.MAX_VALUE);
            // one entry per committed transaction: its sequencer txn, wal id and segment id
            final LongList txns = new LongList();
            final IntList walIds = new IntList();
            final IntList segmentIds = new IntList();
            final int writerCount = 1 + rnd.nextInt(3);
            final ObjList<WalWriter> writers = new ObjList<>();
            try (WalPurgeJob job = new WalPurgeJob(engine) {
                @Override
                protected long getExternalConsumerFloor(TableToken token) {
                    return floor.get();
                }
            }) {
                for (int i = 0; i < writerCount; i++) {
                    writers.add(engine.getWalWriter(tableToken));
                }
                long ts = MicrosTimestampDriver.floor("2026-01-01");
                long applied = 0;
                // the highest hold any sweep has seen: what a sweep removed at a higher hold is gone, so a floor
                // that later drops holds nothing more, and the invariant follows this maximum
                long heldMax = 0;
                final int steps = 200 + rnd.nextInt(300);
                for (int step = 0; step < steps; step++) {
                    final WalWriter writer = writers.getQuick(rnd.nextInt(writerCount));
                    switch (rnd.nextInt(5)) {
                        case 0, 1 -> {
                            final TableWriter.Row row = writer.newRow(ts);
                            row.putLong(0, step);
                            row.append();
                            writer.commit();
                            ts += 1_000_000L;
                            txns.add(engine.getTableSequencerAPI().lastTxn(tableToken));
                            walIds.add(writer.getWalId());
                            segmentIds.add(writer.getSegmentId());
                        }
                        case 2 -> writer.rollSegment();
                        case 3 -> {
                            drainWalQueue();
                            applied = engine.getTableSequencerAPI().lastTxn(tableToken);
                        }
                        default -> {
                            final long last = engine.getTableSequencerAPI().lastTxn(tableToken);
                            // a floor in [0, last + 1], or no hold at all; it may drop below an earlier floor
                            floor.set(rnd.nextInt(4) == 0 ? Long.MAX_VALUE : rnd.nextInt((int) last + 2));
                            heldMax = Math.max(heldMax, Math.min(applied, floor.get()));
                            sweep(job, interval);
                            assertHeld(tableToken, txns, walIds, segmentIds, heldMax, partSize);
                        }
                    }
                }
                // the hold released, everything applied and every writer back in the pool and closed: the next
                // sweep removes every wal of the table
                floor.set(Long.MAX_VALUE);
                drainWalQueue();
                Misc.freeObjListAndClear(writers);
                engine.releaseAllWalWriters();
                sweep(job, interval);
                for (int i = 0; i < writerCount; i++) {
                    assertWalExistence(false, tableToken, i + 1);
                }
            } finally {
                Misc.freeObjListAndClear(writers);
            }
        });
    }

    private static void sweep(WalPurgeJob job, long interval) {
        setCurrentMicros(currentMicros + 2 * interval);
        job.drain(0);
    }

    private void assertHeld(
            TableToken tableToken,
            LongList txns,
            IntList walIds,
            IntList segmentIds,
            long held,
            int partSize
    ) {
        for (int i = 0, n = txns.size(); i < n; i++) {
            final long txn = txns.getQuick(i);
            if (txn > held) {
                assertSegmentExistence(true, tableToken, walIds.getQuick(i), segmentIds.getQuick(i));
                if (partSize > 0) {
                    assertSeqPartExists(tableToken, (txn - 1) / partSize);
                }
            }
        }
    }

    private void assertSeqPartExists(TableToken tableToken, long partNo) {
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot())
                    .concat(tableToken)
                    .concat(WalUtils.SEQ_DIR)
                    .concat(WalUtils.TXNLOG_PARTS_DIR)
                    .concat(String.valueOf(partNo))
                    .$();
            Assert.assertTrue(Utf8s.toString(path), TestFilesFacadeImpl.INSTANCE.exists(path.$()));
        }
    }
}
