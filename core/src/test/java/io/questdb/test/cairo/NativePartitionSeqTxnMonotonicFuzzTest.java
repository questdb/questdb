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

package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TxReader;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fuzz characterization of the per-partition native seqTxn contract.
 * The guarantee is <b>monotonic-safety</b>, not cross-instance determinism:
 * <ul>
 *   <li><b>lower bound</b> -- {@code stamp(P) >= canonical(P)}, where {@code canonical(P)} is the highest
 *       seqTxn of any transaction that wrote a row into partition P;</li>
 *   <li><b>strict increase</b> -- any write to P raises P's stamp.</li>
 * </ul>
 * The same WAL applied with a different transaction grouping (one block vs 1-by-1) may stamp the same
 * partition with <i>different</i> values -- but both stay {@code >= canonical}. This test runs the same
 * generated stream through both groupings and asserts the monotonic-safe envelope on each, so a regression
 * that lets any stamp slip <i>below</i> canonical (the only direction that would corrupt the dedup gate by
 * skipping a needed upload) fails loudly. The serial path is sampled after every applied commit so a
 * transient decrease cannot hide behind a later recovery. The test also proves the two groupings hold
 * identical data, so any stamp divergence is purely a grouping artifact.
 */
public class NativePartitionSeqTxnMonotonicFuzzTest extends AbstractCairoTest {

    private static final long BASE_TS = 1_577_836_800_000_000L; // 2020-01-01
    private static final long DAY = 86_400_000_000L;

    @Test
    public void testNativeSeqTxnStaysMonotonicSafeAcrossBlockBatching() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int txnCount = 30 + rnd.nextInt(60);
            final int daySpan = 4 + rnd.nextInt(8);

            execute("CREATE TABLE blk (ts TIMESTAMP, seq LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE ser (ts TIMESTAMP, seq LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Build the transaction stream once; both tables receive byte-for-byte identical inserts.
            // canonical[dayFloor] = highest transaction ordinal (== that commit's WAL seqTxn on a fresh
            // table) that wrote a row into that day.
            final Map<Long, Long> canonical = new HashMap<>();
            final String[] inserts = new String[txnCount];
            final List<Set<Long>> touchedPartitions = new ArrayList<>(txnCount);
            for (int t = 0; t < txnCount; t++) {
                final long ord = t + 1;
                final int rows = 1 + rnd.nextInt(6);
                final StringBuilder sb = new StringBuilder("INSERT INTO %s VALUES ");
                final Set<Long> touched = new HashSet<>();
                for (int r = 0; r < rows; r++) {
                    // O3 bias: each row lands in a random day, so later transactions routinely backfill
                    // older partitions -- the shape that lets a block straddle and over-stamp.
                    final long day = rnd.nextInt(daySpan);
                    final long withinDay = (rnd.nextLong() >>> 1) % DAY;
                    final long ts = BASE_TS + day * DAY + withinDay;
                    if (r > 0) {
                        sb.append(", ");
                    }
                    sb.append('(').append(ts).append("::timestamp, ").append(ord).append(')');
                    final long floor = ts - ts % DAY;
                    canonical.merge(floor, ord, Math::max);
                    touched.add(floor);
                }
                inserts[t] = sb.toString();
                touchedPartitions.add(touched);
            }

            // blk: every commit is visible to one drain, so ApplyWal2TableJob forms multi-transaction blocks.
            for (int t = 0; t < txnCount; t++) {
                execute(String.format(inserts[t], "blk"));
            }
            drainWalQueue();

            // ser: drain after every commit, so exactly one seqTxn is ever visible -> block size 1.
            final Map<Long, Long> canonicalSoFar = new HashMap<>();
            final Map<Long, Long> previousStamps = new HashMap<>();
            for (int t = 0; t < txnCount; t++) {
                execute(String.format(inserts[t], "ser"));
                drainWalQueue();
                final long ord = t + 1;
                final Set<Long> touched = touchedPartitions.get(t);
                for (long floor : touched) {
                    canonicalSoFar.put(floor, ord);
                }
                assertMonotonicStep("ser", ord, canonicalSoFar, previousStamps, touched);
            }

            final int blkOverApprox = assertMonotonicSafe("blk", canonical);
            final int serOverApprox = assertMonotonicSafe("ser", canonical);

            // Same data, possibly different stamps -- the accepted non-determinism. Prove the data really is
            // identical, so any stamp difference is a grouping artifact and nothing else.
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT ts, seq FROM ser ORDER BY ts, seq",
                    "SELECT ts, seq FROM blk ORDER BY ts, seq",
                    LOG
            );

            LOG.info().$("monotonic-safe fuzz done [txns=").$(txnCount)
                    .$(", daySpan=").$(daySpan)
                    .$(", blkOverApprox=").$(blkOverApprox)
                    .$(", serOverApprox=").$(serOverApprox).I$();
        });
    }

    // Asserts every non-active native partition's offset-3 stamp lies in [canonical(P), highWater]: the
    // monotonic-safe envelope the cold dedup gate relies on. The active (last) partition is excluded, since
    // the gate never reads it (StoragePolicyCheckJob iterates partitionCount - 1). Returns how many
    // partitions are stamped strictly above canonical (the over-approximation count, for visibility).
    private int assertMonotonicSafe(String table, Map<Long, Long> canonical) {
        int overApprox = 0;
        try (TableReader reader = getReader(table)) {
            final TxReader tx = reader.getTxFile();
            Assert.assertTrue(table + " must span multiple partitions to exercise the contract",
                    tx.getPartitionCount() >= 2);
            final long highWater = tx.getSeqTxn();
            for (int i = 0, n = tx.getPartitionCount() - 1; i < n; i++) {
                final long floor = tx.getPartitionFloor(tx.getPartitionTimestampByIndex(i));
                final long stamp = tx.getNativePartitionSeqTxn(i);
                final long canon = canonical.getOrDefault(floor, 0L);
                Assert.assertTrue(table + " floor=" + floor + " stamp=" + stamp + " < canonical=" + canon
                                + " -- offset-3 must never under-approximate (would corrupt the cold dedup gate)",
                        stamp >= canon);
                Assert.assertTrue(table + " floor=" + floor + " stamp=" + stamp + " > highWater=" + highWater,
                        stamp <= highWater);
                Assert.assertTrue(table + " floor=" + floor + " was not stamped", stamp >= 1);
                if (stamp > canon) {
                    overApprox++;
                }
            }
        }
        return overApprox;
    }

    // Samples all native partitions, including the active partition, immediately after one serial WAL
    // commit. A touched existing partition must advance strictly; every other existing partition must not
    // decrease. The canonical lower bound is checked at the same point so a transient under-stamp cannot
    // be repaired by a later transaction before the fuzz test observes it.
    private void assertMonotonicStep(
            String table,
            long expectedSeqTxn,
            Map<Long, Long> canonical,
            Map<Long, Long> previousStamps,
            Set<Long> touched
    ) {
        final Map<Long, Long> currentStamps = new HashMap<>();
        final Set<Long> seenTouched = new HashSet<>();
        try (TableReader reader = getReader(table)) {
            final TxReader tx = reader.getTxFile();
            final long highWater = tx.getSeqTxn();
            Assert.assertEquals(table + " must apply exactly one WAL transaction per drain", expectedSeqTxn, highWater);
            for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                final long floor = tx.getPartitionFloor(tx.getPartitionTimestampByIndex(i));
                final long stamp = tx.getNativePartitionSeqTxn(i);
                final long canon = canonical.getOrDefault(floor, 0L);
                final Long previous = previousStamps.get(floor);

                Assert.assertTrue(
                        table + " seqTxn=" + highWater + " floor=" + floor + " stamp=" + stamp
                                + " < canonical=" + canon + " at intermediate sample",
                        stamp >= canon
                );
                Assert.assertTrue(
                        table + " seqTxn=" + highWater + " floor=" + floor + " stamp=" + stamp
                                + " > highWater=" + highWater,
                        stamp <= highWater
                );
                Assert.assertTrue(table + " seqTxn=" + highWater + " floor=" + floor + " was not stamped", stamp >= 1);
                if (touched.contains(floor)) {
                    seenTouched.add(floor);
                    if (previous != null) {
                        Assert.assertTrue(
                                table + " seqTxn=" + highWater + " touched floor=" + floor + " did not advance"
                                        + " [previous=" + previous + ", current=" + stamp + ']',
                                stamp > previous
                        );
                    }
                } else if (previous != null) {
                    Assert.assertTrue(
                            table + " seqTxn=" + highWater + " untouched floor=" + floor + " decreased"
                                    + " [previous=" + previous + ", current=" + stamp + ']',
                            stamp >= previous
                    );
                }
                currentStamps.put(floor, stamp);
            }
        }
        Assert.assertEquals(table + " touched partitions must all be present after apply", touched, seenTouched);
        Assert.assertTrue(
                table + " must not lose partitions while replaying insert-only WAL",
                currentStamps.keySet().containsAll(previousStamps.keySet())
        );
        previousStamps.clear();
        previousStamps.putAll(currentStamps);
    }
}
