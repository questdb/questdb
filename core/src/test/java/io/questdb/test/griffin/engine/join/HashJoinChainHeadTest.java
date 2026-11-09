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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * A probe hands out the link at which its duplicate iterator stands, the head of a key's chain
 * right after the lookup, and any probe of the same build restarts iteration there: a worker that
 * defers walking a key's matches hands the head to the consumer, whose own probe walks them later,
 * in the chain order that the serial hash join's {@code LongChain} lists them in.
 */
public class HashJoinChainHeadTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    // At most this many rows a partition, so that a parallel build of t splits into many partitions.
    private static final int ROWS_PER_PARTITION = 7;

    @Test
    public void testIntKeyChainHeadsRestartChainsInChainOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(20, 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 RecordCursorFactory serialFactory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(2), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                Assert.assertTrue(frames.hasParquetFrames());
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> label(record.getInt(0)), 2,
                        null, sqlExecutionContext);
                // Every key t can hold, of one row, of many rows and NULL; key 1_091 is NULL in t, so it
                // misses, and so do 999 and -1.
                final IntList keys = new IntList();
                for (int key = 0; key < 6; key++) {
                    keys.add(key);
                }
                for (int x = 1; x <= 600; x += 10) {
                    keys.add(1_000 + x);
                }
                keys.add(Numbers.INT_NULL);
                keys.add(999);
                keys.add(-1);
                final KeyWalk walk = (probe, isUnchecked, visitor) -> {
                    final FrozenHashJoinBuild.IntProbe intProbe = (FrozenHashJoinBuild.IntProbe) probe;
                    for (int i = 0, n = keys.size(); i < n; i++) {
                        final int key = keys.getQuick(i);
                        if (isUnchecked) {
                            intProbe.findUnchecked(key);
                        } else {
                            intProbe.find(key);
                        }
                        visitor.visit(label(key), probe);
                    }
                };
                for (boolean isPartitioned : new boolean[]{false, true}) {
                    final String label = isPartitioned ? "partitioned" : "serial";
                    final FrozenHashJoinBuild.IntKeyed frozen = isPartitioned
                            ? FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, ROWS_PER_PARTITION, -1, null, tracker, NOOP)
                            : FrameBuilds.buildIntSerial(configuration, build, frames, 0, null, ORDER_ASC, tracker, NOOP);
                    Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                    try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                         FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                        assertChainHeads(label, a, b, walk, expected);
                    }
                    assertExpiredAndCounted(frozen, frozen.newProbe(), build::close, keys.getQuick(0));
                    Assert.assertEquals(label, 0, tracker.getUsed());

                    // A build of one row per key: the unique lookup positions the payload itself and
                    // leaves no chain behind.
                    final LongPredicate keepSingles = FrameBuilds.keepByValue(configuration, frames, 2, HashJoinChainHeadTest::isSingle);
                    final FrozenHashJoinBuild.IntKeyed unique = isPartitioned
                            ? FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, ROWS_PER_PARTITION, -1, keepSingles, tracker, NOOP)
                            : FrameBuilds.buildIntSerial(configuration, build, frames, 0, keepSingles, ORDER_ASC, tracker, NOOP);
                    Assert.assertEquals(label, unique.getRowCount(), unique.getKeyCount());
                    Assert.assertEquals(label, 56, unique.getRowCount());
                    try (FrozenHashJoinBuild.IntProbe probe = unique.newProbe()) {
                        for (int i = 0, n = keys.size(); i < n; i++) {
                            final int key = keys.getQuick(i);
                            final List<Long> chain = expected.get(label(key));
                            final boolean isSingle = chain != null && chain.size() == 1;
                            Assert.assertEquals(label + ", key " + key, isSingle, probe.findSingleUnchecked(key));
                            Assert.assertEquals(label + ", key " + key, 0, probe.chainHead());
                            Assert.assertFalse(label + ", key " + key, probe.hasNext());
                            if (isSingle) {
                                Assert.assertEquals(label + ", key " + key, chain.get(0).longValue(), probe.getRecord().getLong(0));
                            }
                        }
                    }
                    build.close();
                    Assert.assertEquals(label, 0, tracker.getUsed());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testStagedKeyChainHeadsRestartChainsInChainOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // p holds every text of t's s, NULL included, and one that t lacks.
            execute("CREATE TABLE p AS (SELECT DISTINCT s FROM t)");
            execute("INSERT INTO p VALUES ('absent')");
            sqlExecutionContext.changePageFrameSizes(20, 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 RecordCursorFactory serialFactory = select("t");
                 RecordCursorFactory probeFactory = select("p");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(2), factory.getMetadata());
                 MapHashJoinBuild build = new MapHashJoinBuild(configuration, new ArrayColumnTypes().add(ColumnType.VARCHAR), true, 4, 1024, 16)) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                Assert.assertTrue(frames.hasParquetFrames());
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> label(record.getVarcharA(1)), 2,
                        null, sqlExecutionContext);
                // The build stages t's s, column 1, and the probes p's s, column 0.
                final BytecodeAssembler asm = new BytecodeAssembler();
                final ListColumnFilter buildKey = new ListColumnFilter();
                buildKey.add(2);
                final ListColumnFilter probeKey = new ListColumnFilter();
                probeKey.add(1);
                final RecordSink buildSink = RecordSinkFactory.getInstance(configuration, asm, factory.getMetadata(), buildKey, null);
                final KeyWalk walk = (probe, isUnchecked, visitor) -> {
                    final FrozenHashJoinBuild.RecordProbe recordProbe = (FrozenHashJoinBuild.RecordProbe) probe;
                    try (RecordCursor cursor = probeFactory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            if (isUnchecked) {
                                recordProbe.findUnchecked(record);
                            } else {
                                recordProbe.find(record);
                            }
                            visitor.visit(label(record.getVarcharA(0)), probe);
                        }
                    }
                };
                for (boolean isPartitioned : new boolean[]{false, true}) {
                    final String label = isPartitioned ? "partitioned" : "serial";
                    final FrozenHashJoinBuild.RecordKeyed frozen = isPartitioned
                            ? FrameBuilds.buildMapPartitioned(configuration, build, frames, buildSink, ROWS_PER_PARTITION, -1, null, tracker, NOOP)
                            : FrameBuilds.buildMapSerial(configuration, build, frames, buildSink, null, ORDER_ASC, tracker, NOOP);
                    Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                    // Each probe stages its keys through a sink of its own.
                    try (FrozenHashJoinBuild.RecordProbe a = frozen.newProbe(probeSink(asm, probeFactory, probeKey));
                         FrozenHashJoinBuild.RecordProbe b = frozen.newProbe(probeSink(asm, probeFactory, probeKey))) {
                        assertChainHeads(label, a, b, walk, expected);
                    }
                    final FrozenHashJoinBuild.RecordProbe expiring = frozen.newProbe(probeSink(asm, probeFactory, probeKey));
                    try (RecordCursor cursor = probeFactory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        expiring.find(cursor.getRecord());
                        assertExpiredAndCounted(frozen, expiring, build::close, -1);
                    }
                    Assert.assertEquals(label, 0, tracker.getUsed());

                    // A build of one row per key: the unique lookup positions the payload itself and
                    // leaves no chain behind.
                    final LongPredicate keepSingles = FrameBuilds.keepByValue(configuration, frames, 2, HashJoinChainHeadTest::isSingle);
                    final FrozenHashJoinBuild.RecordKeyed unique = isPartitioned
                            ? FrameBuilds.buildMapPartitioned(configuration, build, frames, buildSink, ROWS_PER_PARTITION, -1, keepSingles, tracker, NOOP)
                            : FrameBuilds.buildMapSerial(configuration, build, frames, buildSink, keepSingles, ORDER_ASC, tracker, NOOP);
                    Assert.assertEquals(label, unique.getRowCount(), unique.getKeyCount());
                    Assert.assertEquals(label, 56, unique.getRowCount());
                    int singleCount = 0;
                    try (FrozenHashJoinBuild.RecordProbe probe = unique.newProbe(probeSink(asm, probeFactory, probeKey));
                         RecordCursor cursor = probeFactory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            final String key = label(record.getVarcharA(0));
                            final List<Long> chain = expected.get(key);
                            final boolean isSingle = chain != null && chain.size() == 1;
                            Assert.assertEquals(label + ", key " + key, isSingle, probe.findSingleUnchecked(record));
                            Assert.assertEquals(label + ", key " + key, 0, probe.chainHead());
                            Assert.assertFalse(label + ", key " + key, probe.hasNext());
                            if (isSingle) {
                                Assert.assertEquals(label + ", key " + key, chain.get(0).longValue(), probe.getRecord().getLong(0));
                                singleCount++;
                            }
                        }
                    }
                    Assert.assertEquals(label, 56, singleCount);
                    build.close();
                    Assert.assertEquals(label, 0, tracker.getUsed());
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    /**
     * Looks every key up through probe a, and checks right after each lookup that the probe's chain
     * head is 0 for a miss and otherwise restarts the key's chain, on a and on b, from the head and
     * from the link that a stands at after its first match; then restarts every head that a handed
     * out on b, in the reverse order of the lookups, as a consumer walks the heads that a worker
     * deferred, and checks that a restart replaces an iterator that stands mid-chain.
     */
    private static void assertChainHeads(
            String label,
            FrozenHashJoinBuild.Probe a,
            FrozenHashJoinBuild.Probe b,
            KeyWalk walk,
            Map<String, List<Long>> expected
    ) throws SqlException {
        final Map<String, Long> heads = new LinkedHashMap<>();
        final int[] counts = new int[3];
        walk.forEachKey(a, false, (key, probe) -> {
            final String keyLabel = label + ", key " + key;
            final long head = probe.chainHead();
            final List<Long> chain = expected.get(key);
            if (chain == null) {
                Assert.assertEquals(keyLabel, 0, head);
                Assert.assertFalse(keyLabel, probe.hasNext());
                counts[0]++;
                return;
            }
            Assert.assertTrue(keyLabel, head > 0);
            heads.put(key, head);
            counts[chain.size() == 1 ? 1 : 2]++;
            FrameBuilds.assertChain(keyLabel, probe, chain);
            Assert.assertEquals(keyLabel, 0, probe.chainHead());
            // A restart past the chain's end walks it again.
            probe.ofChain(head);
            Assert.assertEquals(keyLabel, head, probe.chainHead());
            FrameBuilds.assertChain(keyLabel + ", restarted", probe, chain);
            // After the first match the probe stands at the second, which the other probe takes over.
            probe.ofChain(head);
            probe.next();
            Assert.assertEquals(keyLabel, chain.get(0).longValue(), probe.getRecord().getLong(0));
            final long rest = probe.chainHead();
            Assert.assertEquals(keyLabel, chain.size() > 1, rest != 0);
            b.ofChain(rest);
            FrameBuilds.assertChain(keyLabel + ", rest on another probe", b, chain.subList(1, chain.size()));
            FrameBuilds.assertChain(keyLabel + ", rest", probe, chain.subList(1, chain.size()));
        });
        // Misses, keys of one row and keys of many.
        Assert.assertTrue(label + ", misses", counts[0] > 0);
        Assert.assertEquals(label + ", keys of one row", 56, counts[1]);
        Assert.assertEquals(label + ", keys of many rows", 7, counts[2]);
        Assert.assertEquals(label, expected.size(), heads.size());

        // The unchecked lookups stand at the same heads.
        walk.forEachKey(a, true, (key, probe) -> {
            final Long head = heads.get(key);
            Assert.assertEquals(label + ", unchecked key " + key, head != null ? head : 0L, probe.chainHead());
        });

        // A consumer walks the deferred heads later, through a probe of its own.
        final List<String> keys = new ArrayList<>(heads.keySet());
        Collections.reverse(keys);
        for (String key : keys) {
            b.ofChain(heads.get(key));
            FrameBuilds.assertChain(label + ", deferred key " + key, b, expected.get(key));
        }

        // A restart replaces an iterator that stands mid-chain, and a restart at 0 leaves it exhausted.
        String longest = null;
        for (String key : keys) {
            if (longest == null || expected.get(key).size() > expected.get(longest).size()) {
                longest = key;
            }
        }
        final String other = keys.get(0).equals(longest) ? keys.get(1) : keys.get(0);
        a.ofChain(heads.get(longest));
        a.next();
        Assert.assertTrue(a.hasNext());
        a.ofChain(heads.get(other));
        FrameBuilds.assertChain(label + ", replaced by key " + other, a, expected.get(other));
        a.ofChain(heads.get(longest));
        a.ofChain(0);
        Assert.assertFalse(label, a.hasNext());
        Assert.assertEquals(label, 0, a.chainHead());

        // A value that no link takes trips the probe's assertion, and leaves it as it stood.
        final long head = heads.get(longest);
        a.ofChain(head);
        Assert.assertThrows(AssertionError.class, () -> a.ofChain(head + 1));
        Assert.assertThrows(AssertionError.class, () -> a.ofChain(-2));
        Assert.assertThrows(AssertionError.class, () -> a.ofChain(4));
        long maxHead = 0;
        for (long value : heads.values()) {
            maxHead = Math.max(maxHead, value);
        }
        final long pastLastRow = maxHead + 16 * 1_000;
        Assert.assertThrows(AssertionError.class, () -> a.ofChain(pastLastRow));
        Assert.assertEquals(label, head, a.chainHead());
        FrameBuilds.assertChain(label + ", after misuse", a, expected.get(longest));
    }

    /**
     * Checks that a build which counted its key rows keeps no chain to stand in or restart, and that
     * the probe of a closed build trips its assertion, for the probe given, which a lookup has
     * positioned, and which this closes.
     */
    private static void assertExpiredAndCounted(
            FrozenHashJoinBuild frozen,
            FrozenHashJoinBuild.Probe probe,
            Runnable closeBuild,
            int intKey
    ) {
        try {
            final long head = probe.chainHead();
            frozen.countKeyRows();
            Assert.assertThrows(AssertionError.class, probe::chainHead);
            Assert.assertThrows(AssertionError.class, () -> probe.ofChain(head));
            closeBuild.run();
            Assert.assertThrows(AssertionError.class, probe::chainHead);
            Assert.assertThrows(AssertionError.class, () -> probe.ofChain(0));
            if (probe instanceof FrozenHashJoinBuild.IntProbe intProbe) {
                Assert.assertThrows(AssertionError.class, () -> intProbe.find(intKey));
            }
        } finally {
            probe.close();
        }
    }

    // t holds 600 rows over six daily partitions, the first two of them Parquet. Its INT key k holds
    // 56 keys of one row, 1_000 plus x for every tenth x from 1 but the four that are multiples of 13
    // (91, 221, 351 and 481), seven keys of many rows, 0 to 5 and NULL, which repeat within frames,
    // across frames, partitions and formats; s holds k's key as text, and v numbers the rows from one.
    private static void createTables() throws SqlException {
        execute("CREATE TABLE t (k INT, s VARCHAR, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t
                SELECT CASE WHEN x % 13 = 0 THEN NULL WHEN x % 10 = 1 THEN (1_000 + x)::INT ELSE (x % 6)::INT END,
                       CASE WHEN x % 13 = 0 THEN NULL WHEN x % 10 = 1 THEN 'one' || x ELSE 'many' || (x % 6) END,
                       x, timestamp_sequence('2020-01-01', 864_000_000L)
                FROM long_sequence(600)
                """);
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
    }

    private static IntList ints(int... values) {
        final IntList list = new IntList();
        for (int value : values) {
            list.add(value);
        }
        return list;
    }

    // The row with this v holds a key of its own: every tenth row from the first, but for a NULL key.
    private static boolean isSingle(long v) {
        return v % 10 == 1 && v % 13 != 0;
    }

    private static String label(int key) {
        return key == Numbers.INT_NULL ? "null" : Integer.toString(key);
    }

    private static String label(Utf8Sequence key) {
        return key == null ? "null" : key.toString();
    }

    private static RecordSink probeSink(BytecodeAssembler asm, RecordCursorFactory probeFactory, ListColumnFilter probeKey) {
        return RecordSinkFactory.getInstance(configuration, asm, probeFactory.getMetadata(), probeKey, null);
    }

    @FunctionalInterface
    private interface KeyVisitor {
        void visit(String key, FrozenHashJoinBuild.Probe probe);
    }

    /**
     * Looks every key of a fixed list up through the probe, in the list's order, through the checked
     * or the unchecked lookup, and hands the key's label and the probe to the visitor right after each.
     */
    @FunctionalInterface
    private interface KeyWalk {
        void forEachKey(FrozenHashJoinBuild.Probe probe, boolean isUnchecked, KeyVisitor visitor) throws SqlException;
    }
}
