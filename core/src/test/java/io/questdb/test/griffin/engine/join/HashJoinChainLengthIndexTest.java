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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * A frozen build indexes the length of every chain of rows beside its links, for a count of the
 * joined pairs that the same execution may read afterwards: any probe of the build then reads the
 * matches that a walk from any link would visit, the key's row count right after a lookup, 0 on a
 * miss, and the matches left after each match, with one read, and still walks the chains in the
 * order that the serial hash join's {@code LongChain} lists them in. The pass takes 4 bytes a row,
 * which the build's tracker charges once per execution, checks the breaker that it is given once
 * per MiB of rows, and a failure leaves the build as it was. Every check runs over rows of 16 bytes,
 * with payload, and of 8 bytes, without, over INT keys and staged VARCHAR keys, built serially and
 * in partitions.
 */
public class HashJoinChainLengthIndexTest extends AbstractCairoTest {
    private static final long MIB = 1024 * 1024;
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    // At most this many rows a partition, so that a parallel build of t splits into many partitions.
    private static final int ROWS_PER_PARTITION = 7;

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBackwardBuildChainLengths() throws Exception {
        assertMemoryLeak(() -> {
            HashJoinChainHeadTest.createTables();
            sqlExecutionContext.changePageFrameSizes(20, 20);
            // A build over a backward scan meets each frame's rows from the last, and lists each key's
            // rows from its first in the table to its last, as the serial hash join over that scan does.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory backward = select("t ORDER BY ts DESC");
                 RecordCursorFactory serialBackward = select("t ORDER BY ts DESC");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, HashJoinChainHeadTest.ints(2), backward.getMetadata())) {
                Assert.assertEquals(ORDER_DESC, FrameBuilds.scanOrder(backward));
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialBackward, record -> HashJoinChainHeadTest.label(record.getInt(0)), 2,
                        null, sqlExecutionContext);
                final KeyWalk walk = intKeyWalk();
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 16)) {
                        for (boolean isPartitioned : new boolean[]{false, true}) {
                            final String label = label(hasPayload, isPartitioned) + ", backward";
                            frames.open(backward, ORDER_DESC, sqlExecutionContext);
                            frames.loadFrames();
                            final FrozenHashJoinBuild.IntKeyed frozen = isPartitioned
                                    ? FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, ROWS_PER_PARTITION, -1, null, ORDER_DESC, tracker, NOOP)
                                    : FrameBuilds.buildIntSerial(configuration, build, frames, 0, null, ORDER_DESC, tracker, NOOP);
                            Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                            try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                                 FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                                assertIndexed(label, frozen, a, tracker);
                                assertChainLengths(label, a, b, walk, expected, hasPayload);
                            }
                            build.close();
                            frames.clear();
                            Assert.assertEquals(label, 0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCancelledPassLeavesTheChainsAndItsBlockToTheNextPass() throws Exception {
        assertMemoryLeak(() -> {
            HashJoinChainHeadTest.createTables();
            sqlExecutionContext.changePageFrameSizes(20, 20);
            final AtomicBooleanCircuitBreaker cancelled = new AtomicBooleanCircuitBreaker(engine);
            cancelled.cancel();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 RecordCursorFactory serialFactory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, HashJoinChainHeadTest.ints(2), factory.getMetadata())) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> HashJoinChainHeadTest.label(record.getInt(0)), 2,
                        null, sqlExecutionContext);
                final KeyWalk walk = intKeyWalk();
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 16)) {
                        for (boolean isPartitioned : new boolean[]{false, true}) {
                            final String label = label(hasPayload, isPartitioned);
                            final FrozenHashJoinBuild.IntKeyed frozen = buildInt(build, frames, isPartitioned, tracker, NOOP);
                            final long used = tracker.getUsed();
                            final long indexBytes = frozen.getRowCount() * Integer.BYTES;
                            // The pass allocates its block, then meets the cancellation at its first check.
                            final CairoException e = Assert.assertThrows(CairoException.class, () -> frozen.indexChainLengths(cancelled));
                            Assert.assertTrue(label + ": " + e.getMessage(), e.isCancellation());
                            TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                            Assert.assertFalse(label, frozen.hasChainLengthIndex());
                            Assert.assertEquals(label, used + indexBytes, tracker.getUsed());
                            try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                                assertChainsWalk(label + ", cancelled", probe, walk, expected, hasPayload);
                            }
                            // The next pass indexes into the same block.
                            frozen.indexChainLengths(NOOP);
                            Assert.assertTrue(label, frozen.hasChainLengthIndex());
                            Assert.assertEquals(label, used + indexBytes, tracker.getUsed());
                            try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                                 FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                                assertChainLengths(label + ", retried", a, b, walk, expected, hasPayload);
                            }
                            build.close();
                            Assert.assertEquals(label, 0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIntKeyChainLengths() throws Exception {
        assertMemoryLeak(() -> {
            HashJoinChainHeadTest.createTables();
            sqlExecutionContext.changePageFrameSizes(20, 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 RecordCursorFactory serialFactory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, HashJoinChainHeadTest.ints(2), factory.getMetadata())) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                Assert.assertTrue(frames.hasParquetFrames());
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> HashJoinChainHeadTest.label(record.getInt(0)), 2,
                        null, sqlExecutionContext);
                final KeyWalk walk = intKeyWalk();
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 16)) {
                        for (boolean isPartitioned : new boolean[]{false, true}) {
                            final String label = label(hasPayload, isPartitioned);
                            final FrozenHashJoinBuild.IntKeyed frozen = buildInt(build, frames, isPartitioned, tracker, NOOP);
                            Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                            try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                                 FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                                assertIndexed(label, frozen, a, tracker);
                                assertChainLengths(label, a, b, walk, expected, hasPayload);
                            }
                            assertExpired(label, frozen, frozen.newProbe(), build::close);
                            Assert.assertEquals(label, 0, tracker.getUsed());
                            // A build that counted its key rows keeps no link to index.
                            final FrozenHashJoinBuild.IntKeyed counted = buildInt(build, frames, isPartitioned, tracker, NOOP);
                            counted.countKeyRows();
                            assertCountedRefusesThePass(label, counted);
                            build.close();
                            Assert.assertEquals(label, 0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMemoryLimitInPass() throws Exception {
        assertMemoryLeak(() -> {
            HashJoinChainHeadTest.createTables();
            sqlExecutionContext.changePageFrameSizes(20, 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 RecordCursorFactory serialFactory = select("t");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, HashJoinChainHeadTest.ints(2), factory.getMetadata())) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> HashJoinChainHeadTest.label(record.getInt(0)), 2,
                        null, sqlExecutionContext);
                final KeyWalk walk = intKeyWalk();
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 16)) {
                        for (boolean isPartitioned : new boolean[]{false, true}) {
                            final String label = label(hasPayload, isPartitioned);
                            // A limit one byte short of the block fails the pass before it writes.
                            FrozenHashJoinBuild.IntKeyed frozen = buildInt(build, frames, isPartitioned, tracker, NOOP);
                            long used = tracker.getUsed();
                            final long indexBytes = frozen.getRowCount() * Integer.BYTES;
                            tracker.setLimit(used + indexBytes - 1);
                            final FrozenHashJoinBuild.IntKeyed failed = frozen;
                            final CairoException e = Assert.assertThrows(CairoException.class, () -> failed.indexChainLengths(NOOP));
                            Assert.assertTrue(label + ": " + e.getMessage(), e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            Assert.assertFalse(label, frozen.hasChainLengthIndex());
                            Assert.assertEquals(label, used, tracker.getUsed());
                            tracker.setLimit(Long.MAX_VALUE);
                            try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                                assertChainsWalk(label + ", over the limit", probe, walk, expected, hasPayload);
                            }
                            build.close();
                            Assert.assertEquals(label, 0, tracker.getUsed());

                            // A limit that holds the block exactly lets the pass index.
                            frozen = buildInt(build, frames, isPartitioned, tracker, NOOP);
                            used = tracker.getUsed();
                            tracker.setLimit(used + indexBytes);
                            frozen.indexChainLengths(NOOP);
                            Assert.assertTrue(label, frozen.hasChainLengthIndex());
                            Assert.assertEquals(label, used + indexBytes, tracker.getUsed());
                            tracker.setLimit(Long.MAX_VALUE);
                            try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                                 FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                                assertChainLengths(label + ", at the limit", a, b, walk, expected, hasPayload);
                            }
                            build.close();
                            Assert.assertEquals(label, 0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPassChecksTheBreakerThatItIsGiven() throws Exception {
        assertMemoryLeak(() -> {
            // 200_000 rows over seven keys: chains of about 28_571 rows, which run across every MiB of
            // the heap, 4 MiB of rows of 16 bytes, but for a part, and 2 MiB of rows of 8 bytes.
            execute("CREATE TABLE big AS (SELECT (x % 7)::INT k, x v FROM long_sequence(200_000))");
            sqlExecutionContext.changePageFrameSizes(20_000, 20_000);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("big");
                 RecordCursorFactory serialFactory = select("big");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, HashJoinChainHeadTest.ints(1), factory.getMetadata())) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                Assert.assertEquals(10, frames.getFrameCount());
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> HashJoinChainHeadTest.label(record.getInt(0)), 1,
                        null, sqlExecutionContext);
                final KeyWalk walk = (probe, visitor) -> {
                    for (int key = -1; key < 8; key++) {
                        ((FrozenHashJoinBuild.IntProbe) probe).find(key);
                        visitor.visit(HashJoinChainHeadTest.label(key), probe);
                    }
                };
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 16)) {
                        for (boolean isPartitioned : new boolean[]{false, true}) {
                            final String label = label(hasPayload, isPartitioned);
                            // The build opens with one breaker and the pass takes another.
                            final CountingSqlExecutionCircuitBreaker buildBreaker = new CountingSqlExecutionCircuitBreaker(NOOP);
                            final FrozenHashJoinBuild.IntKeyed frozen = isPartitioned
                                    ? FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 50_000, -1, null, tracker, buildBreaker)
                                    : FrameBuilds.buildIntSerial(configuration, build, frames, 0, null, ORDER_ASC, tracker, buildBreaker);
                            Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                            final long buildChecks = buildBreaker.getCheckCount();
                            final CountingSqlExecutionCircuitBreaker passBreaker = new CountingSqlExecutionCircuitBreaker(NOOP);
                            frozen.indexChainLengths(passBreaker);
                            final long rowBytes = frozen.getRowCount() * (hasPayload ? 16 : 8);
                            Assert.assertEquals(label, hasPayload ? 4 : 2, (rowBytes + MIB - 1) / MIB);
                            Assert.assertEquals(label, (rowBytes + MIB - 1) / MIB, passBreaker.getCheckCount());
                            Assert.assertEquals(label, buildChecks, buildBreaker.getCheckCount());
                            try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                                 FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                                assertChainLengths(label, a, b, walk, expected, hasPayload);
                            }
                            build.close();
                            Assert.assertEquals(label, 0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testStagedKeyChainLengths() throws Exception {
        assertMemoryLeak(() -> {
            HashJoinChainHeadTest.createTables();
            // p holds every text of t's s, NULL included, and one that t lacks.
            execute("CREATE TABLE p AS (SELECT DISTINCT s FROM t)");
            execute("INSERT INTO p VALUES ('absent')");
            sqlExecutionContext.changePageFrameSizes(20, 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("t");
                 RecordCursorFactory serialFactory = select("t");
                 RecordCursorFactory probeFactory = select("p");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, HashJoinChainHeadTest.ints(2), factory.getMetadata())) {
                frames.open(factory, ORDER_ASC, sqlExecutionContext);
                frames.loadFrames();
                final Map<String, List<Long>> expected = FrameBuilds.longChains(serialFactory, record -> HashJoinChainHeadTest.label(record.getVarcharA(1)), 2,
                        null, sqlExecutionContext);
                // The build stages t's s, column 1, and the probes p's s, column 0.
                final BytecodeAssembler asm = new BytecodeAssembler();
                final ListColumnFilter buildKey = new ListColumnFilter();
                buildKey.add(2);
                final ListColumnFilter probeKey = new ListColumnFilter();
                probeKey.add(1);
                final RecordSink buildSink = RecordSinkFactory.getInstance(configuration, asm, factory.getMetadata(), buildKey, null);
                final KeyWalk walk = (probe, visitor) -> {
                    try (RecordCursor cursor = probeFactory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            ((FrozenHashJoinBuild.RecordProbe) probe).find(record);
                            visitor.visit(HashJoinChainHeadTest.label(record.getVarcharA(0)), probe);
                        }
                    }
                };
                for (boolean hasPayload : new boolean[]{true, false}) {
                    try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new ArrayColumnTypes().add(ColumnType.VARCHAR), hasPayload, 4, 1024, 16)) {
                        for (boolean isPartitioned : new boolean[]{false, true}) {
                            final String label = label(hasPayload, isPartitioned);
                            final FrozenHashJoinBuild.RecordKeyed frozen = buildMap(build, frames, buildSink, isPartitioned, tracker);
                            Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                            // Each probe stages its keys through a sink of its own.
                            try (FrozenHashJoinBuild.RecordProbe a = frozen.newProbe(HashJoinChainHeadTest.probeSink(asm, probeFactory, probeKey));
                                 FrozenHashJoinBuild.RecordProbe b = frozen.newProbe(HashJoinChainHeadTest.probeSink(asm, probeFactory, probeKey))) {
                                assertIndexed(label, frozen, a, tracker);
                                assertChainLengths(label, a, b, walk, expected, hasPayload);
                            }
                            assertExpired(label, frozen, frozen.newProbe(HashJoinChainHeadTest.probeSink(asm, probeFactory, probeKey)), build::close);
                            Assert.assertEquals(label, 0, tracker.getUsed());
                            final FrozenHashJoinBuild.RecordKeyed counted = buildMap(build, frames, buildSink, isPartitioned, tracker);
                            counted.countKeyRows();
                            assertCountedRefusesThePass(label, counted);
                            build.close();
                            Assert.assertEquals(label, 0, tracker.getUsed());
                        }
                    }
                }
            }
        });
    }

    /**
     * Looks every key up through probe a, and checks that a reads the key's row count as the chain
     * length of the lookup's head, 0 for a miss, and then the matches left after each match that it
     * walks, in the chain order that the expected chains list, while b reads the same lengths at the
     * same links. Keys of one row, keys of many and misses all come up.
     */
    private static void assertChainLengths(
            String label,
            FrozenHashJoinBuild.Probe a,
            FrozenHashJoinBuild.Probe b,
            KeyWalk walk,
            Map<String, List<Long>> expected,
            boolean hasPayload
    ) throws SqlException {
        final int[] counts = new int[3];
        walk.forEachKey(a, (key, probe) -> {
            final String keyLabel = label + ", key " + key;
            final List<Long> chain = expected.get(key);
            final int size = chain != null ? chain.size() : 0;
            counts[size == 0 ? 0 : size == 1 ? 1 : 2]++;
            final long head = probe.chainHead();
            Assert.assertEquals(keyLabel, size, probe.chainLength(head));
            Assert.assertEquals(keyLabel, size, b.chainLength(head));
            b.ofChain(head);
            Assert.assertEquals(keyLabel, size, b.chainLength(b.chainHead()));
            for (int i = 0; i < size; i++) {
                Assert.assertTrue(keyLabel, probe.hasNext());
                probe.next();
                if (hasPayload) {
                    Assert.assertEquals(keyLabel + ", match " + i, chain.get(i).longValue(), probe.getRecord().getLong(0));
                }
                Assert.assertEquals(keyLabel + ", after match " + i, size - 1 - i, probe.chainLength(probe.chainHead()));
            }
            Assert.assertFalse(keyLabel, probe.hasNext());
        });
        // Every key of the build came up, and keys that it lacks.
        Assert.assertTrue(label + ", misses", counts[0] > 0);
        Assert.assertEquals(label + ", keys of one row", expected.values().stream().filter(chain -> chain.size() == 1).count(), counts[1]);
        Assert.assertEquals(label + ", keys of many rows", expected.values().stream().filter(chain -> chain.size() > 1).count(), counts[2]);
        Assert.assertTrue(label + ", keys of many rows", counts[2] > 0);
        Assert.assertEquals(label, 0, a.chainLength(0));
        Assert.assertEquals(label, 0, b.chainLength(0));
    }

    /**
     * Walks the chain of every key that the walk looks up, in the chain order that the expected
     * chains list, without reading a chain length: a probe of a build without the index.
     */
    private static void assertChainsWalk(
            String label,
            FrozenHashJoinBuild.Probe probe,
            KeyWalk walk,
            Map<String, List<Long>> expected,
            boolean hasPayload
    ) throws SqlException {
        walk.forEachKey(probe, (key, p) -> {
            final List<Long> chain = expected.get(key);
            if (hasPayload) {
                FrameBuilds.assertChain(label + ", key " + key, p, chain);
            } else {
                int walked = 0;
                while (p.hasNext()) {
                    p.next();
                    walked++;
                }
                Assert.assertEquals(label + ", key " + key, chain != null ? chain.size() : 0, walked);
            }
        });
    }

    // A build that counted its key rows refuses the pass, and keeps no index.
    private static void assertCountedRefusesThePass(String label, FrozenHashJoinBuild counted) {
        final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, () -> counted.indexChainLengths(NOOP));
        TestUtils.assertContains(e.getMessage(), "hash join build rows are already counted");
        Assert.assertFalse(label, counted.hasChainLengthIndex());
    }

    // Once the build closes, its snapshot refuses the pass, and the probe that a lookup positioned
    // trips its expiry assertion; the probe closes here.
    private static void assertExpired(String label, FrozenHashJoinBuild frozen, FrozenHashJoinBuild.Probe probe, Runnable closeBuild) {
        try {
            Assert.assertTrue(label, frozen.hasChainLengthIndex());
            final long length = probe.chainLength(0);
            Assert.assertEquals(label, 0, length);
            closeBuild.run();
            Assert.assertFalse(label, frozen.hasChainLengthIndex());
            final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, () -> frozen.indexChainLengths(NOOP));
            TestUtils.assertContains(e.getMessage(), "hash join build has expired");
            Assert.assertThrows(AssertionError.class, () -> probe.chainLength(0));
        } finally {
            probe.close();
        }
    }

    // Before the pass, a probe trips its assertion on a chain length; the pass then charges the
    // tracker 4 bytes a row, and a second pass of the execution trips its own assertion.
    private static void assertIndexed(String label, FrozenHashJoinBuild frozen, FrozenHashJoinBuild.Probe probe, LimitedMemoryTracker tracker) {
        Assert.assertFalse(label, frozen.hasChainLengthIndex());
        Assert.assertThrows(AssertionError.class, () -> probe.chainLength(0));
        final long used = tracker.getUsed();
        frozen.indexChainLengths(NOOP);
        Assert.assertTrue(label, frozen.hasChainLengthIndex());
        Assert.assertEquals(label, used + frozen.getRowCount() * Integer.BYTES, tracker.getUsed());
        Assert.assertThrows(AssertionError.class, () -> frozen.indexChainLengths(NOOP));
        Assert.assertEquals(label, used + frozen.getRowCount() * Integer.BYTES, tracker.getUsed());
    }

    private static FrozenHashJoinBuild.IntKeyed buildInt(
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            boolean isPartitioned,
            LimitedMemoryTracker tracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        return isPartitioned
                ? FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, ROWS_PER_PARTITION, -1, null, tracker, circuitBreaker)
                : FrameBuilds.buildIntSerial(configuration, build, frames, 0, null, ORDER_ASC, tracker, circuitBreaker);
    }

    private static FrozenHashJoinBuild.RecordKeyed buildMap(
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordSink buildSink,
            boolean isPartitioned,
            LimitedMemoryTracker tracker
    ) {
        return isPartitioned
                ? FrameBuilds.buildMapPartitioned(configuration, build, frames, buildSink, ROWS_PER_PARTITION, -1, null, tracker, NOOP)
                : FrameBuilds.buildMapSerial(configuration, build, frames, buildSink, null, ORDER_ASC, tracker, NOOP);
    }

    // Looks up every key that t can hold, of one row, of many rows and NULL, and misses: key 1_091 is
    // NULL in t, and so are the other multiples of 13 among 1_000 plus every tenth x, and t lacks 999
    // and -1.
    private static KeyWalk intKeyWalk() {
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
        return (probe, visitor) -> {
            for (int i = 0, n = keys.size(); i < n; i++) {
                final int key = keys.getQuick(i);
                ((FrozenHashJoinBuild.IntProbe) probe).find(key);
                visitor.visit(HashJoinChainHeadTest.label(key), probe);
            }
        };
    }

    private static String label(boolean hasPayload, boolean isPartitioned) {
        return (hasPayload ? "rows of 16 bytes, " : "rows of 8 bytes, ") + (isPartitioned ? "partitioned" : "serial");
    }

    @FunctionalInterface
    private interface KeyVisitor {
        void visit(String key, FrozenHashJoinBuild.Probe probe);
    }

    /**
     * Looks every key of a fixed set up through the probe and hands the key's label and the probe to
     * the visitor right after each lookup.
     */
    @FunctionalInterface
    private interface KeyWalk {
        void forEachKey(FrozenHashJoinBuild.Probe probe, KeyVisitor visitor) throws SqlException;
    }
}
