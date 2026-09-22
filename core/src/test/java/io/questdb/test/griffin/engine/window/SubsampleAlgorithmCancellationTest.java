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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.table.LttbAlgorithm;
import io.questdb.griffin.engine.table.M4Algorithm;
import io.questdb.griffin.engine.table.MinMaxAlgorithm;
import io.questdb.griffin.engine.table.SubsampleAlgorithm;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

public class SubsampleAlgorithmCancellationTest extends AbstractCairoTest {
    private static final int ROWS = 32_769;
    private static final DefaultSqlExecutionCircuitBreakerConfiguration BREAKER_CONFIGURATION = new DefaultSqlExecutionCircuitBreakerConfiguration() {
        @Override
        public int getCircuitBreakerThrottle() {
            return 2_000_000;
        }
    };

    @Test
    public void testCancellationDuringEmptyBuckets() throws Exception {
        assertEmptyBucketCancellation(M4Algorithm.INSTANCE);
        assertEmptyBucketCancellation(MinMaxAlgorithm.INSTANCE);
    }

    @Test
    public void testLttbGapPhases() throws Exception {
        for (Phase phase : List.of(Phase.GAP_SCAN, Phase.GAP_FLOOR, Phase.GAP_TARGETS, Phase.GAP_EMIT)) {
            assertPhase(new LttbAlgorithm(1), phase, 500, 1, false, false);
        }
        // This target leaves budget above the two-point floor: exercise the other target loop.
        assertPhase(new LttbAlgorithm(1), Phase.GAP_TARGETS, ROWS / 2, 8, false, false);
    }

    @Test
    public void testLttbPreselection() throws Exception {
        assertPhase(new LttbAlgorithm(0), Phase.PRESELECT, 500, 0, false, false);
        assertPhase(new LttbAlgorithm(0), Phase.PRESELECT, 500, 0, true, false);
    }

    @Test
    public void testLttbRescale() throws Exception {
        assertPhase(new LttbAlgorithm(0), Phase.RESCALE, ROWS / 2, 0, false, true);
    }

    @Test
    public void testLttbTriangles() throws Exception {
        assertPhase(new LttbAlgorithm(0), Phase.TRIANGLES, ROWS / 2, 0, false, false);
        assertPhase(new LttbAlgorithm(0), Phase.TRIANGLES, ROWS / 2, 0, true, false);
    }

    @Test
    public void testM4() throws Exception {
        assertPhase(M4Algorithm.INSTANCE, Phase.BUCKETS, 500, 0, false, false);
        assertPhase(M4Algorithm.INSTANCE, Phase.BUCKETS, 500, 0, true, false);
    }

    @Test
    public void testMinMax() throws Exception {
        assertPhase(MinMaxAlgorithm.INSTANCE, Phase.BUCKETS, 500, 0, false, false);
        assertPhase(MinMaxAlgorithm.INSTANCE, Phase.BUCKETS, 500, 0, true, false);
    }

    @Test
    public void testProbeRateWithEmptyBuckets() throws Exception {
        assertProbeRate(M4Algorithm.INSTANCE, ROWS - 1, 0, true);
        assertProbeRate(MinMaxAlgorithm.INSTANCE, ROWS - 1, 0, true);
    }

    @Test
    public void testProbeRateWithTinyLttbSegments() throws Exception {
        assertProbeRate(new LttbAlgorithm(1), 500, 1, false);
        assertProbeRate(new LttbAlgorithm(1), ROWS / 2, 8, false);
    }

    private void assertEmptyBucketCancellation(SubsampleAlgorithm algorithm) throws Exception {
        assertMemoryLeak(() -> {
            final long bytes = (long) ROWS * SubsampleAlgorithm.ENTRY_SIZE;
            final long buffer = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
            // Leave the final input index unaligned with the checkpoint mask: a row-only check
            // at that last row must not conceal a skipped walk through thousands of empty buckets.
            final int count = ROWS - 1;
            final long lastFirstBucketIndex = algorithm == M4Algorithm.INSTANCE ? count - 2 : 96;
            try (
                    NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, BREAKER_CONFIGURATION);
                    DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT) {
                        @Override
                        public void add(long value) {
                            super.add(value);
                            if (value == lastFirstBucketIndex) {
                                breaker.cancel();
                            }
                        }
                    }
            ) {
                fill(buffer, 0, false, false, true);
                Unsafe.getUnsafe().putLong(buffer + (long) (count - 1) * SubsampleAlgorithm.ENTRY_SIZE, 1_000_000_000L);
                breaker.statefulThrowExceptionIfTripped();
                try {
                    algorithm.select(buffer, count, count - 1, false, selected, breaker);
                    Assert.fail("expected cancellation during empty buckets");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                    Assert.assertTrue(breaker.checkIfTripped());
                    Assert.assertEquals(lastFirstBucketIndex, selected.get(selected.size() - 1));
                    Assert.assertEquals(algorithm == M4Algorithm.INSTANCE ? 3 : 2, selected.size());
                }
            } finally {
                Unsafe.free(buffer, bytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void assertPhase(SubsampleAlgorithm algorithm, Phase phase, int target, int segmentSize, boolean hasIntegralValues, boolean hasHugeValues) throws Exception {
        assertMemoryLeak(() -> {
            final long bytes = (long) ROWS * SubsampleAlgorithm.ENTRY_SIZE;
            final long buffer = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
            try (
                    DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList expected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)
            ) {
                fill(buffer, segmentSize, hasIntegralValues, hasHugeValues, false);
                for (int mode = 0; mode < 2; mode++) {
                    // Reset scratch so phase detection sees this invocation's progress, not the
                    // previous run's lists. The normal reuse assertion below does not reset it.
                    if (algorithm instanceof LttbAlgorithm lttb) {
                        lttb.close();
                    }
                    try (PhaseBreaker breaker = new PhaseBreaker(algorithm, selected, phase, segmentSize, mode == 0)) {
                        // Put the count-throttled API inside its window. A real check on its first
                        // call must not accidentally make sparse, double-throttled checks pass.
                        breaker.statefulThrowExceptionIfTripped();
                        try {
                            algorithm.select(buffer, ROWS, target, hasIntegralValues, selected, breaker);
                            Assert.fail("expected cancellation in " + phase);
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isCancellation());
                            Assert.assertTrue("phase checkpoint missing: " + phase, breaker.hasReachedPhase);
                            Assert.assertEquals(mode == 0 ? 1 : 2, breaker.phaseChecks);
                            Assert.assertTrue("unbounded output after cancellation", selected.size() - breaker.selectedAtCancel <= 2048);
                        }
                        Assert.assertTrue(breaker.checkIfTripped());
                    }
                    // Compare a reopened, partially populated algorithm with a fresh instance.
                    final SubsampleAlgorithm fresh = algorithm instanceof LttbAlgorithm
                            ? new LttbAlgorithm(segmentSize > 0 ? 1 : 0) : algorithm;
                    try (NetworkSqlExecutionCircuitBreaker replacement = new NetworkSqlExecutionCircuitBreaker(engine, BREAKER_CONFIGURATION)) {
                        algorithm.select(buffer, ROWS, target, hasIntegralValues, selected, replacement);
                        fresh.select(buffer, ROWS, target, hasIntegralValues, expected, replacement);
                        Assert.assertEquals(expected.size(), selected.size());
                        for (long i = 0; i < expected.size(); i++) {
                            Assert.assertEquals(expected.get(i), selected.get(i));
                        }
                    } finally {
                        if (fresh instanceof LttbAlgorithm lttb) {
                            lttb.close();
                        }
                    }
                }
            } finally {
                Unsafe.free(buffer, bytes, MemoryTag.NATIVE_DEFAULT);
                if (algorithm instanceof LttbAlgorithm lttb) {
                    lttb.close();
                }
            }
        });
    }

    private void assertProbeRate(SubsampleAlgorithm algorithm, int target, int segmentSize, boolean hasEmptyBuckets) throws Exception {
        assertMemoryLeak(() -> {
            final long bytes = (long) ROWS * SubsampleAlgorithm.ENTRY_SIZE;
            final long buffer = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
            try (
                    CountingBreaker breaker = new CountingBreaker();
                    DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)
            ) {
                fill(buffer, segmentSize, false, false, hasEmptyBuckets);
                algorithm.select(buffer, ROWS, target, false, selected, breaker);
                Assert.assertTrue("no cancellation checkpoints", breaker.checks > 0);
                // A generous work-based ceiling catches a clock read per bucket/segment without
                // timing assertions. Empty buckets repeatedly visit the same input index.
                Assert.assertTrue("too many real checks: " + breaker.checks, breaker.checks < ROWS / 64);
            } finally {
                Unsafe.free(buffer, bytes, MemoryTag.NATIVE_DEFAULT);
                if (algorithm instanceof LttbAlgorithm lttb) {
                    lttb.close();
                }
            }
        });
    }

    private static void fill(long buffer, int segmentSize, boolean hasIntegralValues, boolean hasHugeValues, boolean hasEmptyBuckets) {
        for (int i = 0; i < ROWS; i++) {
            final long ts = hasEmptyBuckets ? (i == ROWS - 1 ? 1_000_000_000L : 0)
                    : segmentSize > 0 ? (long) (i / segmentSize) * 1000 + i % segmentSize : i;
            final long address = buffer + (long) i * SubsampleAlgorithm.ENTRY_SIZE;
            Unsafe.getUnsafe().putLong(address, ts);
            if (hasIntegralValues) {
                Unsafe.getUnsafe().putLong(address + 8, Long.MAX_VALUE - (i % 97));
            } else {
                Unsafe.getUnsafe().putDouble(address + 8, hasHugeValues ? Double.MAX_VALUE : i % 97);
            }
        }
    }

    private static DirectLongList list(LttbAlgorithm algorithm, String name) {
        try {
            Field field = LttbAlgorithm.class.getDeclaredField(name);
            field.setAccessible(true);
            return (DirectLongList) field.get(algorithm);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private enum Phase {
        BUCKETS, PRESELECT, TRIANGLES, RESCALE, GAP_SCAN, GAP_FLOOR, GAP_TARGETS, GAP_EMIT
    }

    private static class CountingBreaker extends NetworkSqlExecutionCircuitBreaker {
        private int checks;

        CountingBreaker() {
            super(engine, BREAKER_CONFIGURATION);
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            checks++;
            super.statefulThrowExceptionIfTrippedTimeThrottled();
        }
    }

    private static class PhaseBreaker extends NetworkSqlExecutionCircuitBreaker {
        private final SubsampleAlgorithm algorithm;
        private final boolean isPreCancelled;
        private final Phase phase;
        private final int segmentCount;
        private final DirectLongList selected;
        private boolean hasReachedPhase;
        private int phaseChecks;
        private long selectedAtCancel;

        PhaseBreaker(SubsampleAlgorithm algorithm, DirectLongList selected, Phase phase, int segmentSize, boolean isPreCancelled) {
            super(engine, BREAKER_CONFIGURATION);
            this.algorithm = algorithm;
            this.selected = selected;
            this.phase = phase;
            this.segmentCount = segmentSize > 0 ? (ROWS + segmentSize - 1) / segmentSize : 0;
            this.isPreCancelled = isPreCancelled;
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            if (!hasReachedPhase && isInPhase()) {
                hasReachedPhase = true;
                selectedAtCancel = selected.size();
                if (isPreCancelled) {
                    cancel();
                }
            }
            if (hasReachedPhase) {
                phaseChecks++;
            }
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            if (hasReachedPhase && phaseChecks == 1) {
                cancel();
            }
        }

        private boolean isInPhase() {
            for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                if (!frame.getClassName().startsWith("io.questdb.griffin.engine.table.")) {
                    continue;
                }
                final String method = frame.getMethodName();
                switch (phase) {
                    case BUCKETS -> {
                        return method.equals("select");
                    }
                    case PRESELECT -> {
                        return method.equals("preselectMinMax");
                    }
                    case TRIANGLES -> {
                        return method.equals("lttbCore");
                    }
                    case RESCALE -> {
                        return method.equals("maxAreaIndexRescaled");
                    }
                    default -> {
                        if (!method.equals("selectGapPreserving")) {
                            return false;
                        }
                        final LttbAlgorithm lttb = (LttbAlgorithm) algorithm;
                        final DirectLongList segments = list(lttb, "segments");
                        final DirectLongList targets = list(lttb, "targets");
                        final boolean hasScanned = segments != null && segments.size() == 2L * segmentCount;
                        return switch (phase) {
                            case GAP_SCAN -> !hasScanned;
                            case GAP_FLOOR -> hasScanned && targets == null;
                            case GAP_TARGETS -> targets != null && targets.size() < segmentCount;
                            case GAP_EMIT -> targets != null && targets.size() == segmentCount;
                            default -> false;
                        };
                    }
                }
            }
            return false;
        }
    }
}
