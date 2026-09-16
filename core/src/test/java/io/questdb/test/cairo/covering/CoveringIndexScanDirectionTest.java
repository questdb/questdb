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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the scan-direction declarations the per-key (unordered) covering scan rests on, at the
 * factory itself rather than through a query's answer or its plan.
 * <p>
 * <b>Why these need their own assertions.</b> Every one of the mechanisms below is invisible to
 * a result comparison: the merged and per-key modes return the SAME rows, so a scan that
 * wrongly kept the merge, an ordering opt-out that was wrongly refused, and a direction
 * declared too wide all leave every value assertion in the suite passing. They are also
 * invisible to a plan assertion, because the plan prints the plan-stable PERMISSION. Four
 * separate mutations of this machinery -- neutering the order-sensitivity backstop, deleting
 * {@code tryDisableTimestampOrdering}'s refusal, reducing
 * {@code AdaptiveSymbolPatternRecordCursorFactory.getPageFrameScanDirection()} to a bare
 * {@code SCAN_DIRECTION_FORWARD}, and deleting {@code getScanDirection()}'s per-key
 * {@code SCAN_DIRECTION_OTHER} branch -- each produced zero test failures across the suite.
 * <p>
 * <b>Both directions, deliberately.</b> Each declaration is asserted on a shape where it must
 * be {@code SCAN_DIRECTION_OTHER} AND on a shape where it must be
 * {@code SCAN_DIRECTION_FORWARD}. A one-sided assertion is caught by a mutation that widens the
 * answer but not by one that narrows it, or the reverse -- which is exactly the gap that let a
 * bare-{@code FORWARD} mutation survive 730 tests while the bare-{@code OTHER} one was caught
 * by four.
 */
public class CoveringIndexScanDirectionTest extends AbstractCoveringIndexQueryTest {

    /**
     * {@code getPageFrameScanDirection()} must survive every page-frame-transparent wrapper
     * between the group-by and the scan.
     * <p>
     * The narrow answer exists so a page-frame consumer is not refused on the strength of a
     * delegate that has no frames to give. It is declared as a {@code default} method
     * forwarding to {@code getScanDirection()}, so a wrapper that delegates only
     * {@code getScanDirection()} silently degrades it back to the wide answer -- and the
     * group-by never talks to a scan directly, it talks to a filter wrapping a projection
     * wrapping the scan. This asserts the answer the TOP of the chain gives equals the one the
     * covering factory at the bottom gives.
     */
    @Test
    public void testPageFrameScanDirectionSurvivesTheWrapperChain() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            // count() over a pattern filter puts an async filter BETWEEN the consumer and the
            // pattern factory, and the pattern factory is the one place where the two answers
            // genuinely differ: its index route drains key by key (so getScanDirection() is
            // OTHER) but has no page frames to give (so getPageFrameScanDirection() is FORWARD).
            // A wrapper that forwards only getScanDirection() reports OTHER for both and the
            // divergence is lost at the first hop.
            final String sql = "SELECT count() FROM pattern_tel WHERE sym LIKE 'A%'";
            try (RecordCursorFactory top = select(sql)) {
                final ObjList<RecordCursorFactory> chain = chainOf(top);
                final int patternIdx = indexOf(chain, AdaptiveSymbolPatternRecordCursorFactory.class);
                Assert.assertTrue(
                        "no adaptive symbol-pattern factory under " + top.getClass().getSimpleName()
                                + " -- the query stopped routing through it, so this test no longer"
                                + " exercises what it claims",
                        patternIdx >= 0
                );
                final RecordCursorFactory pattern = chain.getQuick(patternIdx);
                Assert.assertEquals(
                        "the pattern factory's two answers no longer differ on this shape, so a"
                                + " wrapper could drop the narrow one without this test noticing."
                                + " Find a shape where they do before weakening this.",
                        RecordCursorFactory.SCAN_DIRECTION_OTHER,
                        pattern.getScanDirection()
                );
                Assert.assertEquals(
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        pattern.getPageFrameScanDirection()
                );

                // Every ancestor that forwards getScanDirection() must forward the narrow answer
                // too. Stop at the first that states a direction of its own.
                int wrappers = 0;
                for (int i = patternIdx - 1; i >= 0; i--) {
                    final RecordCursorFactory wrapper = chain.getQuick(i);
                    if (wrapper.getScanDirection() != pattern.getScanDirection()) {
                        break;
                    }
                    wrappers++;
                    Assert.assertEquals(
                            wrapper.getClass().getSimpleName() + " forwards getScanDirection() to its"
                                    + " base but not getPageFrameScanDirection(). The latter is a"
                                    + " default method forwarding to the former, so the narrow answer"
                                    + " degrades back to the wide one at this hop -- and the wide one"
                                    + " describes a delegate a page-frame consumer can never be"
                                    + " served by.",
                            pattern.getPageFrameScanDirection(),
                            wrapper.getPageFrameScanDirection()
                    );
                }
                Assert.assertTrue(
                        "no forwarding wrapper sits above the pattern factory on this shape, so the"
                                + " loop above asserted nothing",
                        wrappers > 0
                );
            }
        });
    }

    /**
     * The adaptive symbol-pattern factory's narrow answer on the shape it exists for: the index
     * route drains key by key and is unordered, but it has no page frames, so a page-frame
     * consumer must be told FORWARD. A mutation reducing the override to a bare
     * {@code SCAN_DIRECTION_OTHER} fails here;
     * {@link #testPatternFactoryDeclaresOtherWhenAPageFrameDelegateIsNotForward()} is the arm a
     * bare {@code SCAN_DIRECTION_FORWARD} fails.
     */
    @Test
    public void testPatternFactoryDeclaresForwardWhenOnlyTheIndexRouteIsUnordered() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            final String sql = "SELECT array_agg(price) FROM pattern_tel WHERE sym LIKE 'A%'";
            try (RecordCursorFactory top = select(sql)) {
                final AdaptiveSymbolPatternRecordCursorFactory pattern =
                        findBase(top, AdaptiveSymbolPatternRecordCursorFactory.class);
                Assert.assertNotNull("no adaptive symbol-pattern factory under " + top.getClass().getSimpleName(), pattern);
                Assert.assertEquals(
                        "the shape stopped exercising the divergence: the record-cursor answer is no"
                                + " longer OTHER, so the assertion below would pass on a factory that"
                                + " had no narrow answer to give",
                        RecordCursorFactory.SCAN_DIRECTION_OTHER,
                        pattern.getScanDirection()
                );
                Assert.assertEquals(
                        "the pattern factory declared its page frames unordered on the strength of"
                                + " its INDEX route, which has no page frames and can therefore never"
                                + " be the delegate that serves a page-frame consumer. That is the"
                                + " false positive this override exists to remove: it refuses"
                                + " first()/last()/array_agg() over a pattern filter on a"
                                + " POSTING-indexed symbol.",
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        pattern.getPageFrameScanDirection()
                );
            }
        });
    }

    /**
     * The other direction, which nothing covered: when a delegate a page-frame consumer CAN be
     * served by is not forward, the narrow answer must say so. A negative-limit descending scan
     * gives the pattern factory a backward scan delegate. A mutation reducing the override to a
     * bare {@code SCAN_DIRECTION_FORWARD} fails only here.
     */
    @Test
    public void testPatternFactoryDeclaresOtherWhenAPageFrameDelegateIsNotForward() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolPatternTable();
            final String sql = "SELECT sym, price, ts FROM pattern_tel WHERE sym LIKE 'A%' ORDER BY ts DESC LIMIT 5";
            try (RecordCursorFactory top = select(sql)) {
                final AdaptiveSymbolPatternRecordCursorFactory pattern =
                        findBase(top, AdaptiveSymbolPatternRecordCursorFactory.class);
                Assert.assertNotNull("no adaptive symbol-pattern factory under " + top.getClass().getSimpleName(), pattern);
                Assert.assertEquals(
                        "the pattern factory declared forward page frames over a BACKWARD scan"
                                + " delegate -- a delegate a page-frame consumer really can be served"
                                + " by. A consumer that elides an ORDER BY ts on the strength of this"
                                + " returns the rows reversed.",
                        RecordCursorFactory.SCAN_DIRECTION_OTHER,
                        pattern.getPageFrameScanDirection()
                );
            }
        });
    }

    /**
     * The covering scan must keep advertising forward order when nothing released it from the
     * guarantee. This is the arm a mutation deleting the per-key branch from
     * {@code getScanDirection()} passes; its twin below is the one it fails.
     */
    @Test
    public void testScanDeclaresForwardWhenOrderingWasNotReleased() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryMultiPartition();
            // A time-bucket grouping draws each bucket from several keys, so the opt-out must be
            // declined and the k-way merge kept -- and the scan stays timestamp-ascending.
            final String sql = "SELECT ts, first(value) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC') SAMPLE BY 1h";
            try (RecordCursorFactory top = select(sql)) {
                final CoveringIndexRecordCursorFactory covering =
                        findBase(top, CoveringIndexRecordCursorFactory.class);
                Assert.assertNotNull("no covering scan under " + top.getClass().getSimpleName(), covering);
                Assert.assertEquals(
                        "the covering scan stopped advertising timestamp order over a grouping that"
                                + " was never offered the opt-out, or tryDisableTimestampOrdering"
                                + " granted the permission to a consumer that cannot use it."
                                + " Advertising OTHER here costs every ORDER BY ts elision and"
                                + " SAMPLE BY fast path over this scan.",
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        covering.getScanDirection()
                );
            }
        });
    }

    /**
     * A multi-key covering latestBy merges nothing and emits one row per key in KEY order, so
     * it must refuse the ordering opt-out outright rather than be granted a permission it
     * cannot honour. This is the arm that catches deleting
     * {@code tryDisableTimestampOrdering}'s {@code latestBy || multiKeyPageFrameCursor == null}
     * refusal.
     */
    @Test
    public void testScanDeclaresOtherForMultiKeyLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryMultiPartition();
            final String sql = "SELECT param_id, value FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC') LATEST ON ts PARTITION BY param_id";
            try (RecordCursorFactory top = select(sql)) {
                final CoveringIndexRecordCursorFactory covering =
                        findBase(top, CoveringIndexRecordCursorFactory.class);
                Assert.assertNotNull("no covering scan under " + top.getClass().getSimpleName(), covering);
                Assert.assertEquals(
                        "a multi-key covering latestBy advertised timestamp-ascending frames. It"
                                + " emits one row per key in KEY order, which is not timestamp order,"
                                + " so a SAMPLE BY or an ORDER BY ts elision built on this"
                                + " declaration silently reads the wrong rows.",
                        RecordCursorFactory.SCAN_DIRECTION_OTHER,
                        covering.getScanDirection()
                );
            }
        });
    }

    /**
     * Grouping by exactly the index column releases the covering scan from designated-timestamp
     * order, and the scan must SAY so. Nothing else can: the plan prints the plan-stable
     * permission, and merged and per-key return the same rows.
     */
    @Test
    public void testScanDeclaresOtherWhenGroupedByTheIndexKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetryMultiPartition();
            final String sql = "SELECT param_id, first(value) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC') ORDER BY param_id";
            try (RecordCursorFactory top = select(sql)) {
                final CoveringIndexRecordCursorFactory covering =
                        findBase(top, CoveringIndexRecordCursorFactory.class);
                Assert.assertNotNull("no covering scan under " + top.getClass().getSimpleName(), covering);
                Assert.assertEquals(
                        "the covering scan advertised timestamp-ascending frames after accepting an"
                                + " ordering opt-out. Per-key mode emits one key's posting list per"
                                + " frame, so the stream is NOT timestamp-ascending, and a consumer"
                                + " that elides an ORDER BY ts on the strength of this declaration"
                                + " returns rows in the wrong order.",
                        RecordCursorFactory.SCAN_DIRECTION_OTHER,
                        covering.getScanDirection()
                );
            }
        });
    }

    /**
     * The {@code getBaseFactory()} chain, outermost first. Bounded so a factory that returns
     * itself as its base fails the test rather than hanging it.
     */
    private static ObjList<RecordCursorFactory> chainOf(RecordCursorFactory factory) {
        final ObjList<RecordCursorFactory> chain = new ObjList<>();
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 64; i++) {
            chain.add(f);
            f = f.getBaseFactory();
        }
        return chain;
    }

    /**
     * Walk the {@code getBaseFactory()} chain for the first factory of the given type.
     * Returns null when the chain holds none, which every caller asserts against rather than
     * dereferencing -- a query that stopped routing through the covering index would otherwise
     * make its test pass by proving nothing.
     */
    private static <T> T findBase(RecordCursorFactory factory, Class<T> type) {
        final int idx = indexOf(chainOf(factory), type);
        return idx < 0 ? null : type.cast(chainOf(factory).getQuick(idx));
    }

    private static int indexOf(ObjList<RecordCursorFactory> chain, Class<?> type) {
        for (int i = 0, n = chain.size(); i < n; i++) {
            if (type.isInstance(chain.getQuick(i))) {
                return i;
            }
        }
        return -1;
    }

    private void createSymbolPatternTable() throws Exception {
        execute("CREATE TABLE pattern_tel (" +
                "  sym SYMBOL INDEX TYPE POSTING INCLUDE (price)," +
                "  price DOUBLE," +
                "  ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO pattern_tel VALUES ('AA', 1.0, 0), ('AB', 2.0, 1)");
        execute("INSERT INTO pattern_tel SELECT 'BA', x::DOUBLE, timestamp_sequence(2, 1) FROM long_sequence(1000)");
    }
}
