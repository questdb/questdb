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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

/**
 * One fuzz run's draw of the live view checkpoint and repair configuration: the cadence, the
 * cost and budget knobs the repair route decision reads, and the switch of every route. At
 * their defaults a fuzz-sized run never prices a keyed scan cheaper and never parks a
 * replay, so the keyed routes, segment yield and sparse publication go unreached. The draws
 * here are the ones the targeted route tests use to get there.
 * <p>
 * A draw is plain fields, so a sub-arm that steers toward one route overrides the fields it
 * needs before {@link #apply}. A field at {@link #DEFAULT} resets its property to the
 * configured default.
 * <p>
 * The refresh turn's duration budget stays at its default: the repair's budget check reads the
 * microsecond clock, which the tests hold still while a job drains, so a duration budget never
 * trips inside a turn. {@link #replayMaxRows} is the deterministic budget lever.
 */
final class LiveViewFuzzKnobs {
    static final long DEFAULT = -1;
    boolean isAdaptiveCadence;
    boolean isIsolatedRuntime;
    boolean isKeyedReplay;
    boolean isOpenSegmentKeyedReplay;
    boolean isPerSegment;
    boolean isSegmentYield;
    boolean isSparsePublication;
    long checkpointMaxDurationMicros;
    long checkpointRows;
    long compactStalePercent;
    long compactThreshold;
    long indexOpenRows;
    long maxChainedBoundaries;
    long replayMaxRows;
    long scanMaxKeys;
    long scanMaxRows;
    long turnMaxCommits;

    /**
     * Draws every knob. Each route switch is off in one run of five, so a batch reaches both
     * the route and the fallback it would otherwise take.
     */
    static LiveViewFuzzKnobs randomize(Rnd rnd) {
        final LiveViewFuzzKnobs knobs = new LiveViewFuzzKnobs();
        // Mostly a seal per pass or two. One run in five seals on rows rarely, so the duration
        // cadence below, when drawn, is what seals.
        knobs.checkpointRows = rnd.nextInt(5) == 0 ? 32 + rnd.nextInt(64) : 1 + rnd.nextInt(4);
        // A refresh pass moves the clock 250ms, so these seal every one to four passes.
        knobs.checkpointMaxDurationMicros = rnd.nextBoolean() ? DEFAULT : 250_000L * (1 + rnd.nextInt(4));
        knobs.indexOpenRows = rnd.nextBoolean() ? 1 : (rnd.nextBoolean() ? 4 : 256);
        knobs.replayMaxRows = switch (rnd.nextInt(3)) {
            case 0 -> 1;
            case 1 -> 1 + rnd.nextInt(16);
            default -> 1_000_000;
        };
        knobs.maxChainedBoundaries = switch (rnd.nextInt(3)) {
            case 0 -> 0;
            case 1 -> 1 + rnd.nextInt(3);
            default -> 256;
        };
        knobs.turnMaxCommits = rnd.nextBoolean() ? 1 : 64;
        // A budget of 0 is no budget at all, so the tight draws are 1 and a few.
        knobs.scanMaxKeys = switch (rnd.nextInt(4)) {
            case 0 -> 1;
            case 1 -> 2 + rnd.nextInt(4);
            default -> DEFAULT;
        };
        knobs.scanMaxRows = switch (rnd.nextInt(4)) {
            case 0 -> 1;
            case 1 -> 2 + rnd.nextInt(32);
            default -> DEFAULT;
        };
        // The partition map compacts only above the threshold, which a few keys never pass at
        // the default.
        knobs.compactThreshold = rnd.nextBoolean() ? DEFAULT : 1 + rnd.nextInt(4);
        knobs.compactStalePercent = rnd.nextBoolean() ? DEFAULT : rnd.nextInt(101);
        knobs.isPerSegment = rnd.nextInt(5) != 0;
        knobs.isKeyedReplay = rnd.nextInt(5) != 0;
        knobs.isOpenSegmentKeyedReplay = rnd.nextInt(5) != 0;
        knobs.isSegmentYield = rnd.nextInt(5) != 0;
        knobs.isSparsePublication = rnd.nextInt(5) != 0;
        knobs.isIsolatedRuntime = rnd.nextInt(5) != 0;
        knobs.isAdaptiveCadence = rnd.nextBoolean();
        return knobs;
    }

    void apply(PropertySetter setter) {
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, checkpointRows);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, checkpointMaxDurationMicros);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, indexOpenRows);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, replayMaxRows);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, maxChainedBoundaries);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_COMMITS, turnMaxCommits);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, scanMaxKeys);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_ROWS, scanMaxRows);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, compactThreshold);
        set(setter, PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_STALE_PERCENT, compactStalePercent);
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, String.valueOf(isPerSegment));
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, String.valueOf(isKeyedReplay));
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, String.valueOf(isOpenSegmentKeyedReplay));
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SEGMENT_YIELD_ENABLED, String.valueOf(isSegmentYield));
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, String.valueOf(isSparsePublication));
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, String.valueOf(isIsolatedRuntime));
        setter.set(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, String.valueOf(isAdaptiveCadence));
    }

    /**
     * Every draw, on one line, so a failing run reproduces from its log.
     */
    void toSink(StringSink sink) {
        sink.put("ckptRows=").put(checkpointRows)
                .put(", ckptDuration=").put(checkpointMaxDurationMicros)
                .put(", openRows=").put(indexOpenRows)
                .put(", replayMaxRows=").put(replayMaxRows)
                .put(", chained=").put(maxChainedBoundaries)
                .put(", turnCommits=").put(turnMaxCommits)
                .put(", scanMaxKeys=").put(scanMaxKeys)
                .put(", scanMaxRows=").put(scanMaxRows)
                .put(", compactThreshold=").put(compactThreshold)
                .put(", compactStale=").put(compactStalePercent)
                .put(", perSegment=").put(isPerSegment)
                .put(", keyed=").put(isKeyedReplay)
                .put(", openKeyed=").put(isOpenSegmentKeyedReplay)
                .put(", yield=").put(isSegmentYield)
                .put(", sparse=").put(isSparsePublication)
                .put(", isolated=").put(isIsolatedRuntime)
                .put(", adaptive=").put(isAdaptiveCadence);
    }

    private static void set(PropertySetter setter, PropertyKey key, long value) {
        setter.set(key, value == DEFAULT ? null : String.valueOf(value));
    }

    /**
     * {@code AbstractCairoTest.setProperty(PropertyKey, String)}, which a null value resets.
     */
    @FunctionalInterface
    interface PropertySetter {
        void set(PropertyKey key, String value);
    }
}
