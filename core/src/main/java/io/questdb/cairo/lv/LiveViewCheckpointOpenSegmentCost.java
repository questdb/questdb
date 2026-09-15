/*******************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.cairo.lv;

import io.questdb.std.Numbers;
import org.jetbrains.annotations.TestOnly;

/**
 * Per-view elapsed-time cost model for the two open-segment resume executors.
 * <p>
 * The whole-range executor has a setup term that row-only pricing cannot express: when
 * the selected checkpoint root is older than the primary runtime, it clears the live maps
 * and decodes the complete root before scanning the replay interval. The keyed executor
 * restores and transplants only the correction's key domain. This model puts both terms in
 * nanoseconds and keeps the original posting-list estimate as the keyed scan's work unit.
 * <p>
 * Samples are kept per view because state width and compiled window work are view-specific.
 * Warm whole-range scans (runtime root reused) and cold ones (root restored) are learned
 * separately. Failed, forced, and sparse-fallback repairs are rejected by the caller.
 */
public final class LiveViewCheckpointOpenSegmentCost {
    // Cold-start envelope. These are deliberately broad engineering priors, not a hardware
    // fit: successful repairs replace them with per-view measurements. The keyed upper bound
    // below adds another 50% before it may override the row-only verdict.
    private static final long COLD_KEY_STATE_NANOS_PER_KEY = 5_000;
    private static final long COLD_KEYED_NANOS_PER_COST_ROW = 250;
    private static final long COLD_RESTORE_NANOS_PER_BYTE = 6;
    private static final long COLD_WHOLE_NANOS_PER_ROW = 100;
    private static final int EMA_ALPHA_INV = 8;
    private static final int EMA_OUTLIER_MULTIPLIER = 5;
    private static final int KEYED_UPPER_BOUND_PERCENT = 150;
    // A restore-aware override needs a material win. The existing row verdict remains
    // authoritative without this margin, preserving its established small-range behavior.
    private static final int ROUTE_HYSTERESIS_PERCENT = 15;

    private long coldWholeScanNanos;
    private long coldWholeScanRows;
    private long keyStateKeys;
    private long keyStateNanos;
    private long keyedScanCostRows;
    private long keyedScanNanos;
    private long lastKeyedEstimateNanos = Numbers.LONG_NULL;
    private long lastWholeEstimateNanos = Numbers.LONG_NULL;
    private long restoreBytes;
    private long restoreNanos;
    private long warmWholeScanNanos;
    private long warmWholeScanRows;

    public long getLastKeyedEstimateNanos() {
        return lastKeyedEstimateNanos;
    }

    public long getLastWholeEstimateNanos() {
        return lastWholeEstimateNanos;
    }

    void reset() {
        coldWholeScanNanos = 0;
        coldWholeScanRows = 0;
        keyStateKeys = 0;
        keyStateNanos = 0;
        keyedScanCostRows = 0;
        keyedScanNanos = 0;
        lastKeyedEstimateNanos = Numbers.LONG_NULL;
        lastWholeEstimateNanos = Numbers.LONG_NULL;
        restoreBytes = 0;
        restoreNanos = 0;
        warmWholeScanNanos = 0;
        warmWholeScanRows = 0;
    }

    /**
     * Records a successful keyed repair. {@code scanCommitApplyNanos} is the route-specific
     * scan/window/WAL append, commit and apply work; {@code keyStatePhaseNanos} is the
     * key-scoped root restore plus transplant.
     */
    public void recordKeyed(
            long scanCommitApplyNanos,
            long scanCostRows,
            long keyStatePhaseNanos,
            long keyCount
    ) {
        if (scanCommitApplyNanos > 0 && scanCostRows > 0) {
            keyedScanNanos = foldEma(keyedScanNanos, scanCommitApplyNanos);
            keyedScanCostRows = foldEma(keyedScanCostRows, scanCostRows);
        }
        if (keyStatePhaseNanos > 0 && keyCount > 0) {
            keyStateNanos = foldEma(keyStateNanos, keyStatePhaseNanos);
            keyStateKeys = foldEma(keyStateKeys, keyCount);
        }
    }

    /**
     * Records a successful whole-range repair. Map clearing belongs to the restore setup:
     * both disappear when the runtime root is reusable, and both scale with retained state.
     */
    public void recordWhole(
            boolean runtimeAnchorReusable,
            long scanCommitApplyNanos,
            long wholeRangeRows,
            long mapClearAndRestoreNanos,
            long selectedRootLogicalBytes
    ) {
        if (scanCommitApplyNanos > 0 && wholeRangeRows > 0) {
            if (runtimeAnchorReusable) {
                warmWholeScanNanos = foldEma(warmWholeScanNanos, scanCommitApplyNanos);
                warmWholeScanRows = foldEma(warmWholeScanRows, wholeRangeRows);
            } else {
                coldWholeScanNanos = foldEma(coldWholeScanNanos, scanCommitApplyNanos);
                coldWholeScanRows = foldEma(coldWholeScanRows, wholeRangeRows);
            }
        }
        if (!runtimeAnchorReusable && mapClearAndRestoreNanos > 0 && selectedRootLogicalBytes > 0) {
            restoreNanos = foldEma(restoreNanos, mapClearAndRestoreNanos);
            restoreBytes = foldEma(restoreBytes, selectedRootLogicalBytes);
        }
    }

    /**
     * Whether restore setup makes the keyed route materially cheaper even though the
     * posting-row model preferred the whole range. Static eligibility is intentionally not
     * part of this method; the caller applies every correctness and configuration gate after
     * pricing, just as it does for the row verdict.
     */
    public boolean shouldOverrideWholeRange(
            boolean runtimeAnchorReusable,
            long selectedRootLogicalBytes,
            long wholeRangeRows,
            long keyedCostRows,
            long keyCount
    ) {
        lastWholeEstimateNanos = estimateWhole(
                runtimeAnchorReusable,
                selectedRootLogicalBytes,
                wholeRangeRows
        );
        lastKeyedEstimateNanos = estimateKeyed(keyedCostRows, keyCount);
        if (runtimeAnchorReusable || selectedRootLogicalBytes <= 0 || keyCount <= 0) {
            return false;
        }
        final long keyedUpperBound = percent(lastKeyedEstimateNanos, KEYED_UPPER_BOUND_PERCENT);
        final long wholeWithHysteresis = percent(lastWholeEstimateNanos, 100 - ROUTE_HYSTERESIS_PERCENT);
        return keyedUpperBound < wholeWithHysteresis;
    }

    @TestOnly
    public void setRatesForTest(
            long restoreSampleNanos,
            long restoreSampleBytes,
            long coldWholeSampleNanos,
            long coldWholeSampleRows,
            long warmWholeSampleNanos,
            long warmWholeSampleRows,
            long keyedSampleNanos,
            long keyedSampleCostRows,
            long keyStateSampleNanos,
            long keyStateSampleKeys
    ) {
        restoreNanos = Math.max(0, restoreSampleNanos);
        restoreBytes = Math.max(0, restoreSampleBytes);
        coldWholeScanNanos = Math.max(0, coldWholeSampleNanos);
        coldWholeScanRows = Math.max(0, coldWholeSampleRows);
        warmWholeScanNanos = Math.max(0, warmWholeSampleNanos);
        warmWholeScanRows = Math.max(0, warmWholeSampleRows);
        keyedScanNanos = Math.max(0, keyedSampleNanos);
        keyedScanCostRows = Math.max(0, keyedSampleCostRows);
        keyStateNanos = Math.max(0, keyStateSampleNanos);
        keyStateKeys = Math.max(0, keyStateSampleKeys);
    }

    private long estimateKeyed(long scanCostRows, long keyCount) {
        final long scan = keyedScanNanos > 0 && keyedScanCostRows > 0
                ? scale(keyedScanNanos, keyedScanCostRows, scanCostRows)
                : saturatedMultiply(scanCostRows, COLD_KEYED_NANOS_PER_COST_ROW);
        final long state = keyStateNanos > 0 && keyStateKeys > 0
                ? scale(keyStateNanos, keyStateKeys, keyCount)
                : saturatedMultiply(keyCount, COLD_KEY_STATE_NANOS_PER_KEY);
        return saturatedAdd(scan, state);
    }

    private long estimateWhole(boolean runtimeAnchorReusable, long logicalBytes, long rows) {
        final long sampleNanos = runtimeAnchorReusable ? warmWholeScanNanos : coldWholeScanNanos;
        final long sampleRows = runtimeAnchorReusable ? warmWholeScanRows : coldWholeScanRows;
        final long scan = sampleNanos > 0 && sampleRows > 0
                ? scale(sampleNanos, sampleRows, rows)
                : saturatedMultiply(rows, COLD_WHOLE_NANOS_PER_ROW);
        if (runtimeAnchorReusable) {
            return scan;
        }
        final long restore = restoreNanos > 0 && restoreBytes > 0
                ? scale(restoreNanos, restoreBytes, logicalBytes)
                : saturatedMultiply(logicalBytes, COLD_RESTORE_NANOS_PER_BYTE);
        return saturatedAdd(scan, restore);
    }

    private static long foldEma(long previous, long sample) {
        if (sample <= 0) {
            return previous;
        }
        if (previous == 0) {
            return sample;
        }
        final long cap = saturatedMultiply(previous, EMA_OUTLIER_MULTIPLIER);
        final long capped = Math.min(sample, cap);
        try {
            final long weighted = Math.multiplyExact(previous, EMA_ALPHA_INV - 1L);
            return Math.addExact(Math.addExact(weighted, capped), EMA_ALPHA_INV / 2L) / EMA_ALPHA_INV;
        } catch (ArithmeticException overflow) {
            return previous / 2 + capped / 2;
        }
    }

    private static long percent(long value, int percent) {
        if (value <= 0) {
            return 0;
        }
        return scale(value, 100, percent);
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private static long saturatedMultiply(long value, long multiplier) {
        if (value <= 0 || multiplier <= 0) {
            return 0;
        }
        return value > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : value * multiplier;
    }

    private static long scale(long sampleValue, long sampleUnits, long targetUnits) {
        if (sampleValue <= 0 || sampleUnits <= 0 || targetUnits <= 0) {
            return 0;
        }
        final double estimate = (double) sampleValue * (double) targetUnits / (double) sampleUnits;
        if (!Double.isFinite(estimate) || estimate >= Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return Math.max(1, (long) Math.ceil(estimate));
    }
}
