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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.std.LongList;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

/**
 * Cuts a row band of the in-memory tier's slot down to the rows an interval filter admits,
 * expressing the answer as row sub-bands both live-view read paths can walk.
 * <p>
 * This is what lets an interval-filtered read route. When the optimiser pushes an interval
 * into the LV table's scan (a {@code WHERE} on the designated timestamp), the disk side
 * yields only the rows inside those intervals. The slot has no filter of its own, so
 * serving its band whole next to that scan emits rows the query excluded - wrong results,
 * not stale ones. Applying the SAME intervals to the slot's band restores the union: disk
 * holds every applied row in the intervals and the lead holds exactly the rows in the
 * intervals that disk lacks.
 * <p>
 * <b>Why a row band and not a predicate per row.</b> Both paths need the answer in row
 * space - the page-frame path publishes frames, which are row ranges - and both must agree
 * row-for-row on what they serve, so they share one definition rather than each testing
 * rows their own way. The slot's timestamps ascend (a live view's output is its base's
 * designated timestamp, which {@code CairoEngine.validateLiveViewTimestamp} enforces at
 * CREATE), so each interval maps to one contiguous row range found by binary search.
 * <p>
 * The bands come back ascending, disjoint and non-empty, as flat {@code (lo, hi)} pairs
 * with {@code hi} EXCLUSIVE - the row-range convention a page frame already uses. Note this
 * is the opposite end convention to the intervals themselves, which are closed at both
 * ends; {@link #cut} is where the two meet.
 */
public final class LiveViewIntervalBands {

    private LiveViewIntervalBands() {
    }

    /**
     * The number of rows the bands cover.
     */
    public static long countRows(LongList bands) {
        long rows = 0;
        for (int i = 0, n = bands.size(); i < n; i += 2) {
            rows += bands.getQuick(i + 1) - bands.getQuick(i);
        }
        return rows;
    }

    /**
     * Cuts {@code [bandLo, bandHi)} of {@code slot} down to the rows whose designated
     * timestamp falls inside {@code intervals}, appending the surviving sub-bands to
     * {@code bandsOut}.
     * <p>
     * A null {@code intervals} means no interval filter, so the band survives whole - the
     * shape every non-interval read takes, and the reason both paths can walk bands
     * unconditionally instead of forking on whether a filter is present. An EMPTY
     * {@code intervals} is the opposite and is NOT the same thing: the filter admits
     * nothing, so no band survives.
     *
     * @param slot      the pinned slot, whose timestamps ascend over the band
     * @param tsColumn  the slot's designated timestamp column
     * @param bandLo    first row of the band to cut, inclusive
     * @param bandHi    last row of the band to cut, exclusive
     * @param intervals flat closed (lo, hi) timestamp pairs, ascending and disjoint, or null
     * @param bandsOut  receives flat half-open (lo, hi) row pairs; appended to, not cleared
     */
    public static void cut(
            LiveViewInMemoryBuffer slot,
            int tsColumn,
            long bandLo,
            long bandHi,
            @Nullable LongList intervals,
            LongList bandsOut
    ) {
        if (bandLo >= bandHi) {
            return;
        }
        if (intervals == null) {
            bandsOut.add(bandLo, bandHi);
            return;
        }
        final long tsAddress = slot.dataAddress(tsColumn);
        // The intervals ascend and so does the band, so each interval's rows start at or
        // above the previous interval's last row: carrying searchLo forward keeps the whole
        // cut linear in the band rather than re-searching it per interval. Same reason the
        // native interval cursor carries partitionLimit across its own intervals.
        long searchLo = bandLo;
        for (int i = 0, n = intervals.size(); i < n; i += 2) {
            final long intervalLo = intervals.getQuick(i);
            final long intervalHi = intervals.getQuick(i + 1);
            if (searchLo >= bandHi) {
                // The band is spent; every later interval sits above its last row.
                break;
            }
            // First row at or above intervalLo. findTs takes the last row at or below its
            // argument, so probe intervalLo - 1 - except at Long.MIN_VALUE, an open lower
            // bound (WHERE ts < x) that would wrap. The whole remaining band is then in.
            final long lo = intervalLo == Long.MIN_VALUE
                    ? searchLo
                    : findTs(tsAddress, intervalLo - 1, searchLo, bandHi - 1) + 1;
            if (lo >= bandHi) {
                // This interval is above every remaining row, and so is every later one.
                break;
            }
            // First row above intervalHi. The interval is CLOSED, so a row at exactly
            // intervalHi is IN and the search probes intervalHi itself.
            final long hi = findTs(tsAddress, intervalHi, lo, bandHi - 1) + 1;
            if (lo < hi) {
                bandsOut.add(lo, hi);
            }
            // hi >= lo >= searchLo (findTs never answers below its own rowLo - 1), so this
            // only ever moves the floor up.
            searchLo = hi;
        }
    }

    /**
     * The index of the last row at or below {@code value} within {@code [rowLo, rowHi]},
     * or {@code rowLo - 1} when every row is above it. Mirrors
     * {@code NativeTimestampFinder.findTimestamp}, which is how the native interval cursor
     * turns an interval bound into a row - the slot's timestamp column is a contiguous
     * native vector of 64-bit values exactly like a partition's.
     */
    private static long findTs(long tsAddress, long value, long rowLo, long rowHi) {
        final long idx = Vect.binarySearch64Bit(tsAddress, value, rowLo, rowHi, Vect.BIN_SEARCH_SCAN_DOWN);
        return idx < 0 ? -idx - 2 : idx;
    }
}
