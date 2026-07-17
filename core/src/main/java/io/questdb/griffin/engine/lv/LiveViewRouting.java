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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * The predicates that decide whether a live-view read may serve the in-memory
 * tier, shared by the two read paths: {@link LiveViewRecordCursor} (the
 * record-cursor seam split) and {@link LiveViewRecordCursorFactory}'s
 * page-frame path (which binds a {@link LiveViewPageFrameCursor}). Both paths
 * must agree row-for-row on what they serve, so they must agree on when they
 * serve it - a fence that drifts between them is a wrong-results bug, not a
 * staleness one.
 * <p>
 * The routing decision has three parts, and the two paths differ only in how
 * they reach the disk-side scan:
 * <ul>
 *   <li>the projection resolves against the tier's columns
 *   ({@link #buildTierColumnMapping});</li>
 *   <li>the disk scan is a table scan that exposes an LV-table seqTxn and either applies no
 *   filter of its own or applies one the read can reproduce over the slot
 *   ({@link #diskReaderSeqTxn}, {@link #diskIntervals});</li>
 *   <li>the slot holds rows and is stamped with that same seqTxn
 *   ({@link #isFenced}).</li>
 * </ul>
 */
final class LiveViewRouting {

    private LiveViewRouting() {
    }

    /**
     * Resolves each projected column to the tier column it reads, appending the result to
     * {@code tierColumnsOut} (output column {@code i} -> {@code tierColumnsOut[i]}), and
     * returns true when every column resolves.
     * <p>
     * The scan's {@link ColumnMapping} already carries that resolution: it maps output
     * column {@code i} to the LV-table storage column the scan reads, and the tier stores
     * the LV table's columns in declared order, so {@code mapping.getColumnIndex(i)} IS
     * the tier column. The type comparison guards the premise that the two column spaces
     * agree - a mismatch means the tier was shaped from a different schema than the read
     * sees, so fail safe to disk-only rather than serve another column's bytes.
     * <p>
     * Leaves {@code tierColumnsOut} empty on false.
     */
    static boolean buildTierColumnMapping(
            @Nullable PageFrameCursor frameCursor,
            RecordMetadata baseMetadata,
            LiveViewInMemoryBuffer buffer,
            IntList tierColumnsOut
    ) {
        if (frameCursor == null) {
            return false;
        }
        final ColumnMapping mapping = frameCursor.getColumnMapping();
        final int columnCount = baseMetadata.getColumnCount();
        if (mapping == null || mapping.getColumnCount() != columnCount) {
            return false;
        }
        final int tierColumnCount = buffer.columnCount();
        for (int i = 0; i < columnCount; i++) {
            final int tierColumn = mapping.getColumnIndex(i);
            if (tierColumn < 0
                    || tierColumn >= tierColumnCount
                    || buffer.columnType(tierColumn) != baseMetadata.getColumnType(i)) {
                tierColumnsOut.clear();
                return false;
            }
            tierColumnsOut.add(tierColumn);
        }
        return true;
    }

    /**
     * The intervals a routed read must apply to the slot's band, or null when it must apply
     * none. Only meaningful for a cursor {@link #diskReaderSeqTxn} did not answer
     * {@code LONG_NULL} for, which is what establishes that a reported interval filter also
     * describes itself.
     *
     * @see LiveViewIntervalBands
     */
    static @Nullable LongList diskIntervals(@Nullable PageFrameCursor frameCursor) {
        return frameCursor instanceof TablePageFrameCursor tpfc && tpfc.hasIntervalFilter()
                ? tpfc.getIntervals()
                : null;
    }

    /**
     * Returns the disk scan's LV-table seqTxn, or {@code LONG_NULL} when the scan is not a
     * table-reader scan we can fence cheaply and reproduce over the slot.
     * <p>
     * Routing assumes the disk side yields exactly the LV-table rows the slot's band is cut
     * against, so any scan shape that under-returns rows relative to that band must
     * disengage the fence (fail safe to disk-only). Two shapes do:
     * <ul>
     *   <li>an <b>interval filter</b> - a WHERE on the designated timestamp, which the
     *   optimiser pushes into the scan. The disk side then yields only the rows inside the
     *   intervals, so an unfiltered slot band would over-return. This is admissible only
     *   because the cursor also hands back the intervals themselves
     *   ({@link #diskIntervals}), which lets the read cut the slot's band by the same
     *   filter; a cursor reporting an interval filter it cannot describe stays disk-only.
     *   Note this permits the interval but does NOT permit the seam: the seam's cut takes
     *   the disk scan's trailing {@code leadStart} rows, an identity an interval breaks by
     *   narrowing the scan beneath the wrapper. Both paths route an interval-filtered read
     *   lead-only.</li>
     *   <li>an <b>active parquet pushdown filter</b> - row-group pruning drops rows the
     *   frames still count, so the scan under-returns with nothing to reproduce the pruning
     *   from.</li>
     * </ul>
     * A non-table cursor (a synthetic or external frame source) carries no seqTxn at all.
     */
    static long diskReaderSeqTxn(@Nullable PageFrameCursor frameCursor) {
        if (frameCursor instanceof TablePageFrameCursor tpfc
                && !tpfc.hasActivePushdownFilter()
                && (!tpfc.hasIntervalFilter() || tpfc.getIntervals() != null)) {
            final TableReader reader = tpfc.getTableReader();
            if (reader != null) {
                return reader.getSeqTxn();
            }
        }
        return Numbers.LONG_NULL;
    }

    /**
     * The consistency fence: true when the slot holds rows and is stamped with the same
     * LV-table version as the disk snapshot. Equal seqTxns mean the slot's overlap band and
     * the disk snapshot reflect the identical LV-table version, so that band agrees
     * row-for-row and the tier may lead disk by its un-flushed lead.
     */
    static boolean isFenced(LiveViewInMemoryBuffer slot, long diskSeqTxn) {
        final long slotSeqTxn = slot.lvSeqTxn();
        return slot.rowCount() > 0 && slotSeqTxn != Numbers.LONG_NULL && slotSeqTxn == diskSeqTxn;
    }

    /**
     * Reports whether the slot is stamped with an LV-table seqTxn strictly NEWER than the
     * disk snapshot - i.e. the disk reader was opened before the flush that produced the
     * slot. Serving such a read would disengage the fence and route disk-only against a
     * STALE, smaller disk snapshot: a live view would then appear to shrink relative to an
     * earlier read that already reflected the flush's rows. Both read paths re-open the
     * disk side against a fresh snapshot while this holds; the slot's flush is already
     * applied (the flush stamps the slot only after applyWalDirect), so a re-opened reader
     * observes at least the slot's seqTxn and the retry converges.
     */
    static boolean isSlotNewerThanDisk(@Nullable LiveViewInMemoryBuffer slot, long diskSeqTxn) {
        if (slot == null || diskSeqTxn == Numbers.LONG_NULL) {
            return false;
        }
        final long slotSeqTxn = slot.lvSeqTxn();
        return slotSeqTxn != Numbers.LONG_NULL && slotSeqTxn > diskSeqTxn;
    }
}
