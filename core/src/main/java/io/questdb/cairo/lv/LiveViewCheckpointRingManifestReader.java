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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.file.BlockFileUtils.BLOCK_HEADER_SIZE;

/**
 * Reader-side view of the {@code _checkpoints/_ring} manifest block.
 * <p>
 * A populated reader is a <em>candidate</em>, not a trusted ring: it has passed
 * structural validation only. The trust decision - {@code coveredBaseSeqTxn}
 * against the reconciled applied floor - belongs to the refresh worker, which
 * is the only place the reconciled floor exists. The startup sweep runs before
 * any worker and can see only the raw, possibly stale {@code _lv.s}.
 * <p>
 * Validation rejects the manifest as a whole, never entry by entry: a partial
 * ring is a claim nothing on disk backs. Rejection surfaces as
 * {@link CairoException#LV_CHECKPOINT_RING_MANIFEST_INVALID}, distinct from the
 * version-mismatch codes that signal a compatibility break in <em>required</em>
 * state. The ring is derived, so every rejection costs a boundary rebuild and
 * never invalidates the view; the exception message carries the reason for the
 * fallback log line.
 * <p>
 * Validation is structural and cheap by design: it opens no {@code .cp} files.
 * CRCing every listed checkpoint would cost the full retention byte budget per
 * view on the startup thread, to validate state that only an O3 needs. A listed
 * checkpoint that turns out corrupt is handled lazily at use time, which evicts
 * that one entry without disturbing its neighbours. Nor does it enforce the
 * configured retention count / byte budget: the codec has no configuration, and
 * rehydration runs the ring through the same prune the flush cycle uses, so a
 * lowered budget trims the recovered ring instead of discarding it.
 */
public class LiveViewCheckpointRingManifestReader implements Mutable {
    // Packed ring records, ENTRY_SIZE longs each, oldest first - the layout
    // LiveViewInstance's ring holds, so rehydration is a straight copy.
    private final LongList entries = new LongList();
    private long coveredBaseSeqTxn = Numbers.LONG_NULL;
    private long generation = Numbers.LONG_NULL;

    @Override
    public void clear() {
        coveredBaseSeqTxn = Numbers.LONG_NULL;
        generation = Numbers.LONG_NULL;
        entries.clear();
    }

    /**
     * Base seqTxn at which every listed entry is proven sealed. Trust the ring
     * iff this equals the reconciled applied floor.
     */
    public long getCoveredBaseSeqTxn() {
        return coveredBaseSeqTxn;
    }

    /**
     * The packed ring, {@link LiveViewCheckpointRingManifest#ENTRY_SIZE} longs
     * per record, oldest first. Owned by this reader - copy before retaining.
     */
    public LongList getEntries() {
        return entries;
    }

    public long getEntryBaseSeqTxn(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_BASE_SEQ_TXN);
    }

    public int getEntryCount() {
        return entries.size() / LiveViewCheckpointRingManifest.ENTRY_SIZE;
    }

    public long getEntryLvRowsTotal(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_LV_ROWS_TOTAL);
    }

    public long getEntryLvSeqTxn(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_LV_SEQ_TXN);
    }

    public long getEntryMaxTs(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_MAX_TS);
    }

    public long getEntryStateBytes(int index) {
        return entries.getQuick(index * LiveViewCheckpointRingManifest.ENTRY_SIZE + LiveViewCheckpointRingManifest.ENTRY_STATE_BYTES);
    }

    /**
     * Publication counter of the manifest on disk. Diagnostic only.
     */
    public long getGeneration() {
        return generation;
    }

    /**
     * Populates this reader from {@code _ring} block file data, leaving it
     * cleared if the manifest does not validate.
     *
     * @throws CairoException with {@link CairoException#LV_CHECKPOINT_RING_MANIFEST_INVALID}
     *                        when the block is absent, truncated, version-skewed
     *                        or violates an entry invariant. The block file layer
     *                        throws its own critical exception on a checksum
     *                        mismatch or torn region - there is no automatic
     *                        fallback to the prior region. The caller treats both
     *                        the same way: log, fall back to the highest
     *                        {@code .cp}, carry on.
     */
    public LiveViewCheckpointRingManifestReader of(
            @NotNull BlockFileReader reader,
            @NotNull TableToken liveViewToken
    ) {
        clear();
        try {
            return parse(reader, liveViewToken);
        } catch (Throwable th) {
            // Never hand back a half-populated candidate: the block file layer
            // throws on a checksum mismatch or torn region, and validation
            // throws part-way through a payload. Either way the reader must
            // read as "no candidate", which is what the fallback expects.
            clear();
            throw th;
        }
    }

    private CairoException invalid(@NotNull TableToken liveViewToken) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_RING_MANIFEST_INVALID)
                .put("live view checkpoint ring manifest invalid [view=")
                .put(liveViewToken.getTableName())
                .put(", ");
    }

    private LiveViewCheckpointRingManifestReader parse(
            @NotNull BlockFileReader reader,
            @NotNull TableToken liveViewToken
    ) {
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() != LiveViewCheckpointRingManifest.RING_MANIFEST_BLOCK_TYPE) {
                continue;
            }
            final long payloadLength = block.length() - BLOCK_HEADER_SIZE;
            // Bounds-check ahead of every read: a ReadableBlock getter is a raw
            // offset into the mapped region, so a truncated payload must be
            // caught here rather than by reading the neighbouring block's bytes
            // as our own.
            if (payloadLength < LiveViewCheckpointRingManifest.RING_MANIFEST_HEADER_SIZE) {
                throw invalid(liveViewToken)
                        .put("manifest block too short [expected=")
                        .put(LiveViewCheckpointRingManifest.RING_MANIFEST_HEADER_SIZE)
                        .put(", actual=").put(payloadLength)
                        .put(']');
            }
            long offset = 0;
            final int onDiskVersion = block.getInt(offset);
            if (onDiskVersion > LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION) {
                throw invalid(liveViewToken)
                        .put("manifest format version not supported [onDiskVersion=")
                        .put(onDiskVersion)
                        .put(", supportedVersion=").put(LiveViewCheckpointRingManifest.RING_MANIFEST_FORMAT_VERSION)
                        .put(']');
            }
            offset += Integer.BYTES;
            generation = block.getLong(offset);
            offset += Long.BYTES;
            coveredBaseSeqTxn = block.getLong(offset);
            offset += Long.BYTES;
            final int entryCount = block.getInt(offset);
            offset += Integer.BYTES;
            final long expectedLength = LiveViewCheckpointRingManifest.RING_MANIFEST_HEADER_SIZE
                    + (long) entryCount * LiveViewCheckpointRingManifest.ENTRY_SIZE * Long.BYTES;
            // Catches a negative or absurd count as well as a truncated tail:
            // the arithmetic runs in long, so no count can wrap into a length
            // the payload happens to match.
            if (entryCount < 0 || expectedLength != payloadLength) {
                throw invalid(liveViewToken)
                        .put("manifest entry count does not match block length [entryCount=")
                        .put(entryCount)
                        .put(", expectedLength=").put(expectedLength)
                        .put(", actualLength=").put(payloadLength)
                        .put(']');
            }
            for (int i = 0, n = entryCount * LiveViewCheckpointRingManifest.ENTRY_SIZE; i < n; i++) {
                entries.add(block.getLong(offset));
                offset += Long.BYTES;
            }
            validateEntries(liveViewToken);
            return this;
        }
        throw invalid(liveViewToken).put("manifest block not found");
    }

    /**
     * Enforces the entry invariants the anchor search and the trust rule lean
     * on. Strictly increasing {@code lvSeqTxn} / {@code maxTs} also rules out
     * duplicates, and the {@code coveredBaseSeqTxn} bounds catch a manifest
     * claiming an entry is sealed at a position the entry itself sits above.
     */
    private void validateEntries(@NotNull TableToken liveViewToken) {
        long priorBaseSeqTxn = Long.MIN_VALUE;
        long priorLvSeqTxn = Long.MIN_VALUE;
        long priorMaxTs = Long.MIN_VALUE;
        long totalStateBytes = 0;
        for (int i = 0, n = getEntryCount(); i < n; i++) {
            final long lvSeqTxn = getEntryLvSeqTxn(i);
            final long maxTs = getEntryMaxTs(i);
            final long baseSeqTxn = getEntryBaseSeqTxn(i);
            final long stateBytes = getEntryStateBytes(i);
            if (i > 0 && lvSeqTxn <= priorLvSeqTxn) {
                throw rejectEntry(liveViewToken, i, "lvSeqTxn not strictly increasing", lvSeqTxn, priorLvSeqTxn);
            }
            if (i > 0 && maxTs <= priorMaxTs) {
                throw rejectEntry(liveViewToken, i, "maxTs not strictly increasing", maxTs, priorMaxTs);
            }
            if (i > 0 && baseSeqTxn < priorBaseSeqTxn) {
                throw rejectEntry(liveViewToken, i, "baseSeqTxn decreasing", baseSeqTxn, priorBaseSeqTxn);
            }
            if (lvSeqTxn > coveredBaseSeqTxn) {
                throw rejectEntry(liveViewToken, i, "lvSeqTxn above coveredBaseSeqTxn", lvSeqTxn, coveredBaseSeqTxn);
            }
            if (baseSeqTxn > coveredBaseSeqTxn) {
                throw rejectEntry(liveViewToken, i, "baseSeqTxn above coveredBaseSeqTxn", baseSeqTxn, coveredBaseSeqTxn);
            }
            // A negative stateBytes is meaningless on its own and would also
            // let the byte-budget prune under-count; the overflow guard below
            // relies on the sum being monotone.
            if (stateBytes < 0) {
                throw rejectEntry(liveViewToken, i, "negative stateBytes", stateBytes, totalStateBytes);
            }
            if (totalStateBytes + stateBytes < totalStateBytes) {
                throw rejectEntry(liveViewToken, i, "stateBytes sum overflow", stateBytes, totalStateBytes);
            }
            totalStateBytes += stateBytes;
            priorBaseSeqTxn = baseSeqTxn;
            priorLvSeqTxn = lvSeqTxn;
            priorMaxTs = maxTs;
        }
    }

    private CairoException rejectEntry(
            @NotNull TableToken liveViewToken,
            int index,
            @NotNull String reason,
            long value,
            long comparedTo
    ) {
        return invalid(liveViewToken)
                .put(reason)
                .put(" [index=").put(index)
                .put(", value=").put(value)
                .put(", comparedTo=").put(comparedTo)
                .put(']');
    }
}
