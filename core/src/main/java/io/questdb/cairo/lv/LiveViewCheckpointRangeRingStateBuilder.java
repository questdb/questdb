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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Copy-on-write chunk builder for the partitioned bounded-RANGE value ring.
 * Sealed chunks are reused by reference, expiration advances a logical offset
 * into the shared head, and only the rows this boundary appended are encoded.
 * <p>
 * The value column holds zero, one, two or four 64-bit words per row, which the
 * ring's value kind selects: a DOUBLE ring stores exact IEEE-754 bits (raw or
 * ALP-compressed), a LONG/DATE/TIMESTAMP or narrow DECIMAL ring stores one payload
 * word (raw or FoR-compressed), a DECIMAL128/DECIMAL256 ring stores two or four such
 * words, most significant first, and a valueless ring stores none - {@code count}'s
 * per-row state is the designated timestamp itself, so its chunk is a timestamp page
 * on its own.
 * {@link #of} configures which kind the seal writes. A {@code max}/{@code min} frame
 * ring uses the same payload as a value ring but tags its value pages with the deque
 * page kinds so a deque-family root stays distinct from a value-ring root.
 * <p>
 * The tail a boundary appends is always a fresh chunk, so a chunk boundary sits
 * at every checkpoint boundary and a sealed chunk is never rewritten. That is
 * what keeps a seal proportional to the rows the batch added instead of the
 * live frame: reopening the previous tail to top it up would re-encode every
 * row already in it, which for a dense cadence is the whole frame, every time.
 * The price is one chunk per boundary spanned by the frame; the caller bounds
 * that by rebuilding from empty once a partition's chunk count reaches its cap
 * ({@link #getChunkCount()}).
 * <p>
 * The builder never reads a data page. Everything it carries forward - the
 * chunk references, row count, head offset and last timestamp - comes out of
 * the previous root's checksummed partition entry, which is what lets a repair
 * chain onto a boundary whose chunks are still in an unpublished segment.
 * <p>
 * One builder serves every partition a seal freezes, one after another, so what it
 * needs per partition it keeps rather than allocates: the chunk references live in
 * objects it reuses, and {@link #freeze} encodes the scalar into native scratch the
 * builder owns until {@link #close()}. A warm seal therefore allocates no heap per
 * partition.
 */
public class LiveViewCheckpointRangeRingStateBuilder implements Closeable {

    private static final LiveViewCheckpointStatePageRef[] EMPTY_REFS = new LiveViewCheckpointStatePageRef[0];
    // The widest scalar the format has: four scalar words beside the fixed ones.
    private static final long SCALAR_SCRATCH_BYTES = LiveViewCheckpointRangeRingStateReader.scalarStateBytes(4);
    private final LiveViewCheckpointRangeRingStateReader previousReader;
    // Lazy: nothing is allocated until the first freeze. Untracked, as the frozen entry's own
    // scalar buffer is: it holds one scalar, not state that grows with the view.
    private final MemoryCARWImpl scalarScratch =
            new MemoryCARWImpl(SCALAR_SCRATCH_BYTES, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    private final LiveViewCheckpointStateCodec.Scratch scratch;
    private int headOffset;
    private boolean initialized;
    private long lastTimestamp;
    private int maxChunkRows = LiveViewCheckpointStateCodec.CHUNK_ROWS;
    private int pagesPerChunk = 2;
    private int refCount;
    // The first refCount slots are the references the state holds. The objects are reused
    // from one partition to the next, so the slots past refCount keep theirs, and no two
    // slots may name one object: writing a reference through one slot would change the
    // other. The format's reference limit bounds how many the array keeps.
    private LiveViewCheckpointStatePageRef[] refs = new LiveViewCheckpointStatePageRef[8];
    private long rowCount;
    private int scalarWords = 1;
    private int tailCount;
    private int valueKind = LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE;
    private int valueWords = 1;

    public LiveViewCheckpointRangeRingStateBuilder(@NotNull CairoConfiguration configuration) {
        this(configuration, null);
    }

    public LiveViewCheckpointRangeRingStateBuilder(
            @NotNull CairoConfiguration configuration,
            @Nullable MemoryTracker memoryTracker
    ) {
        previousReader = new LiveViewCheckpointRangeRingStateReader(configuration, memoryTracker);
        scratch = new LiveViewCheckpointStateCodec.Scratch(memoryTracker);
    }

    /**
     * Appends one row of a valueless ring in designated-timestamp order. The row is
     * its timestamp, and the chunk it lands in carries no value page.
     */
    public void append(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp
    ) {
        appendWords(writer, timestamp, 0, 0, 0, 0, 0);
    }

    /**
     * Appends one row of a one-word ring in designated-timestamp order. The value
     * arrives as raw 64-bit bits: a DOUBLE ring passes IEEE-754 bits (a NULL
     * first/last/nth row is a legitimate NaN), a LONG/DATE/TIMESTAMP or narrow DECIMAL
     * ring passes the raw payload. The value codec round-trips whatever bits it is
     * handed, so no value is rejected.
     */
    public void append(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            long valueBits
    ) {
        appendWords(writer, timestamp, 1, valueBits, 0, 0, 0);
    }

    /**
     * Appends one row of a two-word (128-bit decimal) ring, most significant word
     * first.
     */
    public void append(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            long hi,
            long lo
    ) {
        appendWords(writer, timestamp, 2, hi, lo, 0, 0);
    }

    /**
     * Appends one row of a four-word (256-bit decimal) ring, most significant word
     * first.
     */
    public void append(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            long hh,
            long hl,
            long lh,
            long ll
    ) {
        appendWords(writer, timestamp, 4, hh, hl, lh, ll);
    }

    @Override
    public void close() {
        Misc.free(previousReader);
        Misc.free(scratch);
        Misc.free(scalarScratch);
        initialized = false;
        refCount = 0;
        refs = EMPTY_REFS;
    }

    /**
     * Drops rows from the logical head without copying a shared page. Whole
     * chunks are unreferenced and a partial head is represented by headOffset.
     */
    public void dropHeadRows(long count) {
        ensureInitialized();
        if (count < 0 || count > rowCount) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring head drop out of bounds")
                    .put(" [count=").put(count).put(", rowCount=").put(rowCount).put(']');
        }
        long remaining = count;
        while (remaining > 0 && refCount > 0) {
            final int rows = refs[0].getRowCount();
            final int available = rows - headOffset;
            if (remaining < available) {
                headOffset += (int) remaining;
                rowCount -= remaining;
                return;
            }
            remaining -= available;
            rowCount -= available;
            removeFirstChunk();
            headOffset = 0;
        }
        if (remaining > 0) {
            // Only the rows this boundary appended are left to drop, and they sit
            // in scratch rather than in a page, so the head advances by moving them
            // down instead of by an offset.
            if (refCount != 0 || headOffset != 0 || remaining > tailCount) {
                throw CairoException.critical(0).put("live view checkpoint RANGE ring mutable head state inconsistent");
            }
            final int kept = tailCount - (int) remaining;
            if (kept > 0) {
                final long timestampBytes = (long) kept * Long.BYTES;
                final long droppedTimestamps = remaining * Long.BYTES;
                Vect.memmove(scratch.timestampsAddress(), scratch.timestampsAddress() + droppedTimestamps, timestampBytes);
                if (valueWords > 0) {
                    Vect.memmove(
                            scratch.valuesAddress(),
                            scratch.valuesAddress() + droppedTimestamps * valueWords,
                            timestampBytes * valueWords
                    );
                }
            }
            rowCount -= remaining;
            tailCount = kept;
            remaining = 0;
        }
        if (remaining != 0 || rowCount < 0) {
            throw CairoException.critical(0).put("live view checkpoint RANGE ring head drop state inconsistent");
        }
        if (rowCount == 0) {
            lastTimestamp = 0;
            refCount = 0;
            tailCount = 0;
            headOffset = 0;
        }
    }

    /**
     * Freezes the candidate descriptor. The exact scalar continuation state (the
     * running aggregate for avg/sum, the emitted frame value for first_value/
     * last_value/nth_value) is stored by raw bits, and the frame size is stored
     * rather than recomputed, to preserve the exact continuation state. The scalar
     * words beyond the ring's declared width are ignored. The {@code keyLength} key
     * bytes at {@code keyAddress}, the scalar and the chunk references are copied into
     * {@code out}, which keeps nothing of the builder's.
     */
    public void freeze(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long keyAddress,
            int keyLength,
            long scalarWord0,
            long scalarWord1,
            long scalarWord2,
            long scalarWord3,
            long frameSize,
            @NotNull LiveViewCheckpointPartitionMapEntry out
    ) {
        sealForFreeze(writer, frameSize);
        final int scalarBytes = LiveViewCheckpointRangeRingStateReader.scalarStateBytes(scalarWords);
        // The scratch is the builder's own memory, never out's, so the copy below reads a
        // source its own writes cannot move.
        final long scalarAddress = scalarScratch.appendAddressFor(0, scalarBytes);
        LiveViewCheckpointRangeRingStateReader.encodeScalar(
                scalarAddress,
                valueKind,
                scalarWords,
                headOffset,
                rowCount,
                scalarWord0,
                scalarWord1,
                scalarWord2,
                scalarWord3,
                frameSize,
                lastTimestamp
        );
        out.of(keyAddress, keyLength, scalarAddress, scalarBytes, refs, refCount);
        initialized = false;
    }

    /**
     * @return the chunks this state currently holds, sealed plus the one the
     * appended tail will become
     */
    public int getChunkCount() {
        ensureInitialized();
        return refCount / pagesPerChunk + (tailCount > 0 ? 1 : 0);
    }

    /**
     * @return the designated timestamp of the newest live row, or 0 for an empty
     * ring
     */
    public long getLastTimestamp() {
        ensureInitialized();
        return lastTimestamp;
    }

    public long getRowCount() {
        ensureInitialized();
        return rowCount;
    }

    /**
     * Carries the previous root's chunk references forward under the given value
     * kind and scalar width. Reads no data page: {@code previous} is the checksummed
     * partition entry, and everything the builder needs is in it. The kind and width
     * must match what the previous root wrote - a function seals a partition under one
     * shape across every boundary - so a mismatch means the caller wired the ring to
     * the wrong function.
     *
     * @param valueKind   one of the
     *                    {@link LiveViewCheckpointRangeRingStateReader} {@code VALUE_KIND_*}
     *                    constants
     * @param scalarWords the words the scalar continuation state occupies: 1, 2 or 4
     */
    public void of(@NotNull LiveViewCheckpointPartitionMapEntry previous, int valueKind, int scalarWords) {
        previousReader.ofMetadata(previous);
        if (previousReader.getValueKind() != valueKind || previousReader.getScalarWordCount() != scalarWords) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring state shape mismatch")
                    .put(" [configuredKind=").put(valueKind).put(", previousKind=").put(previousReader.getValueKind())
                    .put(", configuredScalarWords=").put(scalarWords)
                    .put(", previousScalarWords=").put(previousReader.getScalarWordCount()).put(']');
        }
        final int previousRefCount = previousReader.getStatePageCount();
        ensureRefCapacity(previousRefCount);
        for (int i = 0; i < previousRefCount; i++) {
            previousReader.getStatePageRef(i, pooledRef(i));
        }
        refCount = previousRefCount;
        rowCount = previousReader.getRowCount();
        headOffset = previousReader.getHeadOffset();
        lastTimestamp = previousReader.getLastTimestamp();
        tailCount = 0;
        ofShape(valueKind, scalarWords);
    }

    public void ofEmpty(int valueKind, int scalarWords) {
        refCount = 0;
        rowCount = 0;
        headOffset = 0;
        lastTimestamp = 0;
        tailCount = 0;
        ofShape(valueKind, scalarWords);
    }

    private void appendWords(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long timestamp,
            int words,
            long word0,
            long word1,
            long word2,
            long word3
    ) {
        ensureInitialized();
        if (words != valueWords) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring value width mismatch")
                    .put(" [expected=").put(valueWords).put(", actual=").put(words).put(']');
        }
        if (rowCount > 0 && timestamp < lastTimestamp) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring timestamps must be non-decreasing")
                    .put(" [previous=").put(lastTimestamp).put(", timestamp=").put(timestamp).put(']');
        }
        if (tailCount == maxChunkRows) {
            sealTail(writer);
        }
        Unsafe.putLong(scratch.timestampsAddress() + (long) tailCount * Long.BYTES, timestamp);
        if (words > 0) {
            final long valueAddress = scratch.valuesAddress() + (long) tailCount * words * Long.BYTES;
            Unsafe.putLong(valueAddress, word0);
            if (words > 1) {
                Unsafe.putLong(valueAddress + Long.BYTES, word1);
            }
            if (words > 2) {
                Unsafe.putLong(valueAddress + 2 * Long.BYTES, word2);
                Unsafe.putLong(valueAddress + 3 * Long.BYTES, word3);
            }
        }
        tailCount++;
        rowCount++;
        lastTimestamp = timestamp;
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint RANGE ring state builder is not initialized");
        }
    }

    private void ensureRefCapacity(int capacity) {
        if (capacity > refs.length) {
            refs = Arrays.copyOf(refs, Math.max(capacity, refs.length * 2));
        }
    }

    private void ofShape(int valueKind, int scalarWords) {
        this.valueKind = valueKind;
        this.valueWords = LiveViewCheckpointRangeRingStateReader.valueWords(valueKind);
        this.pagesPerChunk = LiveViewCheckpointRangeRingStateReader.pagesPerChunk(valueKind);
        this.maxChunkRows = LiveViewCheckpointRangeRingStateReader.maxChunkRows(valueKind);
        this.scalarWords = scalarWords;
        // Rejects an invalid width here rather than at freeze, so a wrongly wired
        // function cannot stream a whole partition before failing.
        LiveViewCheckpointRangeRingStateReader.scalarStateBytes(scalarWords);
        initialized = true;
    }

    /**
     * @return the reference object at {@code index}, created the first time the slot is
     * used and reused by every partition after that
     */
    private LiveViewCheckpointStatePageRef pooledRef(int index) {
        LiveViewCheckpointStatePageRef ref = refs[index];
        if (ref == null) {
            ref = refs[index] = new LiveViewCheckpointStatePageRef();
        }
        return ref;
    }

    private void removeFirstChunk() {
        // Rotates rather than shifts: the shift alone would leave the vacated slots naming
        // objects that are still live further down, and the next sealed tail, or the next
        // partition's carried-forward references, would then overwrite a live reference
        // through them. The dropped chunk's objects move to the vacated slots instead, so
        // every slot keeps an object of its own.
        final LiveViewCheckpointStatePageRef droppedTimestampRef = refs[0];
        final LiveViewCheckpointStatePageRef droppedValueRef = pagesPerChunk > 1 ? refs[1] : null;
        System.arraycopy(refs, pagesPerChunk, refs, 0, refCount - pagesPerChunk);
        refCount -= pagesPerChunk;
        refs[refCount] = droppedTimestampRef;
        if (pagesPerChunk > 1) {
            refs[refCount + 1] = droppedValueRef;
        }
    }

    /**
     * Seals the appended tail and validates the logical bounds for {@link #freeze}, which
     * then hands the first {@code refCount} references to the frozen entry.
     */
    private void sealForFreeze(
            @NotNull LiveViewCheckpointDataSegmentWriter writer,
            long frameSize
    ) {
        ensureInitialized();
        // A frame with an unbounded low bound counts rows into its aggregate and
        // then expires them from the ring, so frameSize may exceed the live rows.
        if (frameSize < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring frame size out of bounds")
                    .put(" [frameSize=").put(frameSize).put(", rowCount=").put(rowCount).put(']');
        }
        if (tailCount > 0) {
            sealTail(writer);
        }
        if (rowCount == 0) {
            refCount = 0;
            headOffset = 0;
            lastTimestamp = 0;
        }
        validateLogicalBounds();
    }

    private void sealTail(@NotNull LiveViewCheckpointDataSegmentWriter writer) {
        if (tailCount <= 0) {
            return;
        }
        ensureRefCapacity(refCount + pagesPerChunk);
        if (refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS - pagesPerChunk) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring state page reference count exceeds format limit");
        }
        final int timestampCodec = LiveViewCheckpointStateCodec.encodeTimestamps(
                writer.beginPage(), scratch, scratch.timestampsAddress(), tailCount
        );
        // endPage sets every field of the reference, so a reused object keeps nothing of the
        // page it named before.
        writer.endPage(
                pooledRef(refCount), tailCount * Long.BYTES,
                LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND,
                timestampCodec, tailCount, 0
        );
        if (valueWords == 0) {
            // A valueless ring's row is its timestamp, so the chunk is that page alone.
            refCount++;
            tailCount = 0;
            return;
        }
        final int valuePageKind = LiveViewCheckpointRangeRingStateReader.valuePageKind(valueKind);
        // A wide value spends several words per row; the page still counts rows, so
        // its decoded length carries the width the reader validates against.
        final int valueElements = tailCount * valueWords;
        final int valueCodec = LiveViewCheckpointRangeRingStateReader.isLongColumn(valueKind)
                ? LiveViewCheckpointStateCodec.encodeLongs(
                writer.beginPage(), scratch, scratch.valuesAddress(), valueElements)
                : LiveViewCheckpointStateCodec.encodeDoubles(
                writer.beginPage(), scratch, scratch.valuesAddress(), valueElements);
        writer.endPage(
                pooledRef(refCount + 1), valueElements * Long.BYTES,
                valuePageKind, valueCodec, tailCount, 0
        );
        refCount += 2;
        tailCount = 0;
    }

    private void validateLogicalBounds() {
        long physicalRows = 0;
        for (int i = 0; i < refCount; i += pagesPerChunk) {
            physicalRows += refs[i].getRowCount();
        }
        if ((rowCount == 0 && (physicalRows != 0 || headOffset != 0))
                || (rowCount > 0 && (refCount == 0 || headOffset < 0
                || headOffset >= refs[0].getRowCount() || physicalRows - headOffset != rowCount))) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring logical chunk bounds inconsistent")
                    .put(" [physicalRows=").put(physicalRows)
                    .put(", headOffset=").put(headOffset)
                    .put(", rowCount=").put(rowCount).put(']');
        }
    }
}
