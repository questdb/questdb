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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Builds one checkpoint root's LV-private symbol-key dictionary directory, path-copying every
 * predecessor column's chunk list unchanged and appending new chunks for each column whose live
 * dictionary has grown since the predecessor.
 * <p>
 * A column's chunk list therefore only ever grows: {@link #writeIntoOpenSegment} refuses a
 * column whose live entry count is <em>smaller</em> than the predecessor's frozen
 * {@code symbolCount}. Ids are never renumbered or reclaimed, so a dictionary shrinking would
 * mean an id downstream state still holds has stopped meaning what it did.
 * <p>
 * Reading the predecessor is deliberately cheap: {@link LiveViewCheckpointKeyDictionaryReader#of}
 * decodes only the directory, not the strings each chunk holds, so a seal's cost stays
 * proportional to what changed rather than to the dictionary's total size.
 * <p>
 * A delta wider than one page's limits becomes several chunks rather than one oversized page -
 * see {@link #MAX_CHUNK_ENTRY_COUNT} and {@link #MAX_CHUNK_PAYLOAD_BYTES}. A column's chunk list
 * is ordered and covers its ids contiguously from zero, so the number of chunks a delta takes is
 * invisible to a restore, which walks the list either way.
 */
public class LiveViewCheckpointKeyDictionaryWriter implements Closeable {

    /**
     * Most entries one chunk page carries. A restore refuses a chunk whose entry count is
     * above {@code MAX_ENTRY_COUNT} (1 << 20), so a delta bigger than that has to become
     * several chunks or the seal writes a page no restore can read - which surfaces as an
     * invalid timeline and a rebuild from base, not as a failed seal.
     * <p>
     * The cap sits well below the reader's limit rather than at it. A column's chunk count is
     * bounded by that same constant, so 64K-entry chunks still admit 2^16 x 2^20 ids per
     * column - far past the non-negative int id space a dictionary can hand out - and chunking
     * therefore introduces no ceiling of its own. The cost it does carry is that every chunk
     * ref is path-copied into the directory page on every later seal, 20 bytes a ref.
     */
    public static final int MAX_CHUNK_ENTRY_COUNT = 1 << 16;
    /**
     * Byte budget for one chunk page's payload, the second half of the same cap. A page header
     * carries its payload length in a single int and a restore checksums a whole page in one
     * pass, so a chunk of few but very long entries has to be split on bytes as well as on
     * count. Entries are never split across pages, so a single entry longer than the whole
     * budget - the format admits up to 1 MiB of UTF-8 per entry - takes a page of its own and
     * is the one case a chunk exceeds this.
     */
    public static final int MAX_CHUNK_PAYLOAD_BYTES = 8 * 1024 * 1024;

    private final Path checkpointsDir = new Path();
    /**
     * Scratch: the UTF-8 length of every entry the chunk being built carries, measured by
     * {@link #chunkEnd} and read back by {@link #writeChunk}, so each value is walked once
     * to measure and once to encode rather than twice to measure.
     */
    private final IntList entryUtf8Lengths = new IntList();
    /**
     * Scratch: the payload bytes the chunk {@link #chunkEnd} last measured will take, handed
     * to the segment writer as a size hint so the mapping grows once ahead of the page.
     */
    private long lastChunkPayloadBytes;
    /**
     * Scratch, one slot per column of the build in progress: how many new chunks
     * {@link #writeChunks} wrote for it, zero for a column the predecessor's chunks already
     * cover in full. Slices {@link #newChunkRefs}, which is flat across columns.
     */
    private final IntList newChunkCounts = new IntList();
    /**
     * Scratch: the chunk pages the build in progress wrote, in column order and, within a
     * column, in id order. Populated in a pass that runs to completion - opening and closing
     * every new chunk page - before the directory page itself opens, because
     * {@link LiveViewCheckpointMetaSegmentWriter} holds at most one page open at a time and
     * the directory page's payload names these refs.
     * <p>
     * The refs are pooled across builds and claimed by {@link #nextChunkRef()}, because one
     * wide delta chunks into many pages and a seal must not allocate a reference per page.
     */
    private final ObjList<LiveViewCheckpointPageRef> newChunkRefs = new ObjList<>();
    private final LiveViewCheckpointKeyDictionaryReader predecessorReader;
    private final LongList referencedSegmentIds = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private long lastSegmentBytes;
    private int newChunkRefCount;
    // The predecessor directory predecessorReader currently has open, so isUnchanged and
    // the write that follows it within one seal map the page once. Forgotten by of() and
    // detach(), which bound the reuse to one seal of one view: a segment id names a file
    // only within a directory, and a mapping must not outlive the seal that took it.
    private long openedPredecessorOffset = -1;
    private long openedPredecessorSegmentId = -1;

    public LiveViewCheckpointKeyDictionaryWriter(@NotNull CairoConfiguration configuration) {
        predecessorReader = new LiveViewCheckpointKeyDictionaryReader(configuration);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    @Override
    public void close() {
        Misc.free(predecessorReader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    /**
     * Releases every mapping this build read and discards any in-flight segment, keeping the
     * reader, writer and shells for the next build.
     */
    public void detach() {
        predecessorReader.detach();
        segmentWriter.discard();
        openedPredecessorSegmentId = -1;
        openedPredecessorOffset = -1;
    }

    /**
     * Whether {@code columns} is exactly the dictionary {@code predecessorRef} froze: the
     * same columns in the same order, each with the same entry count. A seal that interned
     * nothing since its predecessor need not write a directory at all - see
     * {@link #reusePredecessor}. A fresh dictionary (null predecessor) is never unchanged.
     */
    public boolean isUnchanged(
            @NotNull LiveViewCheckpointPageRef predecessorRef,
            @NotNull LiveViewCheckpointKeyDictionaryColumnSource columns
    ) {
        if (predecessorRef.isNull()) {
            return false;
        }
        openPredecessor(predecessorRef);
        final int columnCount = columns.getColumnCount();
        if (predecessorReader.getColumnCount() != columnCount) {
            return false;
        }
        for (int c = 0; c < columnCount; c++) {
            if (predecessorReader.getBaseTableId(c) != columns.getBaseTableId(c)
                    || predecessorReader.getBaseWriterColumnIndex(c) != columns.getBaseWriterColumnIndex(c)
                    || predecessorReader.getColumnType(c) != columns.getColumnType(c)
                    || predecessorReader.getSymbolCount(c) != columns.getEntryCount(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Names the predecessor's own directory page as this root's dictionary, for a seal
     * {@link #isUnchanged} found nothing to write. The referenced segments are the page's
     * whole closure - every chunk's segment and the directory page's own - so the root
     * that adopts them keeps them retained exactly as it would a directory it wrote.
     */
    public void reusePredecessor(
            @NotNull LiveViewCheckpointPageRef predecessorRef,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        openPredecessor(predecessorRef);
        referencedSegmentIds.clear();
        for (int c = 0, n = predecessorReader.getColumnCount(); c < n; c++) {
            for (int k = 0, m = predecessorReader.getChunkCount(c); k < m; k++) {
                referencedSegmentIds.add(predecessorReader.getChunkRef(c, k).getSegmentId());
            }
        }
        referencedSegmentIds.add(predecessorRef.getSegmentId());
        sortAndDedupe(referencedSegmentIds);
        out.of(predecessorRef.getSegmentId(), predecessorRef.getOffset(), predecessorRef.getLength());
        lastSegmentBytes = 0;
    }

    /**
     * @return total bytes written to the last build's metadata segment
     */
    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    /**
     * @return every segment the last build's directory closure touches - every chunk's segment,
     * old and new, plus the segment the directory page itself landed in - sorted and deduped.
     * This is the delta a caller folds into a checkpoint root's own segment set, exactly as it
     * already does for the anchor, window and function roots.
     */
    public @NotNull LongList getReferencedSegmentIds() {
        return referencedSegmentIds;
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        // A page reference names a file only together with the directory it lives in, and
        // this writer serves every view a worker seals: forget what was open for the last one.
        openedPredecessorSegmentId = -1;
        openedPredecessorOffset = -1;
    }

    /**
     * Writes the directory into its own metadata segment and commits it.
     *
     * @see #writeIntoOpenSegment
     */
    public void write(
            @NotNull LiveViewCheckpointPageRef predecessorRef,
            @NotNull LiveViewCheckpointKeyDictionaryColumnSource columns,
            long metadataSegmentId,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        segmentWriter.of(checkpointsDir, metadataSegmentId);
        writeIntoOpenSegment(predecessorRef, columns, segmentWriter, out);
        lastSegmentBytes = segmentWriter.commit();
    }

    /**
     * Writes the directory - and every grown column's new chunks - into an aggregate metadata
     * segment the caller owns. {@code predecessorRef} may be a cleared (null) reference for a
     * fresh dictionary. {@code columns} must already be sorted ascending by
     * {@code (baseTableId, baseWriterColumnIndex)}; the writer validates the order rather than
     * establishing it; see {@link LiveViewCheckpointKeyDictionaryColumnSource}.
     */
    public void writeIntoOpenSegment(
            @NotNull LiveViewCheckpointPageRef predecessorRef,
            @NotNull LiveViewCheckpointKeyDictionaryColumnSource columns,
            @NotNull LiveViewCheckpointMetaSegmentWriter writer,
            @NotNull LiveViewCheckpointPageRef out
    ) {
        final boolean hasPredecessor = !predecessorRef.isNull();
        if (hasPredecessor) {
            openPredecessor(predecessorRef);
        }
        referencedSegmentIds.clear();
        final int columnCount = columns.getColumnCount();
        for (int i = 1; i < columnCount; i++) {
            if (compareColumns(columns, i - 1, i) >= 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint key dictionary columns must be strictly increasing");
            }
        }

        // Pass 1: write every grown column's new chunks to completion first. A page must be
        // begun and ended before the next one starts, so a chunk page cannot be opened while
        // the directory page - whose payload names the chunks' refs - is itself still open.
        newChunkCounts.clear();
        newChunkRefCount = 0;
        for (int c = 0; c < columnCount; c++) {
            final int liveCount = columns.getEntryCount(c);
            final int predecessorSymbolCount = predecessorSymbolCount(hasPredecessor, columns, c);
            if (liveCount < predecessorSymbolCount) {
                throw CairoException.critical(0)
                        .put("live view checkpoint key dictionary column shrank")
                        .put(" [baseTableId=").put(columns.getBaseTableId(c))
                        .put(", baseWriterColumnIndex=").put(columns.getBaseWriterColumnIndex(c))
                        .put(", predecessor=").put(predecessorSymbolCount)
                        .put(", live=").put(liveCount).put(']');
            }
            newChunkCounts.add(writeChunks(columns, c, predecessorSymbolCount, liveCount, writer));
        }

        // Pass 2: the directory page, path-copying every predecessor chunk ref and appending
        // pass 1's new ones, in id order, where a column grew.
        final MemoryA mem = writer.beginPage(LiveViewCheckpointKeyDictionaryReader.DIRECTORY_PAGE_KIND);
        mem.putInt(LiveViewCheckpointKeyDictionaryReader.DIRECTORY_FORMAT_VERSION);
        mem.putInt(columnCount);
        int newChunkCursor = 0;
        for (int c = 0; c < columnCount; c++) {
            final int baseTableId = columns.getBaseTableId(c);
            final int baseWriterColumnIndex = columns.getBaseWriterColumnIndex(c);
            mem.putInt(baseTableId);
            mem.putInt(baseWriterColumnIndex);
            mem.putInt(columns.getColumnType(c));
            final byte[] name = LiveViewCheckpointMetadata.encodeUtf8(columns.getColumnName(c));
            mem.putInt(name.length);
            LiveViewCheckpointMetadata.putBytes(mem, name);

            final int predecessorColumnIndex = hasPredecessor
                    ? predecessorReader.findColumn(baseTableId, baseWriterColumnIndex)
                    : -1;
            final int predecessorChunkCount = predecessorColumnIndex >= 0
                    ? predecessorReader.getChunkCount(predecessorColumnIndex)
                    : 0;
            final int newChunkCount = newChunkCounts.getQuick(c);
            mem.putInt(columns.getEntryCount(c));
            mem.putInt(predecessorChunkCount + newChunkCount);
            for (int k = 0; k < predecessorChunkCount; k++) {
                final LiveViewCheckpointPageRef ref = predecessorReader.getChunkRef(predecessorColumnIndex, k);
                LiveViewCheckpointMetadata.putMetaRef(mem, ref);
                referencedSegmentIds.add(ref.getSegmentId());
            }
            for (int k = 0; k < newChunkCount; k++) {
                final LiveViewCheckpointPageRef ref = newChunkRefs.getQuick(newChunkCursor++);
                LiveViewCheckpointMetadata.putMetaRef(mem, ref);
                referencedSegmentIds.add(ref.getSegmentId());
            }
        }
        writer.endPage(out);
        referencedSegmentIds.add(out.getSegmentId());
        sortAndDedupe(referencedSegmentIds);
    }

    private void openPredecessor(LiveViewCheckpointPageRef predecessorRef) {
        if (openedPredecessorSegmentId == predecessorRef.getSegmentId()
                && openedPredecessorOffset == predecessorRef.getOffset()) {
            return;
        }
        openedPredecessorSegmentId = -1;
        openedPredecessorOffset = -1;
        predecessorReader.of(checkpointsDir, predecessorRef);
        openedPredecessorSegmentId = predecessorRef.getSegmentId();
        openedPredecessorOffset = predecessorRef.getOffset();
    }

    private int predecessorSymbolCount(boolean hasPredecessor, LiveViewCheckpointKeyDictionaryColumnSource columns, int columnIndex) {
        if (!hasPredecessor) {
            return 0;
        }
        final int predecessorColumnIndex = predecessorReader.findColumn(
                columns.getBaseTableId(columnIndex), columns.getBaseWriterColumnIndex(columnIndex)
        );
        return predecessorColumnIndex >= 0 ? predecessorReader.getSymbolCount(predecessorColumnIndex) : 0;
    }

    private static int compareColumns(LiveViewCheckpointKeyDictionaryColumnSource columns, int a, int b) {
        final int cmp = Integer.compare(columns.getBaseTableId(a), columns.getBaseTableId(b));
        return cmp != 0 ? cmp : Integer.compare(columns.getBaseWriterColumnIndex(a), columns.getBaseWriterColumnIndex(b));
    }

    private static void sortAndDedupe(LongList list) {
        list.sort();
        int w = 0;
        for (int i = 0, n = list.size(); i < n; i++) {
            if (i == 0 || list.getQuick(i) != list.getQuick(i - 1)) {
                list.setQuick(w++, list.getQuick(i));
            }
        }
        list.setPos(w);
    }

    /**
     * @return one past the last id the chunk starting at {@code fromInclusive} takes:
     * {@link #MAX_CHUNK_ENTRY_COUNT} entries at most, and {@link #MAX_CHUNK_PAYLOAD_BYTES} of
     * payload at most unless the chunk's first entry already exceeds the budget on its own.
     * Never {@code fromInclusive}, so a caller's loop always advances.
     */
    private int chunkEnd(
            LiveViewCheckpointKeyDictionaryColumnSource columns,
            int columnIndex,
            int fromInclusive,
            int toExclusive
    ) {
        final int limit = (int) Math.min(toExclusive, (long) fromInclusive + MAX_CHUNK_ENTRY_COUNT);
        long bytes = LiveViewCheckpointKeyDictionaryReader.CHUNK_HEADER_SIZE;
        int id = fromInclusive;
        entryUtf8Lengths.clear();
        while (id < limit) {
            // Measures rather than encodes: the entry is encoded once, by writeChunk, which
            // reads the length measured here back off entryUtf8Lengths rather than walking
            // the value a second time.
            final int utf8Length = LiveViewCheckpointMetadata.utf8Bytes(columns.getEntryValue(columnIndex, id));
            final long entryBytes = Integer.BYTES + utf8Length;
            if (id > fromInclusive && bytes + entryBytes > MAX_CHUNK_PAYLOAD_BYTES) {
                break;
            }
            entryUtf8Lengths.add(utf8Length);
            bytes += entryBytes;
            id++;
        }
        lastChunkPayloadBytes = bytes;
        return id;
    }

    /**
     * @return the next pooled chunk reference for the build in progress. Refs stay claimed
     * until the next {@link #writeIntoOpenSegment} resets the count, because the directory
     * page reads every one of them after the last chunk page closes.
     */
    private LiveViewCheckpointPageRef nextChunkRef() {
        if (newChunkRefCount == newChunkRefs.size()) {
            newChunkRefs.add(new LiveViewCheckpointPageRef());
        }
        return newChunkRefs.getQuick(newChunkRefCount++);
    }

    private void writeChunk(
            LiveViewCheckpointKeyDictionaryColumnSource columns,
            int columnIndex,
            int fromInclusive,
            int toExclusive,
            LiveViewCheckpointMetaSegmentWriter writer,
            LiveViewCheckpointPageRef out
    ) {
        final MemoryA mem = writer.beginPage(LiveViewCheckpointKeyDictionaryReader.CHUNK_PAGE_KIND, lastChunkPayloadBytes);
        mem.putInt(LiveViewCheckpointKeyDictionaryReader.CHUNK_FORMAT_VERSION);
        mem.putInt(toExclusive - fromInclusive);
        for (int id = fromInclusive; id < toExclusive; id++) {
            // Encoded straight into the page: no byte array per entry, and the length
            // chunkEnd measured for the split decision is the prefix written here.
            final int utf8Length = entryUtf8Lengths.getQuick(id - fromInclusive);
            mem.putInt(utf8Length);
            LiveViewCheckpointMetadata.putUtf8(mem, columns.getEntryValue(columnIndex, id), utf8Length);
        }
        writer.endPage(out);
    }

    /**
     * Writes {@code [fromInclusive, toExclusive)} as however many chunk pages the per-page
     * limits require, appending each one's reference to {@link #newChunkRefs} in id order. An
     * empty range writes nothing.
     *
     * @return how many chunk pages the range took
     */
    private int writeChunks(
            LiveViewCheckpointKeyDictionaryColumnSource columns,
            int columnIndex,
            int fromInclusive,
            int toExclusive,
            LiveViewCheckpointMetaSegmentWriter writer
    ) {
        int chunkCount = 0;
        int id = fromInclusive;
        while (id < toExclusive) {
            final int end = chunkEnd(columns, columnIndex, id, toExclusive);
            writeChunk(columns, columnIndex, id, end, writer, nextChunkRef());
            chunkCount++;
            id = end;
        }
        return chunkCount;
    }
}
