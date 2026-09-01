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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Builds one checkpoint root's LV-private symbol-key dictionary directory, path-copying every
 * predecessor column's chunk list unchanged and appending one new chunk per column whose live
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
 */
public class LiveViewCheckpointKeyDictionaryWriter implements Closeable {

    private final Path checkpointsDir = new Path();
    /**
     * Scratch, one slot per column of the build in progress: the new chunk
     * {@link #writeChunk} wrote for a grown column, or {@code null} for a column the
     * predecessor's chunks already cover in full. Populated in a pass that runs to
     * completion - opening and closing every new chunk page - before the directory page
     * itself opens, because {@link LiveViewCheckpointMetaSegmentWriter} holds at most one
     * page open at a time and the directory page's payload names these refs.
     */
    private final ObjList<LiveViewCheckpointPageRef> newChunkRefs = new ObjList<>();
    private final LiveViewCheckpointKeyDictionaryReader predecessorReader;
    private final LongList referencedSegmentIds = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private long lastSegmentBytes;

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
     * Writes the directory - and one new chunk per grown column - into an aggregate metadata
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
            predecessorReader.of(checkpointsDir, predecessorRef);
        }
        referencedSegmentIds.clear();
        final int columnCount = columns.getColumnCount();
        for (int i = 1; i < columnCount; i++) {
            if (compareColumns(columns, i - 1, i) >= 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint key dictionary columns must be strictly increasing");
            }
        }

        // Pass 1: write every grown column's new chunk to completion first. A page must be
        // begun and ended before the next one starts, so a chunk page cannot be opened while
        // the directory page - whose payload names the chunk's ref - is itself still open.
        newChunkRefs.clear();
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
            newChunkRefs.add(liveCount > predecessorSymbolCount
                    ? writeChunk(columns, c, predecessorSymbolCount, liveCount, writer)
                    : null);
        }

        // Pass 2: the directory page, path-copying every predecessor chunk ref and pointing
        // at pass 1's new one where a column grew.
        final MemoryA mem = writer.beginPage(LiveViewCheckpointKeyDictionaryReader.DIRECTORY_PAGE_KIND);
        mem.putInt(LiveViewCheckpointKeyDictionaryReader.DIRECTORY_FORMAT_VERSION);
        mem.putInt(columnCount);
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
            final LiveViewCheckpointPageRef newChunkRef = newChunkRefs.getQuick(c);
            mem.putInt(columns.getEntryCount(c));
            mem.putInt(predecessorChunkCount + (newChunkRef != null ? 1 : 0));
            for (int k = 0; k < predecessorChunkCount; k++) {
                final LiveViewCheckpointPageRef ref = predecessorReader.getChunkRef(predecessorColumnIndex, k);
                LiveViewCheckpointMetadata.putMetaRef(mem, ref);
                referencedSegmentIds.add(ref.getSegmentId());
            }
            if (newChunkRef != null) {
                LiveViewCheckpointMetadata.putMetaRef(mem, newChunkRef);
                referencedSegmentIds.add(newChunkRef.getSegmentId());
            }
        }
        writer.endPage(out);
        referencedSegmentIds.add(out.getSegmentId());
        sortAndDedupe(referencedSegmentIds);
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

    private LiveViewCheckpointPageRef writeChunk(
            LiveViewCheckpointKeyDictionaryColumnSource columns,
            int columnIndex,
            int fromInclusive,
            int toExclusive,
            LiveViewCheckpointMetaSegmentWriter writer
    ) {
        final MemoryA mem = writer.beginPage(LiveViewCheckpointKeyDictionaryReader.CHUNK_PAGE_KIND);
        mem.putInt(LiveViewCheckpointKeyDictionaryReader.CHUNK_FORMAT_VERSION);
        mem.putInt(toExclusive - fromInclusive);
        for (int id = fromInclusive; id < toExclusive; id++) {
            final byte[] value = LiveViewCheckpointMetadata.encodeUtf8(columns.getEntryValue(columnIndex, id));
            mem.putInt(value.length);
            LiveViewCheckpointMetadata.putBytes(mem, value);
        }
        final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
        writer.endPage(ref);
        return ref;
    }
}
