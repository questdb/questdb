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
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Reads one checkpoint root's LV-private symbol-key dictionary: the sorted directory of
 * distinct base SYMBOL columns a view's partition terms key by, and - on demand, per column
 * - the append-only {@code lvId -> string} chunks each one names.
 * <p>
 * {@link #of} decodes and validates only the directory: column identities, ordering,
 * per-column {@code symbolCount} and the chunk page references each column names. That is
 * cheap and proportional to the number of distinct columns and chunks, not to the strings a
 * dictionary holds, which is what lets {@link LiveViewCheckpointKeyDictionaryWriter} open a
 * predecessor on every seal to path-copy its directory without re-validating every string the
 * dictionary has ever interned.
 * <p>
 * {@link #restoreInto} is the heavier per-column pass: it walks a column's chunks, validates
 * page framing and checksums (via {@link LiveViewCheckpointMetaSegmentReader}), decodes and
 * strictly validates each entry's UTF-8, rejects a duplicate string, and interns every entry
 * into the caller's {@link DirectSymbolMap} in id order - which is also how it builds the
 * {@code string -> lvId} reverse index, in the same pass, per section 6.6. A caller that
 * restores every bound column therefore pays the eager validation section 6.6 asks for; a
 * caller that never touches a column - one no live slot binds - never pays for it. On failure
 * {@code target} is left partially populated: the caller must discard it rather than continue
 * using it, exactly like every other {@code LV_CHECKPOINT_TIMELINE_INVALID} on this path.
 */
public class LiveViewCheckpointKeyDictionaryReader implements Closeable {

    public static final int CHUNK_PAGE_KIND = 0x2b;
    public static final int DIRECTORY_PAGE_KIND = 0x2a;
    static final int CHUNK_FORMAT_VERSION = 1;
    static final int DIRECTORY_FORMAT_VERSION = 1;
    private static final int CHUNK_HEADER_SIZE = 2 * Integer.BYTES;
    private static final int COLUMN_HEADER_SIZE = 4 * Integer.BYTES;
    private static final int COLUMN_FOOTER_SIZE = 2 * Integer.BYTES;
    private static final int DIRECTORY_HEADER_SIZE = 2 * Integer.BYTES;
    private static final int SEGMENT_CACHE_SIZE = 4;

    private final IntList baseTableIds = new IntList();
    private final IntList baseWriterColumnIndexes = new IntList();
    private final Path checkpointsDir = new Path();
    private final ObjList<LiveViewCheckpointPageRef> chunkRefs = new ObjList<>();
    private final long[] chunkSegmentIds = new long[SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointMetaSegmentReader[] chunkSegmentReaders = new LiveViewCheckpointMetaSegmentReader[SEGMENT_CACHE_SIZE];
    private final IntList chunkCounts = new IntList();
    private final IntList chunkStart = new IntList();
    private final CairoConfiguration configuration;
    private final ObjList<byte[]> columnNames = new ObjList<>();
    private final IntList columnTypes = new IntList();
    private final LiveViewCheckpointMetaSegmentReader directoryReader;
    private final IntList symbolCounts = new IntList();
    private int chunkSegmentClock;
    /**
     * Scratch UTF-16 output for {@link #decodeUtf8Strict}, grown on demand and reused across
     * entries. A byte-for-byte decode never produces more chars than input bytes (the widest
     * ratio is one ASCII byte to one char; every multi-byte sequence produces fewer chars than
     * bytes, including the two-char surrogate pair four bytes encode), so sizing it to the
     * entry's byte length is always enough.
     */
    private char[] utf8DecodeChars = new char[64];

    public LiveViewCheckpointKeyDictionaryReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        directoryReader = new LiveViewCheckpointMetaSegmentReader(configuration);
    }

    @Override
    public void close() {
        Misc.free(directoryReader);
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            chunkSegmentReaders[i] = Misc.free(chunkSegmentReaders[i]);
            chunkSegmentIds[i] = -1;
        }
        Misc.free(checkpointsDir);
    }

    /**
     * Unmaps every mapping this reader holds - the directory segment and every cached chunk
     * segment - while keeping the reader itself, so it holds no mapping into a file a later
     * retire, repair or compaction deletes.
     */
    public void detach() {
        directoryReader.close();
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (chunkSegmentReaders[i] != null) {
                chunkSegmentReaders[i].close();
            }
            chunkSegmentIds[i] = -1;
        }
        chunkSegmentClock = 0;
    }

    /**
     * @return the column's base table id, part of its identity together with
     * {@link #getBaseWriterColumnIndex}
     */
    public int getBaseTableId(int columnIndex) {
        checkColumnIndex(columnIndex);
        return baseTableIds.getQuick(columnIndex);
    }

    /**
     * @return the column's base-table writer index, part of its identity together with
     * {@link #getBaseTableId}
     */
    public int getBaseWriterColumnIndex(int columnIndex) {
        checkColumnIndex(columnIndex);
        return baseWriterColumnIndexes.getQuick(columnIndex);
    }

    /**
     * @return the number of chunk pages {@code columnIndex}'s dictionary is spread across
     */
    public int getChunkCount(int columnIndex) {
        checkColumnIndex(columnIndex);
        return chunkCounts.getQuick(columnIndex);
    }

    /**
     * @return the {@code chunkIndex}-th chunk reference for {@code columnIndex}, in id order.
     * Owned by this reader; do not mutate.
     */
    public @NotNull LiveViewCheckpointPageRef getChunkRef(int columnIndex, int chunkIndex) {
        checkColumnIndex(columnIndex);
        if (chunkIndex < 0 || chunkIndex >= chunkCounts.getQuick(columnIndex)) {
            throw CairoException.critical(0)
                    .put("live view checkpoint key dictionary chunk index out of bounds, index=").put(chunkIndex);
        }
        return chunkRefs.getQuick(chunkStart.getQuick(columnIndex) + chunkIndex);
    }

    /**
     * @return the number of distinct base SYMBOL columns this directory names
     */
    public int getColumnCount() {
        return baseTableIds.size();
    }

    /**
     * @return the column's canonical name, UTF-8 encoded, as persisted
     */
    public byte[] getColumnNameUtf8(int columnIndex) {
        checkColumnIndex(columnIndex);
        return columnNames.getQuick(columnIndex);
    }

    /**
     * @return the column's persisted type
     */
    public int getColumnType(int columnIndex) {
        checkColumnIndex(columnIndex);
        return columnTypes.getQuick(columnIndex);
    }

    /**
     * @return the number of ids {@code columnIndex}'s dictionary had handed out when this
     * root's seal froze it - one past the highest valid id
     */
    public int getSymbolCount(int columnIndex) {
        checkColumnIndex(columnIndex);
        return symbolCounts.getQuick(columnIndex);
    }

    /**
     * @return the column index whose identity is {@code (baseTableId, baseWriterColumnIndex)},
     * or {@code -1} when this directory names no such column
     */
    public int findColumn(int baseTableId, int baseWriterColumnIndex) {
        int lo = 0;
        int hi = baseTableIds.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (compareColumnKey(mid, baseTableId, baseWriterColumnIndex) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < baseTableIds.size() && compareColumnKey(lo, baseTableId, baseWriterColumnIndex) == 0 ? lo : -1;
    }

    /**
     * Decodes and validates the key dictionary directory at {@code directoryRef}: page framing
     * and checksum, column ordering and identities, and every chunk reference's structural
     * shape. Does not open a chunk page - see the class javadoc.
     */
    public void of(@Transient @NotNull Path checkpointsDir, @NotNull LiveViewCheckpointPageRef directoryRef) {
        this.checkpointsDir.of(checkpointsDir);
        LiveViewCheckpointMetadata.validateMetaRef(directoryRef, false, "key dictionary");
        directoryReader.of(checkpointsDir, directoryRef.getSegmentId());
        directoryReader.openPage(directoryRef);
        if (directoryReader.getPageKind() != DIRECTORY_PAGE_KIND) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary page kind unknown, kind=").put(directoryReader.getPageKind());
        }
        final int payloadLength = directoryReader.getPagePayloadLength();
        if (payloadLength < DIRECTORY_HEADER_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary payload too small, length=").put(payloadLength);
        }
        final int version = directoryReader.getInt(0);
        final int columnCount = directoryReader.getInt(Integer.BYTES);
        if (version != DIRECTORY_FORMAT_VERSION || columnCount < 0 || columnCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary version or column count invalid")
                    .put(" [version=").put(version).put(", columnCount=").put(columnCount).put(']');
        }

        baseTableIds.clear();
        baseWriterColumnIndexes.clear();
        columnTypes.clear();
        columnNames.clear();
        symbolCounts.clear();
        chunkRefs.clear();
        chunkStart.clear();
        chunkCounts.clear();

        long offset = DIRECTORY_HEADER_SIZE;
        for (int c = 0; c < columnCount; c++) {
            if (offset > payloadLength - COLUMN_HEADER_SIZE) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary column header truncated");
            }
            final int baseTableId = directoryReader.getInt(offset);
            final int baseWriterColumnIndex = directoryReader.getInt(offset + Integer.BYTES);
            final int columnType = directoryReader.getInt(offset + 2L * Integer.BYTES);
            final int nameLength = directoryReader.getInt(offset + 3L * Integer.BYTES);
            offset += COLUMN_HEADER_SIZE;
            LiveViewCheckpointMetadata.validateByteArrayLength(nameLength, "key dictionary column name");
            if (baseTableId < 0 || baseWriterColumnIndex < 0
                    || (c > 0 && compareColumnKey(c - 1, baseTableId, baseWriterColumnIndex) >= 0)) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary columns not strictly increasing")
                        .put(" [baseTableId=").put(baseTableId).put(", baseWriterColumnIndex=").put(baseWriterColumnIndex).put(']');
            }
            if (offset > payloadLength - nameLength) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary column name truncated");
            }
            final byte[] name = LiveViewCheckpointMetadata.readBytes(directoryReader, offset, nameLength);
            offset += nameLength;

            if (offset > payloadLength - COLUMN_FOOTER_SIZE) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary column footer truncated");
            }
            final int symbolCount = directoryReader.getInt(offset);
            final int chunkCount = directoryReader.getInt(offset + Integer.BYTES);
            offset += COLUMN_FOOTER_SIZE;
            if (symbolCount < 0 || chunkCount < 0 || chunkCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary column symbol or chunk count invalid")
                        .put(" [symbolCount=").put(symbolCount).put(", chunkCount=").put(chunkCount).put(']');
            }
            if (offset > payloadLength - (long) chunkCount * LiveViewCheckpointPageRef.BYTES) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary column chunk list truncated");
            }
            final int start = chunkRefs.size();
            for (int k = 0; k < chunkCount; k++) {
                final LiveViewCheckpointPageRef ref = new LiveViewCheckpointPageRef();
                LiveViewCheckpointMetadata.readMetaRef(directoryReader, offset, ref);
                LiveViewCheckpointMetadata.validateMetaRef(ref, false, "key dictionary chunk");
                chunkRefs.add(ref);
                offset += LiveViewCheckpointPageRef.BYTES;
            }

            baseTableIds.add(baseTableId);
            baseWriterColumnIndexes.add(baseWriterColumnIndex);
            columnTypes.add(columnType);
            columnNames.add(name);
            symbolCounts.add(symbolCount);
            chunkStart.add(start);
            chunkCounts.add(chunkCount);
        }
        if (offset != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary payload has trailing bytes");
        }
    }

    /**
     * Rebuilds {@code target} from {@code columnIndex}'s chunks: clears it, then interns every
     * entry in id order, validating page framing/checksums, UTF-8 well-formedness and string
     * uniqueness as it goes, and finally checks the reconstructed size against the directory's
     * frozen {@code symbolCount}. Clearing rather than merging is deliberate - section 6.3's
     * per-root-chain invariant means a restore replaces a slot's dictionary wholesale, so an id
     * a caller already assigned past this root's {@code symbolCount} (a rollback fork) is
     * discarded along with it.
     */
    public void restoreInto(int columnIndex, @NotNull DirectSymbolMap target) {
        checkColumnIndex(columnIndex);
        target.clear();
        final int start = chunkStart.getQuick(columnIndex);
        final int count = chunkCounts.getQuick(columnIndex);
        for (int k = 0; k < count; k++) {
            loadChunkInto(chunkRefs.getQuick(start + k), target, columnIndex);
        }
        final int expected = symbolCounts.getQuick(columnIndex);
        if (target.size() != expected) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary column symbol count mismatch")
                    .put(" [columnIndex=").put(columnIndex)
                    .put(", expected=").put(expected)
                    .put(", actual=").put(target.size()).put(']');
        }
    }

    private void checkColumnIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= baseTableIds.size()) {
            throw CairoException.critical(0)
                    .put("live view checkpoint key dictionary column index out of bounds, index=").put(columnIndex);
        }
    }

    private LiveViewCheckpointMetaSegmentReader chunkReaderFor(long segmentId) {
        for (int i = 0; i < SEGMENT_CACHE_SIZE; i++) {
            if (chunkSegmentIds[i] == segmentId && chunkSegmentReaders[i] != null) {
                return chunkSegmentReaders[i];
            }
        }
        final int slot = chunkSegmentClock;
        chunkSegmentClock = chunkSegmentClock + 1 == SEGMENT_CACHE_SIZE ? 0 : chunkSegmentClock + 1;
        if (chunkSegmentReaders[slot] == null) {
            chunkSegmentReaders[slot] = new LiveViewCheckpointMetaSegmentReader(configuration);
        }
        // Invalidate the slot before the open so a rejected open cannot leave it advertising a
        // healthy segment id against a closed reader - see LiveViewCheckpointPartitionMapReader.
        chunkSegmentIds[slot] = -1;
        chunkSegmentReaders[slot].of(checkpointsDir, segmentId);
        chunkSegmentIds[slot] = segmentId;
        return chunkSegmentReaders[slot];
    }

    private int compareColumnKey(int columnIndex, int baseTableId, int baseWriterColumnIndex) {
        final int cmp = Integer.compare(baseTableIds.getQuick(columnIndex), baseTableId);
        return cmp != 0 ? cmp : Integer.compare(baseWriterColumnIndexes.getQuick(columnIndex), baseWriterColumnIndex);
    }

    /**
     * Decodes {@code bytes} as UTF-8 into {@link #utf8DecodeChars}, rejecting anything a strict
     * decoder would: a truncated or invalid continuation-byte sequence, an overlong encoding, a
     * UTF-8-encoded surrogate, or a code point outside the Unicode range. Hand-rolled rather
     * than {@code java.nio.charset.CharsetDecoder} - a chunk page is opened in the restore path
     * of a package this build's own {@code LiveViewNoGcSourceHygieneTest} keeps
     * {@code ByteBuffer}-free, and {@link LiveViewCheckpointMetadata#putUtf8} is this decoder's
     * write-side counterpart in the same style.
     */
    private CharSequence decodeUtf8Strict(byte[] bytes) {
        final int len = bytes.length;
        if (utf8DecodeChars.length < len) {
            utf8DecodeChars = new char[len];
        }
        int out = 0;
        int i = 0;
        while (i < len) {
            final int b0 = bytes[i] & 0xff;
            if (b0 < 0x80) {
                utf8DecodeChars[out++] = (char) b0;
                i++;
            } else if ((b0 & 0xe0) == 0xc0) {
                final int cp = decodeContinuation(bytes, len, i, 1, b0 & 0x1f);
                if (cp < 0x80) {
                    throw malformedUtf8();
                }
                utf8DecodeChars[out++] = (char) cp;
                i += 2;
            } else if ((b0 & 0xf0) == 0xe0) {
                final int cp = decodeContinuation(bytes, len, i, 2, b0 & 0x0f);
                if (cp < 0x800 || (cp >= 0xd800 && cp <= 0xdfff)) {
                    throw malformedUtf8();
                }
                utf8DecodeChars[out++] = (char) cp;
                i += 3;
            } else if ((b0 & 0xf8) == 0xf0) {
                final int cp = decodeContinuation(bytes, len, i, 3, b0 & 0x07);
                if (cp < 0x10000 || cp > 0x10ffff) {
                    throw malformedUtf8();
                }
                final int adjusted = cp - 0x10000;
                utf8DecodeChars[out++] = (char) (0xd800 + (adjusted >> 10));
                utf8DecodeChars[out++] = (char) (0xdc00 + (adjusted & 0x3ff));
                i += 4;
            } else {
                throw malformedUtf8();
            }
        }
        return new String(utf8DecodeChars, 0, out);
    }

    /**
     * Decodes {@code continuationCount} trailing bytes of a multi-byte sequence starting at
     * {@code i}, folding them into {@code leadBits} (the lead byte's payload bits). Every
     * continuation byte must carry the {@code 10} high bits; anything else - including running
     * past {@code len} - is malformed.
     */
    private int decodeContinuation(byte[] bytes, int len, int i, int continuationCount, int leadBits) {
        if (i + continuationCount >= len) {
            throw malformedUtf8();
        }
        int codePoint = leadBits;
        for (int k = 1; k <= continuationCount; k++) {
            final int b = bytes[i + k] & 0xff;
            if ((b & 0xc0) != 0x80) {
                throw malformedUtf8();
            }
            codePoint = (codePoint << 6) | (b & 0x3f);
        }
        return codePoint;
    }

    private static CairoException malformedUtf8() {
        return LiveViewCheckpointMetadata.invalid("key dictionary entry is not valid UTF-8");
    }

    private void loadChunkInto(LiveViewCheckpointPageRef ref, DirectSymbolMap target, int columnIndex) {
        final LiveViewCheckpointMetaSegmentReader reader = chunkReaderFor(ref.getSegmentId());
        reader.openPage(ref);
        if (reader.getPageKind() != CHUNK_PAGE_KIND) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary chunk page kind unknown, kind=").put(reader.getPageKind());
        }
        final int payloadLength = reader.getPagePayloadLength();
        if (payloadLength < CHUNK_HEADER_SIZE) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary chunk payload too small, length=").put(payloadLength);
        }
        final int version = reader.getInt(0);
        final int entryCount = reader.getInt(Integer.BYTES);
        if (version != CHUNK_FORMAT_VERSION || entryCount < 0 || entryCount > LiveViewCheckpointMetadata.MAX_ENTRY_COUNT) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary chunk version or entry count invalid")
                    .put(" [version=").put(version).put(", entryCount=").put(entryCount).put(']');
        }
        long offset = CHUNK_HEADER_SIZE;
        for (int i = 0; i < entryCount; i++) {
            if (offset > payloadLength - Integer.BYTES) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary chunk entry header truncated");
            }
            final int length = reader.getInt(offset);
            offset += Integer.BYTES;
            LiveViewCheckpointMetadata.validateByteArrayLength(length, "key dictionary entry");
            if (offset > payloadLength - length) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary chunk entry body truncated");
            }
            final byte[] raw = LiveViewCheckpointMetadata.readBytes(reader, offset, length);
            offset += length;
            final CharSequence value = decodeUtf8Strict(raw);
            final int before = target.size();
            final int id = target.intern(value);
            if (id != before) {
                throw LiveViewCheckpointMetadata.invalid("key dictionary column has a duplicate string")
                        .put(" [columnIndex=").put(columnIndex).put(", id=").put(id).put(']');
            }
        }
        if (offset != payloadLength) {
            throw LiveViewCheckpointMetadata.invalid("key dictionary chunk payload has trailing bytes");
        }
    }
}
