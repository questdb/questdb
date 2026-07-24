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
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Restores the persistent chunked ring shared by the partitioned window functions
 * over a bounded RANGE frame: {@code avg}/{@code sum} carry a running aggregate,
 * {@code first_value}/{@code last_value}/{@code nth_value} carry the frame value they
 * emit, {@code max}/{@code min} carry the frame ring their monotonic deque is
 * rebuilt from on restore, and {@code count} carries the frame's timestamps alone.
 * A logical chunk is a timestamp page followed by a value page, or a timestamp page
 * on its own when the ring stores no value.
 * <p>
 * A row's value occupies zero, one, two or four 64-bit words, which the ring's value
 * kind selects. A DOUBLE value page stores exact IEEE-754 bits (raw or XOR-compressed)
 * in one word; a LONG/DATE/TIMESTAMP page and a narrow DECIMAL page (physical width 8,
 * 16, 32 or 64 bits, all of which a signed 64-bit word holds exactly) store the raw
 * payload in one word, because an arbitrary integer has no floating-point structure to
 * compress and reinterpreting it as a double could canonicalize a NaN bit pattern; a
 * DECIMAL128 page stores two raw words and a DECIMAL256 page four, most significant
 * first. {@link #VALUE_KIND_NONE} stores none: {@code count}'s per-row state is the
 * designated timestamp itself, so its chunk is the timestamp page alone and it carries
 * no value page to pay for. The reader delivers every value as raw 64-bit words and
 * leaves the function to interpret them. The partition entry's checksummed scalar
 * payload owns the ring's value kind, the logical head offset and the exact scalar
 * continuation state.
 * <p>
 * The scalar continuation state is itself one, two or four words wide, independently of
 * the value width: a {@code decimal(20,4)} {@code avg} holds a 64-bit value per row and a
 * 256-bit running sum, while a {@code decimal(38,4)} {@code first_value} holds a 128-bit
 * value per row and no scalar at all.
 * <p>
 * A {@code max}/{@code min} root stores the same {@code (timestamp, value)} frame ring
 * as the value functions, but tags its value pages with the monotonic-deque page kinds
 * ({@link #DEQUE_DOUBLE_VALUE_PAGE_KIND}, {@link #DEQUE_LONG_VALUE_PAGE_KIND},
 * {@link #DEQUE_DECIMAL128_VALUE_PAGE_KIND} and {@link #DEQUE_DECIMAL256_VALUE_PAGE_KIND})
 * so a deque-family root's pages stay self-identifying and never resolve as a value-ring
 * root's. The pages carry the frame ring; the monotonic deque itself is a runtime
 * acceleration structure the function replays out of that ring at restore, so it is
 * never persisted.
 * <p>
 * Ring values may be non-finite: a base {@code first_value}/{@code last_value}
 * over a frame whose oldest/newest row is NULL emits NaN and stores it. The
 * reader therefore validates only timestamp order and logical bounds here; a
 * function whose ring is finite by construction (avg/sum) re-asserts that in its
 * own restore consumer, where a corrupt value page turns into a rejected root
 * rather than a silently wrong aggregate.
 * <p>
 * Chunks carry whatever row count the seal that wrote them appended, capped at
 * {@link #maxChunkRows(int)}: a cadence seal closes its tail so the next root can
 * reference it rather than copy it, which puts a chunk boundary at every checkpoint
 * boundary. The cap divides {@link LiveViewCheckpointStateCodec#CHUNK_ROWS} by the
 * value width, so a chunk's value page never exceeds one codec scratch buffer whatever
 * the width. The scalar row count and head offset, not the chunk sizes, say which rows
 * are live.
 */
public class LiveViewCheckpointRangeRingStateReader implements Closeable, LiveViewCheckpointRingStateSource {

    public static final int DECIMAL128_VALUE_PAGE_KIND = 0x26;
    public static final int DECIMAL256_VALUE_PAGE_KIND = 0x27;
    public static final int DEQUE_DECIMAL128_VALUE_PAGE_KIND = 0x28;
    public static final int DEQUE_DECIMAL256_VALUE_PAGE_KIND = 0x29;
    public static final int DEQUE_DOUBLE_VALUE_PAGE_KIND = 0x24;
    public static final int DEQUE_LONG_VALUE_PAGE_KIND = 0x25;
    public static final int DOUBLE_VALUE_PAGE_KIND = 0x22;
    public static final int FORMAT_VERSION = 2;
    public static final int LONG_VALUE_PAGE_KIND = 0x23;
    public static final int TIMESTAMP_PAGE_KIND = 0x21;
    public static final int VALUE_KIND_DECIMAL128 = 4;
    public static final int VALUE_KIND_DECIMAL256 = 5;
    public static final int VALUE_KIND_DEQUE_DECIMAL128 = 6;
    public static final int VALUE_KIND_DEQUE_DECIMAL256 = 7;
    public static final int VALUE_KIND_DEQUE_DOUBLE = 2;
    public static final int VALUE_KIND_DEQUE_LONG = 3;
    public static final int VALUE_KIND_DOUBLE = 0;
    public static final int VALUE_KIND_LONG = 1;
    public static final int VALUE_KIND_NONE = 8;
    /**
     * Data segments one restore keeps mapped at a time. A cadence seal appends
     * each partition's new rows as a fresh chunk in its own boundary's data
     * segment and carries the older chunks forward by reference, so one
     * partition's ring spans up to {@link LiveViewCheckpointRingSeal#MAX_LIVE_CHUNKS}
     * segments - and every other partition of the same function spans the same
     * ones. A restore walks the partitions one after another, so a reader that
     * maps a single segment re-opens that whole span once per partition. A cache
     * that covers the span maps each segment once per restore instead.
     * <p>
     * Direct-mapped on the segment id, which the seal mints sequentially, so a
     * span that fits collides nowhere. A span that does not fit (a sparse
     * partition can hold its chunk cap over a much wider run of boundaries)
     * degrades to the single-segment behaviour for the ids that collide.
     */
    private static final int DATA_SEGMENT_CACHE_SIZE = Numbers.ceilPow2(LiveViewCheckpointRingSeal.MAX_LIVE_CHUNKS);
    private static final int FLAGS = 0;
    private static final int SCALAR_FIXED_WORDS = 4;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointDataSegmentReader[] dataReaders =
            new LiveViewCheckpointDataSegmentReader[DATA_SEGMENT_CACHE_SIZE];
    private final long[] dataSegmentIds = new long[DATA_SEGMENT_CACHE_SIZE];
    private final LiveViewCheckpointStateCodec.Scratch scratch;
    private long frameSize;
    private int headOffset;
    private boolean initialized;
    private long lastTimestamp;
    // The segment reader the last openPage bound; the decoders read the page off it.
    private LiveViewCheckpointDataSegmentReader openReader;
    private long rowCount;
    private long scalarWord0;
    private long scalarWord1;
    private long scalarWord2;
    private long scalarWord3;
    private int scalarWords = 1;
    private LiveViewCheckpointSegmentDirectoryReader segmentDirectory;
    private LiveViewCheckpointStatePageRef[] statePageRefs = new LiveViewCheckpointStatePageRef[0];
    private int valueKind = VALUE_KIND_DOUBLE;

    public LiveViewCheckpointRangeRingStateReader(@NotNull CairoConfiguration configuration) {
        this(configuration, null);
    }

    public LiveViewCheckpointRangeRingStateReader(
            @NotNull CairoConfiguration configuration,
            @Nullable MemoryTracker memoryTracker
    ) {
        this.configuration = configuration;
        scratch = new LiveViewCheckpointStateCodec.Scratch(memoryTracker);
        Arrays.fill(dataSegmentIds, -1);
    }

    /**
     * @return the rows one chunk of a {@code valueKind} ring may hold. Dividing the
     * codec's chunk cap by the value width keeps a chunk's value page inside one
     * scratch buffer at every width; a valueless ring writes no value page, so the
     * timestamp page alone bounds it
     */
    public static int maxChunkRows(int valueKind) {
        final int words = valueWords(valueKind);
        return words == 0
                ? LiveViewCheckpointStateCodec.CHUNK_ROWS
                : LiveViewCheckpointStateCodec.CHUNK_ROWS / words;
    }

    /**
     * @return the bytes a partition entry's scalar payload occupies for a
     * {@code scalarWords}-wide scalar continuation state
     */
    public static int scalarStateBytes(int scalarWords) {
        validateScalarWords(scalarWords);
        return (SCALAR_FIXED_WORDS + scalarWords) * Long.BYTES;
    }

    /**
     * @return the 64-bit words one row's value occupies under {@code valueKind}, zero
     * for a ring whose rows are timestamps alone
     */
    public static int valueWords(int valueKind) {
        switch (valueKind) {
            case VALUE_KIND_NONE:
                return 0;
            case VALUE_KIND_DOUBLE:
            case VALUE_KIND_LONG:
            case VALUE_KIND_DEQUE_DOUBLE:
            case VALUE_KIND_DEQUE_LONG:
                return 1;
            case VALUE_KIND_DECIMAL128:
            case VALUE_KIND_DEQUE_DECIMAL128:
                return 2;
            case VALUE_KIND_DECIMAL256:
            case VALUE_KIND_DEQUE_DECIMAL256:
                return 4;
            default:
                throw CairoException.critical(0)
                        .put("live view checkpoint RANGE ring value kind invalid [kind=").put(valueKind).put(']');
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < DATA_SEGMENT_CACHE_SIZE; i++) {
            dataReaders[i] = Misc.free(dataReaders[i]);
            dataSegmentIds[i] = -1;
        }
        Misc.free(scratch);
        Misc.free(checkpointsDir);
        initialized = false;
        openReader = null;
        segmentDirectory = null;
        statePageRefs = new LiveViewCheckpointStatePageRef[0];
    }

    /**
     * Unmaps every cached data segment while keeping the readers themselves, so a
     * reader that outlives one restore holds no mapping into files a later retire,
     * repair or compaction deletes - and cannot serve a page out of a segment id
     * a rebuilt timeline re-minted. The next walk re-opens what it touches.
     */
    public void detach() {
        for (int i = 0; i < DATA_SEGMENT_CACHE_SIZE; i++) {
            final LiveViewCheckpointDataSegmentReader reader = dataReaders[i];
            if (reader != null) {
                reader.close();
            }
            dataSegmentIds[i] = -1;
        }
        openReader = null;
    }

    /**
     * Decodes every live row in canonical ring order. Payload validation is
     * deliberately lazy: opening the root validates bounded metadata only,
     * while a malformed referenced data page invalidates the root when read.
     */
    @Override
    public void forEachRow(@NotNull RowConsumer consumer) {
        walk(null, consumer, null, null, 1);
    }

    @Override
    public void forEachRow(@NotNull Decimal128RowConsumer consumer) {
        walk(null, null, consumer, null, 2);
    }

    @Override
    public void forEachRow(@NotNull Decimal256RowConsumer consumer) {
        walk(null, null, null, consumer, 4);
    }

    @Override
    public void forEachTimestamp(@NotNull TimestampConsumer consumer) {
        walk(consumer, null, null, null, 0);
    }

    @Override
    public long getFrameSize() {
        ensureInitialized();
        return frameSize;
    }

    public int getHeadOffset() {
        ensureInitialized();
        return headOffset;
    }

    public long getLastTimestamp() {
        ensureInitialized();
        return lastTimestamp;
    }

    @Override
    public long getRowCount() {
        ensureInitialized();
        return rowCount;
    }

    @Override
    public long getScalarBits() {
        return getScalarWord(0);
    }

    @Override
    public long getScalarWord(int index) {
        ensureInitialized();
        if (index < 0 || index >= scalarWords) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring scalar word out of bounds")
                    .put(" [index=").put(index).put(", words=").put(scalarWords).put(']');
        }
        switch (index) {
            case 0:
                return scalarWord0;
            case 1:
                return scalarWord1;
            case 2:
                return scalarWord2;
            default:
                return scalarWord3;
        }
    }

    @Override
    public int getScalarWordCount() {
        ensureInitialized();
        return scalarWords;
    }

    public int getStatePageCount() {
        ensureInitialized();
        return statePageRefs.length;
    }

    public void getStatePageRef(int index, @NotNull LiveViewCheckpointStatePageRef out) {
        ensureInitialized();
        copyRef(statePageRefs[index], out);
    }

    /**
     * @return the ring's value kind, one of the {@code VALUE_KIND_*} constants. The
     * scalar payload carries it, so an empty ring - which has no value page to
     * identify a kind from - still reports the kind its owner sealed it under
     */
    public int getValueKind() {
        ensureInitialized();
        return valueKind;
    }

    /**
     * Opens {@code entry} for both metadata and payload access.
     */
    public void of(
            @Transient @NotNull Path checkpointsDir,
            @NotNull LiveViewCheckpointSegmentDirectoryReader segmentDirectory,
            @NotNull LiveViewCheckpointPartitionMapEntry entry
    ) {
        ofMetadata(entry);
        if (!Utf8s.equals(this.checkpointsDir, checkpointsDir)) {
            // Segment ids are unique within one view's checkpoint directory and
            // nowhere else, so cached segments of another view cannot be reused.
            detach();
            this.checkpointsDir.of(checkpointsDir);
        }
        this.segmentDirectory = segmentDirectory;
    }

    /**
     * Decodes and validates {@code entry}'s scalar payload and chunk references
     * without binding a data segment to read them from.
     * <p>
     * A cadence seal starts from the previous root this way: it needs the row
     * count, head offset, last timestamp and chunk references to carry the
     * shared prefix forward, and none of those live in a data page. It also
     * means a repair can chain one captured boundary onto the one before it,
     * whose chunks are still sitting in an unpublished temporary segment that no
     * reader could open.
     */
    public void ofMetadata(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
        initialized = false;
        openReader = null;
        this.segmentDirectory = null;
        final byte[] scalar = entry.getScalarState();
        if (scalar.length < scalarStateBytes(1)) {
            throw invalid("RANGE ring scalar state size mismatch")
                    .put(" [minimum=").put(scalarStateBytes(1)).put(", actual=").put(scalar.length).put(']');
        }
        final long header = getLong(scalar, 0);
        final int version = (int) (header & 0xffffL);
        valueKind = (int) ((header >>> 16) & 0xffL);
        scalarWords = (int) ((header >>> 24) & 0xffL);
        headOffset = (int) (header >>> 32);
        if (version != FORMAT_VERSION) {
            throw invalid("RANGE ring state format version mismatch")
                    .put(" [expected=").put(FORMAT_VERSION).put(", actual=").put(version).put(']');
        }
        if (!isValueKindValid(valueKind) || !isScalarWordsValid(scalarWords)) {
            throw invalid("RANGE ring value kind or scalar width invalid")
                    .put(" [valueKind=").put(valueKind).put(", scalarWords=").put(scalarWords).put(']');
        }
        if (scalar.length != scalarStateBytes(scalarWords)) {
            throw invalid("RANGE ring scalar state size mismatch")
                    .put(" [expected=").put(scalarStateBytes(scalarWords))
                    .put(", actual=").put(scalar.length).put(']');
        }
        rowCount = getLong(scalar, Long.BYTES);
        scalarWord0 = getLong(scalar, 2 * Long.BYTES);
        scalarWord1 = scalarWords > 1 ? getLong(scalar, 3 * Long.BYTES) : 0;
        scalarWord2 = scalarWords > 2 ? getLong(scalar, 4 * Long.BYTES) : 0;
        scalarWord3 = scalarWords > 3 ? getLong(scalar, 5 * Long.BYTES) : 0;
        frameSize = getLong(scalar, (2 + scalarWords) * Long.BYTES);
        lastTimestamp = getLong(scalar, (3 + scalarWords) * Long.BYTES);

        final int refCount = entry.getStatePageCount();
        final int pagesPerChunk = pagesPerChunk(valueKind);
        if (refCount % pagesPerChunk != 0 || refCount > LiveViewCheckpointMetadata.MAX_STATE_PAGE_REFS) {
            throw invalid("RANGE ring state page reference count invalid, count=").put(refCount);
        }
        statePageRefs = new LiveViewCheckpointStatePageRef[refCount];
        long physicalRows = 0;
        for (int i = 0; i < refCount; i += pagesPerChunk) {
            final LiveViewCheckpointStatePageRef timestampRef = entry.getStatePageRef(i);
            validateTimestampRef(timestampRef, valueKind);
            if (pagesPerChunk > 1) {
                final LiveViewCheckpointStatePageRef valueRef = entry.getStatePageRef(i + 1);
                validateValueRef(valueRef, valueKind);
                if (timestampRef.getRowCount() != valueRef.getRowCount()) {
                    throw invalid("RANGE ring chunk stream row counts differ")
                            .put(" [timestamps=").put(timestampRef.getRowCount())
                            .put(", values=").put(valueRef.getRowCount()).put(']');
                }
                statePageRefs[i + 1] = LiveViewCheckpointPartitionMapEntry.copyRef(valueRef);
            }
            if (physicalRows > Long.MAX_VALUE - timestampRef.getRowCount()) {
                throw invalid("RANGE ring physical row count overflow");
            }
            physicalRows += timestampRef.getRowCount();
            statePageRefs[i] = LiveViewCheckpointPartitionMapEntry.copyRef(timestampRef);
        }
        // frameSize is the function's own aggregate cardinality, not a ring index:
        // a frame whose low bound is unbounded folds rows into the aggregate and
        // then drops them from the ring, so it counts rows the ring no longer
        // holds. Only its sign is structural here.
        if (rowCount < 0 || frameSize < 0) {
            throw invalid("RANGE ring scalar row counts invalid")
                    .put(" [rowCount=").put(rowCount).put(", frameSize=").put(frameSize).put(']');
        }
        if (rowCount == 0) {
            if (refCount != 0 || headOffset != 0 || lastTimestamp != 0) {
                throw invalid("RANGE ring empty state is not canonical");
            }
        } else if (refCount == 0 || headOffset < 0
                || headOffset >= statePageRefs[0].getRowCount()
                || physicalRows - headOffset != rowCount) {
            throw invalid("RANGE ring logical chunk bounds invalid")
                    .put(" [physicalRows=").put(physicalRows)
                    .put(", headOffset=").put(headOffset)
                    .put(", rowCount=").put(rowCount).put(']');
        }
        initialized = true;
    }

    static void copyRef(LiveViewCheckpointStatePageRef from, LiveViewCheckpointStatePageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getStoredLength(), from.getDecodedLength(),
                from.getPageKind(), from.getCodec(), from.getRowCount(), from.getFlags());
    }

    static byte[] encodeScalar(
            int valueKind,
            int scalarWords,
            int headOffset,
            long rowCount,
            long scalarWord0,
            long scalarWord1,
            long scalarWord2,
            long scalarWord3,
            long frameSize,
            long lastTimestamp
    ) {
        validateScalarWords(scalarWords);
        final byte[] scalar = new byte[scalarStateBytes(scalarWords)];
        putLong(scalar, 0, ((long) headOffset << 32)
                | ((long) (scalarWords & 0xff) << 24)
                | ((long) (valueKind & 0xff) << 16)
                | (FORMAT_VERSION & 0xffffL));
        putLong(scalar, Long.BYTES, rowCount);
        putLong(scalar, 2 * Long.BYTES, scalarWord0);
        if (scalarWords > 1) {
            putLong(scalar, 3 * Long.BYTES, scalarWord1);
        }
        if (scalarWords > 2) {
            putLong(scalar, 4 * Long.BYTES, scalarWord2);
            putLong(scalar, 5 * Long.BYTES, scalarWord3);
        }
        putLong(scalar, (2 + scalarWords) * Long.BYTES, frameSize);
        putLong(scalar, (3 + scalarWords) * Long.BYTES, lastTimestamp);
        return scalar;
    }

    /**
     * @return whether {@code valueKind}'s value column stores a raw 64-bit payload
     * (LONG/DATE/TIMESTAMP or DECIMAL, ring or deque) rather than exact IEEE-754
     * double bits
     */
    static boolean isLongColumn(int valueKind) {
        return valueKind != VALUE_KIND_DOUBLE && valueKind != VALUE_KIND_DEQUE_DOUBLE;
    }

    /**
     * @return the state page references one logical chunk of a {@code valueKind} ring
     * spends: a timestamp page and a value page, or the timestamp page alone when the
     * ring stores no value
     */
    static int pagesPerChunk(int valueKind) {
        return valueWords(valueKind) == 0 ? 1 : 2;
    }

    /**
     * @return the value page kind {@code valueKind} writes: the value-ring kinds for
     * {@code avg}/{@code sum}/{@code first_value}/{@code last_value}/{@code nth_value},
     * the deque kinds for a {@code max}/{@code min} frame ring. A valueless ring writes
     * no value page, so asking for its kind is a wiring error
     */
    static int valuePageKind(int valueKind) {
        switch (valueKind) {
            case VALUE_KIND_DOUBLE:
                return DOUBLE_VALUE_PAGE_KIND;
            case VALUE_KIND_LONG:
                return LONG_VALUE_PAGE_KIND;
            case VALUE_KIND_DEQUE_DOUBLE:
                return DEQUE_DOUBLE_VALUE_PAGE_KIND;
            case VALUE_KIND_DEQUE_LONG:
                return DEQUE_LONG_VALUE_PAGE_KIND;
            case VALUE_KIND_DECIMAL128:
                return DECIMAL128_VALUE_PAGE_KIND;
            case VALUE_KIND_DECIMAL256:
                return DECIMAL256_VALUE_PAGE_KIND;
            case VALUE_KIND_DEQUE_DECIMAL128:
                return DEQUE_DECIMAL128_VALUE_PAGE_KIND;
            case VALUE_KIND_DEQUE_DECIMAL256:
                return DEQUE_DECIMAL256_VALUE_PAGE_KIND;
            default:
                throw CairoException.critical(0)
                        .put("live view checkpoint RANGE ring value kind invalid [kind=").put(valueKind).put(']');
        }
    }

    private static long getLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value |= (long) (bytes[offset + i] & 0xff) << (i * 8);
        }
        return value;
    }

    private static CairoException invalid(CharSequence reason) {
        return LiveViewCheckpointMetadata.invalid(reason);
    }

    private static boolean isScalarWordsValid(int scalarWords) {
        return scalarWords == 1 || scalarWords == 2 || scalarWords == 4;
    }

    private static boolean isValueKindValid(int valueKind) {
        return (valueKind >= VALUE_KIND_DOUBLE && valueKind <= VALUE_KIND_DEQUE_DECIMAL256)
                || valueKind == VALUE_KIND_NONE;
    }

    private static void putLong(byte[] bytes, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            bytes[offset + i] = (byte) (value >>> (i * 8));
        }
    }

    private static void validateCommonRef(LiveViewCheckpointStatePageRef ref, int valueKind, int wordsPerRow) {
        final int rows = ref.getRowCount();
        if (rows <= 0 || rows > maxChunkRows(valueKind)
                || ref.getDecodedLength() != rows * wordsPerRow * Long.BYTES || ref.getFlags() != FLAGS) {
            throw invalid("RANGE ring state page bounds invalid")
                    .put(" [rows=").put(rows)
                    .put(", decodedLength=").put(ref.getDecodedLength())
                    .put(", flags=").put(ref.getFlags()).put(']');
        }
    }

    private static void validateScalarWords(int scalarWords) {
        if (!isScalarWordsValid(scalarWords)) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring scalar width invalid [words=").put(scalarWords).put(']');
        }
    }

    private static void validateTimestampRef(LiveViewCheckpointStatePageRef ref, int valueKind) {
        LiveViewCheckpointMetadata.validateStateRef(ref, false, "RANGE ring timestamp chunk");
        if (ref.getPageKind() != TIMESTAMP_PAGE_KIND
                || (ref.getCodec() != LiveViewCheckpointStateCodec.TIMESTAMP_RAW_64
                && ref.getCodec() != LiveViewCheckpointStateCodec.TIMESTAMP_DELTA_OF_DELTA_VARINT)) {
            throw invalid("RANGE ring timestamp page kind or codec invalid")
                    .put(" [kind=").put(ref.getPageKind()).put(", codec=").put(ref.getCodec()).put(']');
        }
        validateCommonRef(ref, valueKind, 1);
    }

    private static void validateValueRef(LiveViewCheckpointStatePageRef ref, int valueKind) {
        LiveViewCheckpointMetadata.validateStateRef(ref, false, "RANGE ring value chunk");
        final int expectedPageKind = valuePageKind(valueKind);
        final boolean isKindValid = isLongColumn(valueKind)
                ? ref.getPageKind() == expectedPageKind
                  && ref.getCodec() == LiveViewCheckpointStateCodec.LONG_RAW_64
                : ref.getPageKind() == expectedPageKind
                  && (ref.getCodec() == LiveViewCheckpointStateCodec.DOUBLE_RAW_64
                      || ref.getCodec() == LiveViewCheckpointStateCodec.DOUBLE_XOR);
        if (!isKindValid) {
            throw invalid("RANGE ring value page kind or codec invalid")
                    .put(" [valueKind=").put(valueKind).put(", kind=").put(ref.getPageKind())
                    .put(", codec=").put(ref.getCodec()).put(']');
        }
        validateCommonRef(ref, valueKind, valueWords(valueKind));
    }

    LiveViewCheckpointStatePageRef[] copyStatePageRefs() {
        final LiveViewCheckpointStatePageRef[] copy = new LiveViewCheckpointStatePageRef[statePageRefs.length];
        for (int i = 0; i < statePageRefs.length; i++) {
            copy[i] = LiveViewCheckpointPartitionMapEntry.copyRef(statePageRefs[i]);
        }
        return copy;
    }

    int decodeChunk(int chunkIndex, long timestampAddress, long valueAddress) {
        final int pagesPerChunk = pagesPerChunk(valueKind);
        final LiveViewCheckpointStatePageRef timestampRef = statePageRefs[chunkIndex * pagesPerChunk];
        decodeTimestamps(timestampRef, timestampAddress);
        if (pagesPerChunk > 1) {
            decodeValues(statePageRefs[chunkIndex * pagesPerChunk + 1], valueAddress);
        }
        return timestampRef.getRowCount();
    }

    /**
     * Maps {@code segmentId}, or returns the cached mapping of it. The slot a
     * segment id lands in is fixed, so a slot holding another id re-opens: see
     * {@link #DATA_SEGMENT_CACHE_SIZE} for why that is the right trade here.
     */
    private LiveViewCheckpointDataSegmentReader dataSegmentReader(long segmentId, long fileLength) {
        final int slot = (int) (segmentId & (DATA_SEGMENT_CACHE_SIZE - 1));
        LiveViewCheckpointDataSegmentReader reader = dataReaders[slot];
        if (reader == null) {
            reader = new LiveViewCheckpointDataSegmentReader(configuration);
            dataReaders[slot] = reader;
        } else if (dataSegmentIds[slot] == segmentId) {
            return reader;
        }
        // A failed open leaves the slot mapping nothing, so clear the id before it
        // rather than let a throw strand a slot that claims a segment it lost.
        dataSegmentIds[slot] = -1;
        reader.of(checkpointsDir, segmentId, fileLength);
        dataSegmentIds[slot] = segmentId;
        return reader;
    }

    private void decodeTimestamps(LiveViewCheckpointStatePageRef ref, long targetAddress) {
        openPage(ref, TIMESTAMP_PAGE_KIND);
        final int consumed = LiveViewCheckpointStateCodec.decodeTimestamps(
                openReader.getPageAddress(), openReader.getPageStoredLength(), ref.getCodec(), ref.getRowCount(),
                targetAddress, LiveViewCheckpointStateCodec.CHUNK_ROWS
        );
        openReader.assertFullyConsumed(consumed, ref.getDecodedLength(), ref.getRowCount());
    }

    private void decodeValues(LiveViewCheckpointStatePageRef ref, long targetAddress) {
        openPage(ref, valuePageKind(valueKind));
        // A wide value spends several 64-bit words per row, so the codec decodes
        // rows*words elements into one scratch buffer the chunk row cap keeps inside
        // CHUNK_ROWS words.
        final int words = ref.getRowCount() * valueWords(valueKind);
        final int consumed;
        if (isLongColumn(valueKind)) {
            consumed = LiveViewCheckpointStateCodec.decodeLongs(
                    openReader.getPageAddress(), openReader.getPageStoredLength(), ref.getCodec(), words,
                    targetAddress, LiveViewCheckpointStateCodec.CHUNK_ROWS
            );
        } else {
            consumed = LiveViewCheckpointStateCodec.decodeDoubles(
                    openReader.getPageAddress(), openReader.getPageStoredLength(), ref.getCodec(), words,
                    targetAddress, LiveViewCheckpointStateCodec.CHUNK_ROWS
            );
        }
        openReader.assertFullyConsumed(consumed, ref.getDecodedLength(), ref.getRowCount());
    }

    private void ensureBound() {
        if (segmentDirectory == null) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring state reader is not bound to a data segment directory");
        }
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint RANGE ring state reader is not initialized");
        }
    }

    private void openPage(LiveViewCheckpointStatePageRef ref, int pageKind) {
        final long fileLength;
        try {
            fileLength = segmentDirectory.getFileLength(ref.getSegmentId());
        } catch (CairoException e) {
            throw invalid("RANGE ring page references unknown data segment, segmentId=").put(ref.getSegmentId());
        }
        openReader = dataSegmentReader(ref.getSegmentId(), fileLength);
        openReader.openPage(
                ref,
                pageKind,
                ref.getCodec(),
                FLAGS,
                LiveViewCheckpointStateCodec.CHUNK_ROWS,
                LiveViewCheckpointStateCodec.CHUNK_ROWS * Long.BYTES
        );
    }

    /**
     * Replays every live row through whichever consumer matches the ring's value
     * width. A function always reads the width it sealed under, so a mismatch means
     * the caller wired the ring to the wrong function rather than that a page is
     * corrupt.
     */
    private void walk(
            @Nullable TimestampConsumer timestamps,
            @Nullable RowConsumer narrow,
            @Nullable Decimal128RowConsumer decimal128,
            @Nullable Decimal256RowConsumer decimal256,
            int expectedWords
    ) {
        ensureInitialized();
        ensureBound();
        final int words = valueWords(valueKind);
        if (words != expectedWords) {
            throw CairoException.critical(0)
                    .put("live view checkpoint RANGE ring value width mismatch")
                    .put(" [expected=").put(expectedWords).put(", actual=").put(words).put(']');
        }
        long rowsRead = 0;
        long previousTimestamp = 0;
        boolean hasPrevious = false;
        for (int chunk = 0, n = statePageRefs.length / pagesPerChunk(valueKind); chunk < n; chunk++) {
            final int physicalRows = decodeChunk(chunk, scratch.timestampsAddress(), scratch.valuesAddress());
            final int lo = chunk == 0 ? headOffset : 0;
            for (int i = 0; i < physicalRows; i++) {
                final long timestamp = Unsafe.getLong(scratch.timestampsAddress() + (long) i * Long.BYTES);
                // Timestamps must not decrease. Values are not checked for
                // finiteness: NaN is a legitimate first_value/last_value/nth_value
                // over a frame whose oldest/newest row is NULL. avg/sum, whose ring
                // is finite by construction, re-assert that in their restore
                // consumer.
                if (hasPrevious && timestamp < previousTimestamp) {
                    throw invalid("RANGE ring chunk rows are not canonical")
                            .put(" [chunk=").put(chunk).put(", row=").put(i).put(']');
                }
                previousTimestamp = timestamp;
                hasPrevious = true;
                if (i >= lo) {
                    if (timestamps != null) {
                        // A valueless ring decoded no value page; the row is its timestamp.
                        timestamps.accept(timestamp);
                        rowsRead++;
                        continue;
                    }
                    // The value travels as raw 64-bit words, whatever the ring's value
                    // kind; the function reinterprets them.
                    final long base = scratch.valuesAddress() + (long) i * words * Long.BYTES;
                    if (narrow != null) {
                        narrow.accept(timestamp, Unsafe.getLong(base));
                    } else if (decimal128 != null) {
                        decimal128.accept(timestamp, Unsafe.getLong(base), Unsafe.getLong(base + Long.BYTES));
                    } else {
                        decimal256.accept(
                                timestamp,
                                Unsafe.getLong(base),
                                Unsafe.getLong(base + Long.BYTES),
                                Unsafe.getLong(base + 2 * Long.BYTES),
                                Unsafe.getLong(base + 3 * Long.BYTES)
                        );
                    }
                    rowsRead++;
                }
            }
        }
        if (rowsRead != rowCount || (rowCount > 0 && (!hasPrevious || previousTimestamp != lastTimestamp))) {
            throw invalid("RANGE ring scalar/page bounds mismatch")
                    .put(" [decodedRows=").put(rowsRead)
                    .put(", expectedRows=").put(rowCount)
                    .put(", decodedLastTimestamp=").put(previousTimestamp)
                    .put(", expectedLastTimestamp=").put(lastTimestamp).put(']');
        }
    }
}
