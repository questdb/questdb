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

package io.questdb.cairo.idx;

import org.jetbrains.annotations.TestOnly;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectBitSet;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.BinarySequence;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads a covering index that was sealed into a partition's
 * {@code <col>.pidx.<indexTxn>.parquet} plus its {@code <col>.pidx.<indexTxn>._im}
 * sidecar, rather than into the native {@code .pk} / {@code .pv} / {@code .pc*}
 * chain that {@link AbstractPostingIndexReader} serves.
 * <p>
 * The artifact pair is named by an {@code index_txn}, which the nine-argument
 * {@link IndexReader#of} does not carry -- so this reader is bound through
 * {@link #ofParquet} instead, off the token
 * {@code TableReader.getPartitionIndexForm} resolved from the partition's own
 * {@code _pm} mapping at partition-open time. {@link #of} therefore throws.
 * <p>
 * <b>This class answers structure only.</b> The four {@link PostingIndexReader}
 * primitives return their "cannot answer, walk a cursor" sentinels and
 * {@link #getCursor} throws; Phase 2C Task 4 fills them in. The throw is
 * deliberate: an empty cursor here would turn every indexed query over a
 * parquet-sealed partition into a silent empty result, which is exactly the
 * failure the refusal this dispatch replaces existed to prevent.
 */
public abstract class AbstractParquetPostingIndexReader implements PostingIndexReader {
    private static final Log LOG = LogFactory.getLog(AbstractParquetPostingIndexReader.class);
    /**
     * Appended to every message that reports a damaged or unreadable index
     * artifact. These are operator-facing: the query cannot proceed, and the
     * only thing that fixes it is rebuilding the index or taking the partition
     * back to the native form. Saying so in the message is the difference
     * between an incident and a support ticket.
     */
    private static final String RECOVERY_HINT =
            "; rebuild it with ALTER TABLE <table> ALTER COLUMN <column> DROP INDEX"
                    + " then ADD INDEX TYPE POSTING, or take the partition back to native with"
                    + " ALTER TABLE <table> CONVERT PARTITION TO NATIVE LIST '<partition>'";
    protected final IndexMetaFileReader imReader = new IndexMetaFileReader();
    /**
     * The one parsed footer of {@code <col>.pidx.<indexTxn>.parquet}, which
     * every cursor copies rather than re-parsing.
     * <p>
     * A cursor's own {@code of(addr, size)} walks the whole footer: one thrift
     * {@code ColumnChunk} per column per row group. That is affordable once,
     * but a cursor is built per QUERY, so an aggregate over a single key paid
     * it per query -- and profiling a short SQL query showed the thrift
     * ColumnChunk parse above LZ4 decompression. Bound EAGERLY here rather than
     * lazily in the cursor: {@code getDetachedCursor} hands N workers N cursors
     * over one frozen reader, and a lazy shared init is a race on that path.
     */
    protected final ParquetFileDecoder sourceDecoder = new ParquetFileDecoder();
    /**
     * Per row group, the absolute offset of {@code row_id}'s values in the
     * mapping, {@code -1} where they cannot be read directly, and
     * {@code Long.MIN_VALUE} where it has not been asked yet.
     * <p>
     * Resolved lazily and once: the answer is a property of the file, and asking
     * costs a page-header parse -- which is exactly what the answer lets every
     * later lookup skip.
     */
    private long[] rowIdDataOffsets;
    /**
     * Per row group under {@code PAYLOAD_KIND 1}: the address of the group's
     * packed row-id values, the base subtracted from them and the bits each
     * occupies. Resolved together, lazily and once, for the same reason
     * {@link #rowIdDataOffsets} is: the answer is a property of the file.
     * <p>
     * {@code packedDataAddrs} holds 0 where the group's blob cannot be
     * addressed directly and {@code Long.MIN_VALUE} where it has not been asked
     * yet -- a live address is never either.
     */
    private long[] packedDataAddrs;
    /** Blob start where the group carries one base per KEY, 0 where it carries one per group. */
    private long[] packedBlobAddrs;
    /** Per (row group, cover slot) covered-blob data address under the packed arm. */
    private long[] coverBlobAddrs;
    private long[] packedBases;
    private int[] packedBitWidths;
    /** True when the bound file stores a blob per row group rather than a row id per posting. */
    protected boolean packedPayload;
    /**
     * Upper bound on how many row ids are widened per native call while walking
     * a packed run. The batch STARTS at {@link #PACKED_WIDEN_BATCH_MIN} and
     * doubles up to this, which is what lets one constant serve two workloads
     * that want opposite things.
     * <p>
     * A fixed small batch is right for a partial read: taking one row of a key
     * should not widen the key. Measured at P400K, widening whole runs cost
     * 5,498 ops/s against 22,301 for a 64-row batch -- 4x, for rows the caller
     * never asked for.
     * <p>
     * A fixed small batch is wrong for a full drain, because the per-batch cost
     * is a native call and 64-row batches make 391 of them per key where one
     * would do. Measured at P400K on the same cell, a full drain ran 2,414 ops/s
     * batched against 3,013 whole-run.
     * <p>
     * Doubling settles both: a first-row read widens 64, and a 25,000-row drain
     * reaches the end in nine calls rather than 391. No tuning constant has to
     * be right, which matters because the machine these figures came from had
     * competing load and the fixed-size sweep was not resolvable against it.
     */
    protected static final int PACKED_WIDEN_BATCH =
            Integer.getInteger("questdb.idx.packed.batch", 1 << 16);
    /** First batch of a run. Small, so a partial read stays cheap. */
    protected static final int PACKED_WIDEN_BATCH_MIN =
            Integer.getInteger("questdb.idx.packed.batch.min", 64);
    protected long columnTop;
    /**
     * Pruning instrumentation, and shared by every cursor this reader serves.
     * <p>
     * Atomic because {@code getDetachedCursor} hands N workers N cursors over
     * ONE reader and they decode concurrently: a plain {@code long++} is a
     * read-modify-write, so a lost update UNDER-reports the decode. That
     * direction matters -- these two counters are exactly what the pruning
     * assertions read, so a lost update makes a pruning test pass by losing the
     * evidence rather than by pruning.
     */
    protected final AtomicLong decodedRowCount = new AtomicLong();
    protected final AtomicLong decodedRowGroupCount = new AtomicLong();
    protected long indexTxn = -1;
    protected long partitionTimestamp;
    protected long pidxAddr;
    protected long pidxSize;
    private long columnNameTxn = -1;
    protected CharSequence columnName;
    private FilesFacade ff;
    private boolean frozen;
    private long imFileSize;
    private boolean open;
    private long partitionTxn = -1;
    private long pinnedTableTxn = Long.MAX_VALUE;

    @Override
    public void close() {
        open = false;
        // Before the munmap below: the decoder's parsed footer addresses the
        // mapping it was built over.
        sourceDecoder.close();
        rowIdDataOffsets = null;
        packedDataAddrs = null;
        packedBlobAddrs = null;
        coverBlobAddrs = null;
        packedBases = null;
        packedBitWidths = null;
        if (pidxAddr != 0) {
            ff.munmap(pidxAddr, pidxSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            pidxAddr = 0;
            pidxSize = 0;
        }
        // Releases the _im mapping this reader owns; safe on a reader that was
        // never bound and safe to repeat.
        imReader.clear();
    }

    /**
     * Row groups this reader has actually decoded since it was bound. Pruning
     * is asserted against this rather than against a duration: a latency
     * assertion passes on warm-up while the skip misses entirely.
     */
    public long getDecodedRowGroupCount() {
        return decodedRowGroupCount.get();
    }

    /**
     * Rows whose VALUES this reader has decoded -- {@code row_id} and any
     * covered columns -- since it was bound. Pruning level 3 is asserted on
     * this rather than on the row-group count, because narrowing inside a
     * packed group leaves the group count unchanged.
     * <p>
     * The {@code key_id} probe that finds the key's range is not counted: it
     * reads one 4-byte column, which is what buys the narrowing, and counting
     * it would make the metric measure the probe rather than the saving.
     */
    public long getDecodedRowCount() {
        return decodedRowCount.get();
    }

    /**
     * First index in {@code [lo, hi)} whose row id is at or above
     * {@code value}, or {@code hi}.
     * <p>
     * A key's run is ascending whichever way a cursor walks it, so both
     * directions narrow a window the same way. Shared here rather than copied
     * into each cursor: the backward reader went without these for a while and
     * decoded a row group per key as a result.
     */
    protected static long seekFirstAtLeast(long rowIdPtr, long lo, long hi, long value) {
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (Unsafe.getUnsafe().getLong(rowIdPtr + (mid << 3)) < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** First index in {@code [lo, hi)} whose row id exceeds {@code value}, or {@code hi}. */
    protected static long seekFirstAbove(long rowIdPtr, long lo, long hi, long value) {
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (Unsafe.getUnsafe().getLong(rowIdPtr + (mid << 3)) <= value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * {@link #seekFirstAtLeast} against PACKED values, so a window can be
     * narrowed before anything is widened.
     * <p>
     * Widening first and narrowing after is what the packed cursors did
     * originally, and it makes a windowed read widen a whole key run to throw
     * most of it away -- at 2,000 keys over 2M rows that is 1,000 values
     * widened per key however few the window admits. The native chain does not:
     * it binary searches the packed data with single-value unpacks and only
     * then widens the range it settled on. This is that search.
     */
    protected static long packedSeekFirstAtLeast(long block, long lo, long hi, long value) {
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (CoveringCompressor.readLongAt(block, (int) mid) < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** {@link #seekFirstAbove} against PACKED values. See {@link #packedSeekFirstAtLeast}. */
    protected static long packedSeekFirstAbove(long block, long lo, long hi, long value) {
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (CoveringCompressor.readLongAt(block, (int) mid) <= value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Offset of {@code row_id}'s values in {@code rowGroup}, or -1 when the
     * chunk is not a single uncompressed PLAIN page and so has to be decoded.
     */
    protected long rowIdDataOffset(int rowGroup) {
        if (rowIdDataOffsets == null) {
            final int groups = imReader.getIndexRowGroupCount();
            rowIdDataOffsets = new long[Math.max(groups, 1)];
            Arrays.fill(rowIdDataOffsets, Long.MIN_VALUE);
        }
        if (rowGroup < 0 || rowGroup >= rowIdDataOffsets.length) {
            return -1;
        }
        long offset = rowIdDataOffsets[rowGroup];
        if (offset == Long.MIN_VALUE) {
            offset = sourceDecoder.plainColumnDataOffset(rowGroup, imReader.getRowIdColumn());
            rowIdDataOffsets[rowGroup] = offset;
        }
        return offset;
    }

    /**
     * Binds {@code rowGroup}'s packed row-id blob, returning the address of its
     * packed values, or 0 when the blob cannot be addressed in the mapping and
     * the group has to be decoded.
     * <p>
     * After a non-zero return {@link #packedBase(int)} and
     * {@link #packedBitWidth(int)} answer for the same group. All three come
     * out of the blob's own header rather than the {@code _im}, so a mismatched
     * or corrupt {@code _im} cannot silently make the payload decode as
     * different row ids.
     */
    protected long packedDataAddr(int rowGroup) {
        if (packedDataAddrs == null) {
            final int groups = Math.max(imReader.getIndexRowGroupCount(), 1);
            packedDataAddrs = new long[groups];
            packedBlobAddrs = new long[groups];
            packedBases = new long[groups];
            packedBitWidths = new int[groups];
            Arrays.fill(packedDataAddrs, Long.MIN_VALUE);
        }
        if (rowGroup < 0 || rowGroup >= packedDataAddrs.length) {
            return 0;
        }
        long dataAddr = packedDataAddrs[rowGroup];
        if (dataAddr == Long.MIN_VALUE) {
            dataAddr = 0;
            final long pageOffset = sourceDecoder.plainColumnDataOffset(rowGroup, imReader.getRowIdBlobColumn());
            if (pageOffset >= 0) {
                // The group holds exactly one parquet row, so its blob is the
                // only value in the page: a PLAIN BYTE_ARRAY value is a 4-byte
                // little-endian length then the bytes.
                final long blob = pidxAddr + pageOffset + Integer.BYTES;
                final byte mode = Unsafe.getUnsafe().getByte(blob);
                if (mode == PostingIndexUtils.PACKED_MODE_PER_KEY_BLOCKS
                        || mode == PostingIndexUtils.PACKED_MODE_PER_KEY_UNIFORM) {
                    // One linear-prediction block per key. The blob start is
                    // what a key's block is resolved from; there is no
                    // group-wide packed array to point at.
                    packedBlobAddrs[rowGroup] = blob;
                    dataAddr = blob;
                } else if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                    packedBlobAddrs[rowGroup] = 0;
                    packedBases[rowGroup] = Unsafe.getUnsafe().getLong(blob + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                    packedBitWidths[rowGroup] = Unsafe.getUnsafe().getByte(blob + 1) & 0xFF;
                    dataAddr = blob + PostingIndexUtils.PACKED_PAYLOAD_HEADER_SIZE;
                } else if (mode == PostingIndexUtils.PACKED_MODE_PER_KEY) {
                    // The base is per KEY here, so it cannot be resolved until
                    // the caller says which key. The blob address is kept for
                    // that lookup; packedBases holds the group base the per-key
                    // deltas are relative to, which is what a caller that asks
                    // without a key would otherwise silently get wrong -- so
                    // packedBase(int) refuses instead.
                    final int keySpan = Unsafe.getUnsafe().getInt(blob + PostingIndexUtils.PACKED_PER_KEY_SPAN_OFFSET);
                    packedBlobAddrs[rowGroup] = blob;
                    packedBases[rowGroup] = Unsafe.getUnsafe().getLong(blob + PostingIndexUtils.PACKED_PER_KEY_BASE_OFFSET);
                    packedBitWidths[rowGroup] = Unsafe.getUnsafe().getByte(blob + 1) & 0xFF;
                    dataAddr = blob + PostingIndexUtils.packedPerKeyHeaderSize(keySpan);
                }
            }
            packedDataAddrs[rowGroup] = dataAddr;
        }
        return dataAddr;
    }

    /**
     * Address of {@code slot}'s covered values for {@code rowGroup} under the
     * packed arm, or 0 when the blob cannot be addressed in the mapping.
     * <p>
     * Cached per (group, slot) for the same reason the row-id blob is: the
     * answer is a property of the file, and asking costs a page-header parse.
     */
    protected long coverBlobDataAddr(int rowGroup, int slot) {
        final int groups = Math.max(imReader.getIndexRowGroupCount(), 1);
        final int slots = Math.max(imReader.getColumnCount() - imReader.getFirstCoverColumn(), 1);
        if (coverBlobAddrs == null) {
            coverBlobAddrs = new long[groups * slots];
            Arrays.fill(coverBlobAddrs, Long.MIN_VALUE);
        }
        final int at = rowGroup * slots + slot;
        long addr = coverBlobAddrs[at];
        if (addr == Long.MIN_VALUE) {
            addr = 0;
            final long pageOffset = sourceDecoder.plainColumnDataOffset(rowGroup, imReader.getCoverColumnIndex(slot));
            if (pageOffset >= 0) {
                // One row per group, so the group's blob is the only value in
                // the page: a PLAIN BYTE_ARRAY value is a 4-byte little-endian
                // length then the bytes.
                // The blob IS the compressed block; CoveringCompressor's
                // readXxxAt decoders address a value by index inside it.
                addr = pidxAddr + pageOffset + Integer.BYTES;
            }
            coverBlobAddrs[at] = addr;
        }
        return addr;
    }

    /**
     * True when {@code rowGroup}'s row ids live in one frame-of-reference array
     * for the whole group rather than in a block per key.
     * <p>
     * The seal picks the layout per group by cost, so a single file holds both:
     * a group of wide keys keeps its per-key blocks, and one of narrow keys --
     * where a 29-byte block header would cost more than the row ids it
     * describes -- goes flat. Callers must ask rather than assume.
     */
    protected boolean isFlatGroup(int rowGroup) {
        // Resolves the mode as a side effect; packedBlobAddrs is only
        // meaningful once the group has been looked at.
        return packedDataAddr(rowGroup) != 0 && packedBlobAddrs[rowGroup] == 0;
    }

    /**
     * The row id at group ordinal {@code ordinal} of a flat group. Valid only
     * where {@link #isFlatGroup(int)} holds.
     */
    protected long flatRowIdAt(int rowGroup, long ordinal) {
        return BitpackUtils.unpackValue(
                packedDataAddrs[rowGroup], (int) ordinal, packedBitWidths[rowGroup], packedBases[rowGroup]);
    }

    /**
     * First group ordinal in {@code [lo, hi)} whose row id is at or above
     * {@code value}, or {@code hi}. Ordinals are GROUP-relative, matching the
     * {@code _im} directory that produced the bounds.
     */
    protected long flatSeekFirstAtLeast(int rowGroup, long lo, long hi, long value) {
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (flatRowIdAt(rowGroup, mid) < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** First group ordinal in {@code [lo, hi)} whose row id exceeds {@code value}, or {@code hi}. */
    protected long flatSeekFirstAbove(int rowGroup, long lo, long hi, long value) {
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (flatRowIdAt(rowGroup, mid) <= value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * The linear-prediction block holding {@code key}'s row ids in
     * {@code rowGroup}, or 0 when the key holds none or the group is flat.
     * <p>
     * Returns 0 rather than a block for a flat group DELIBERATELY. A flat
     * group's cached address is its packed array, not a blob, so reading a
     * per-key offset out of it yields a wild pointer -- three call sites that
     * missed the layout branch turned into a SIGSEGV rather than an error. 0
     * puts them on the "block is absent" path instead, which throws with the
     * group and key in the message.
     */
    protected long rowIdBlock(int rowGroup, int key) {
        final long blob = packedDataAddr(rowGroup);
        if (blob == 0 || packedBlobAddrs[rowGroup] == 0) {
            return 0;
        }
        final int firstKey = imReader.getRowGroupFirstKey(rowGroup);
        // Uniform blocks are addressed arithmetically, which spares the random
        // load into a 4-byte-per-key offset table -- the load a profile of the
        // 1,000,000-key range read found to be nearly all of its cost.
        if (Unsafe.getUnsafe().getByte(blob) == PostingIndexUtils.PACKED_MODE_PER_KEY_UNIFORM) {
            return PostingIndexUtils.packedUniformBlock(blob, firstKey, key);
        }
        return PostingIndexUtils.coverPerKeyBlock(blob, firstKey, key);
    }

    /**
     * Value subtracted from {@code key}'s row ids before packing. Valid only
     * after {@link #packedDataAddr(int)} has returned non-zero for the group.
     * <p>
     * Takes the key because a blob may carry one base per key rather than one
     * per group -- see {@link PostingIndexUtils#PACKED_MODE_PER_KEY}. Under the
     * per-group mode the key is ignored, so both modes are read the same way and
     * the caller never branches.
     */
    protected long packedBase(int rowGroup, int key) {
        final long blob = packedBlobAddrs[rowGroup];
        if (blob == 0) {
            return packedBases[rowGroup];
        }
        return PostingIndexUtils.packedPerKeyBase(blob, imReader.getRowGroupFirstKey(rowGroup), key);
    }

    /**
     * Bits each of {@code rowGroup}'s packed row ids occupies. Valid only after
     * {@link #packedDataAddr(int)} has returned non-zero for it.
     */
    protected int packedBitWidth(int rowGroup) {
        return packedBitWidths[rowGroup];
    }

    /**
     * Pruning level 2: true when {@code rowGroup}'s row-id extent does not
     * intersect the caller's {@code [minValue, maxValue]}, so the group holds
     * nothing the cursor could emit and need not be decoded.
     * <p>
     * Row id is monotone in the designated timestamp within a partition, so an
     * interval scan's row-id range maps onto this EXACTLY rather than
     * conservatively -- a group is skipped only when it provably holds no row
     * in range.
     * <p>
     * The extents come from the {@code _im}'s own {@code RG_ROW_ID_MIN} /
     * {@code RG_ROW_ID_MAX} sections, not from the {@code row_id} chunk's
     * parquet statistics, because the sections are written unconditionally
     * while that chunk does not exist at all under the alternative payload
     * kind. Reading the stats would silently lose time pruning for that
     * payload rather than failing.
     */
    protected boolean isRowGroupPruned(int rowGroup, long minValue, long maxValue) {
        return imReader.getRowGroupRowIdMin(rowGroup) > maxValue
                || imReader.getRowGroupRowIdMax(rowGroup) < minValue;
    }

    /**
     * Records a decode. Kept next to the pruning predicate so the counter and
     * the skip cannot drift: a group is counted where it is decoded, never
     * where it is merely visited.
     */
    protected void onRowGroupDecoded() {
        decodedRowGroupCount.incrementAndGet();
    }

    /**
     * @param rows rows whose VALUES were decoded, which after level 3 is the
     *             key's slice of the group rather than the whole group.
     */
    protected void onRowGroupDecoded(long rows) {
        decodedRowGroupCount.incrementAndGet();
        decodedRowCount.addAndGet(rows);
    }

    /**
     * Marks every key this partition's index holds, returning how many were
     * NEWLY marked.
     * <p>
     * <b>It must answer, never decline.</b> {@code IndexReader} documents
     * {@code -1} as "not supported, caller falls back to a cursor", but the
     * only caller does {@code foundCount += collectDistinctKeys(foundKeys)},
     * so a {@code -1} does not trigger a fallback -- it silently shortens
     * {@code SELECT DISTINCT} by one per partition.
     * <p>
     * Cheap because only {@code key_id} is projected: every key present is the
     * {@code key_id} of at least one index row, and the groups are key-major
     * so the scan is a run-length walk rather than a set build.
     */
    @Override
    public int collectDistinctKeys(DirectBitSet foundKeys) {
        return collectDistinctKeysInRange(foundKeys, 0, Long.MAX_VALUE);
    }

    /**
     * Marks every key holding at least one posting inside
     * {@code [rowLo, rowHi]}, returning how many were NEWLY marked.
     * <p>
     * <b>Row-group pruning is not enough here.</b> Skipping a group whose extent
     * misses the window is exact, but a group that STRADDLES it holds keys whose
     * own postings all sit outside it, and marking those is a wrong answer
     * rather than a slack one: the caller returns the symbols outright, and the
     * inflated count satisfies its {@code foundCount < totalExpected} scan loop
     * early, so later partitions are never visited. The native reader gates each
     * key on {@code flatKeyHasValueInRange}; this does the same, per key run.
     * <p>
     * A group lying wholly inside the window needs no such test, so it keeps the
     * cheap {@code key_id}-only decode. That covers {@link #collectDistinctKeys}
     * -- whose range admits everything -- so the full-partition fast path still
     * reads one four-byte column and nothing else.
     *
     * @see #collectDistinctKeys(DirectBitSet)
     */
    /**
     * The number of INDEX row groups in the sealed index parquet -- not the
     * DATA row groups the partition itself has. Exposed so a differential test
     * can prove it compares across a row-group boundary rather than assuming
     * it: the {@code _im}'s size grows with the data row group count too, so
     * size alone cannot tell the two apart.
     */
    @TestOnly
    public int getIndexRowGroupCount() {
        return imReader.getIndexRowGroupCount();
    }

    /**
     * Which payload arm the bound file carries.
     * <p>
     * A test that measures or compares the packed arm has to assert this rather
     * than assume the property took: the seal silently declines the arm for a
     * covering index or a compressing codec, and a test that only set the
     * property would then be comparing the per-posting arm against itself.
     */
    @TestOnly
    public boolean isPackedPayload() {
        return packedPayload;
    }

    /**
     * Number of row groups whose row ids the seal laid out flat rather than as
     * a block per key.
     * <p>
     * Exists so a differential test can PROVE it entered the flat path. The
     * layout is chosen per group by cost, so a fixture of wide keys takes the
     * per-key path throughout and would compare that path against the native
     * oracle while appearing to cover both -- the same way the covered grid
     * silently never reached the flat branch on a 16-key fixture.
     */
    /**
     * Number of row groups whose row-id layout is {@code mode}, one of the
     * {@code PostingIndexUtils} payload modes. For deciding whether a layout
     * the seal can emit is ever actually chosen.
     */
    @TestOnly
    public int rowIdGroupCountByMode(byte mode) {
        final int groups = Math.max(imReader.getIndexRowGroupCount(), 1);
        int n = 0;
        for (int rg = 0; rg < groups; rg++) {
            if (packedDataAddr(rg) == 0) {
                continue;
            }
            // A flat group caches its DATA address, which is past the header
            // the mode byte lives in; the blob start is kept only for the
            // per-key modes, so the flat case is identified by its absence.
            final long blob = packedBlobAddrs[rg];
            final byte actual = blob == 0
                    ? PostingIndexUtils.STRIDE_MODE_FLAT
                    : Unsafe.getUnsafe().getByte(blob);
            if (actual == mode) {
                n++;
            }
        }
        return n;
    }

    @TestOnly
    public int flatRowIdGroupCount() {
        final int groups = Math.max(imReader.getIndexRowGroupCount(), 1);
        int n = 0;
        for (int rg = 0; rg < groups; rg++) {
            if (isFlatGroup(rg)) {
                n++;
            }
        }
        return n;
    }

    @Override
    public int collectDistinctKeysInRange(DirectBitSet foundKeys, long rowLo, long rowHi) {
        final int groups = imReader.getIndexRowGroupCount();
        if (groups <= 0) {
            return 0;
        }
        int found = 0;
        // The implicit-null prefix is not in the index, so key 0 has to be
        // marked from columnTop rather than from any row. A deliberate
        // divergence from the native reader, which marks key 0 only from a real
        // posting: those rows genuinely are NULL, so this is the more correct of
        // the two. It cannot be observed today -- a parquet-sealed index always
        // carries a zero column top -- and is kept so that the four answers this
        // class gives about the prefix stay consistent with one another.
        if (columnTop > 0 && rowLo < columnTop && !foundKeys.get(0)) {
            foundKeys.set(0);
            found++;
        }
        try (CountingCursor probe = new CountingCursor()) {
            for (int rg = 0; rg < groups; rg++) {
                final long rows = imReader.getRowGroupNumRows(rg);
                if (rows <= 0 || isRowGroupPruned(rg, rowLo, rowHi)) {
                    continue;
                }
                if (isWholeGroupInRange(rg, rowLo, rowHi)) {
                    // Every row of the group is in the window, so every key the
                    // group holds is present -- and the _im directory names
                    // them. No decode: this walks metadata only, which is what
                    // makes SELECT DISTINCT over the index cheap.
                    final int firstKey = imReader.getRowGroupFirstKey(rg);
                    for (int i = 0, span = imReader.getRowGroupKeyCount(rg); i < span; i++) {
                        final int k = firstKey + i;
                        if (imReader.getKeyRowRangeInGroup(rg, k) == IndexMetaFileReader.KEY_ABSENT) {
                            // In the group's key span but holding no row of it.
                            continue;
                        }
                        if (k >= 0 && k < foundKeys.capacity() && !foundKeys.get(k)) {
                            foundKeys.set(k);
                            found++;
                        }
                    }
                    continue;
                }
                if (packedPayload) {
                    // The group straddles the window, so each key's own run has
                    // to be consulted -- but there is no key_id per posting to
                    // walk and no row_id column to decode. The directory names
                    // the group's keys and bounds each one's run, and the blob
                    // holds the ids, so this reaches the same answer from
                    // metadata plus one widen per key.
                    final int firstKey = imReader.getRowGroupFirstKey(rg);
                    for (int i = 0, span = imReader.getRowGroupKeyCount(rg); i < span; i++) {
                        final int k = firstKey + i;
                        if (k < 0 || k >= foundKeys.capacity() || foundKeys.get(k)) {
                            continue;
                        }
                        final long range = imReader.getKeyRowRangeInGroup(rg, k);
                        if (range == IndexMetaFileReader.KEY_ABSENT) {
                            // In the group's key span but holding no row of it.
                            continue;
                        }
                        final int keyLo = Numbers.decodeLowInt(range);
                        final int keyHi = Numbers.decodeHighInt(range);
                        // Key-relative: the block holds only this key's run, so
                        // it starts at 0 whatever the group ordinal says.
                        final long ptr = probe.unpackRowIds(rg, k, 0, keyHi - keyLo);
                        if (keyHasPostingInRange(ptr, 0, keyHi - keyLo, rowLo, rowHi)) {
                            foundKeys.set(k);
                            found++;
                        }
                    }
                    continue;
                }
                // The group straddles the window, so row_id has to be decoded
                // too and each key's own run consulted.
                probe.decodeGroup(rg);
                final long keyIdPtr = probe.rowGroupBuffers.getChunkDataPtr(0);
                final long rowIdPtr = probe.rowGroupBuffers.getChunkDataPtr(1);
                long i = 0;
                while (i < rows) {
                    final int k = Unsafe.getUnsafe().getInt(keyIdPtr + (i << 2));
                    long end = i + 1;
                    while (end < rows && Unsafe.getUnsafe().getInt(keyIdPtr + (end << 2)) == k) {
                        end++;
                    }
                    if (k >= 0 && k < foundKeys.capacity() && !foundKeys.get(k)
                            && keyHasPostingInRange(rowIdPtr, i, end, rowLo, rowHi)) {
                        foundKeys.set(k);
                        found++;
                    }
                    i = end;
                }
            }
        }
        return found;
    }

    /**
     * True when the key run {@code [lo, hi)} of the decoded {@code row_id} chunk
     * holds a posting inside {@code [rowLo, rowHi]}.
     * <p>
     * Exact, not conservative. Comparing the run's first and last id against the
     * window would admit a run that brackets it without meeting it -- the ids
     * ascend but are not consecutive. So this is the same lower-bound search the
     * native {@code flatKeyHasValueInRange} performs: find the first id at or
     * above {@code rowLo} and test it against {@code rowHi}.
     */
    private static boolean keyHasPostingInRange(long rowIdPtr, long lo, long hi, long rowLo, long rowHi) {
        final long end = hi;
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (Unsafe.getUnsafe().getLong(rowIdPtr + (mid << 3)) < rowLo) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < end && Unsafe.getUnsafe().getLong(rowIdPtr + (lo << 3)) <= rowHi;
    }

    /**
     * How many implicit-null rows fall inside {@code [minValue, nullMaxValue]}.
     * <p>
     * Rows before {@code columnTop} carry no value and are not in the index at
     * all, so key 0 (NULL) owns them implicitly. Bounded by
     * {@code nullMaxValue}, the UNCLAMPED caller max, because the prefix is
     * independent of the index and of {@code getEntryMaxValue}.
     * <p>
     * <b>The window's lower bound counts.</b> {@code minValue} is the page
     * frame's {@code rowLo} and is non-zero for every frame that starts
     * mid-partition; the prefix rows below it are outside the caller's window.
     * The caller adds this straight into a {@code count(*)} total, so counting
     * them over-reports rather than degrading.
     * <p>
     * {@code nullMaxValue + 1} is guarded rather than computed: at
     * {@code Long.MAX_VALUE} it wraps to {@code Long.MIN_VALUE}, which
     * {@code Math.min} then picks -- and since {@code Numbers.LONG_NULL} IS
     * {@code Long.MIN_VALUE}, the result leaves the primitive as the "cannot
     * answer" sentinel with no sign that anything overflowed. That is the value
     * production passes for an unbounded window.
     * <p>
     * Mirrors {@code AbstractPostingIndexReader.countMatchesClamped}'s prefix
     * term exactly, including the {@code minValue < columnTop} guard.
     */
    protected long nullPrefixCount(int key, long minValue, long nullMaxValue) {
        if (key != 0 || columnTop <= 0 || minValue >= columnTop || nullMaxValue < 0) {
            return 0;
        }
        final long nullCount = Math.min(
                columnTop,
                nullMaxValue == Long.MAX_VALUE ? Long.MAX_VALUE : nullMaxValue + 1
        );
        return Math.max(0L, nullCount - minValue);
    }

    /**
     * True when every row of {@code rowGroup} belongs to {@code key}.
     * <p>
     * The index is key-major, so within a key's run only the FIRST and LAST
     * groups can be shared with a neighbour -- an interior group cannot hold
     * another key without breaking the run. A single-group run is a boundary
     * group on both sides and is never treated as dedicated.
     */
    protected boolean isGroupDedicatedTo(int rowGroup, int key, int rgLo, int rgHi) {
        return rowGroup > rgLo && rowGroup < rgHi;
    }

    /**
     * True when the group's whole row-id extent sits inside the window, so no
     * row of it can be clipped.
     */
    protected boolean isWholeGroupInRange(int rowGroup, long minValue, long maxValue) {
        return imReader.getRowGroupRowIdMin(rowGroup) >= minValue
                && imReader.getRowGroupRowIdMax(rowGroup) <= maxValue;
    }

    /**
     * A cursor used only to count and to pick, never handed to a caller. It
     * exists so the metadata primitives decode through the same per-cursor
     * state everything else does -- its own decoder, buffers and projection --
     * rather than borrowing the pooled cursor, which a concurrent worker may be
     * iterating.
     */
    protected class CountingCursor extends AbstractCoveringCursor {
        @Override
        public void close() {
            freeResources();
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException("counting cursor is not iterable");
        }

        @Override
        public long next() {
            throw new UnsupportedOperationException("counting cursor is not iterable");
        }

        long countInGroup(int rowGroup, int key, long minValue, long maxValue) {
            final long range = imReader.getKeyRowRangeInGroup(rowGroup, key);
            if (range == IndexMetaFileReader.KEY_ABSENT) {
                return 0;
            }
            final long lo = Numbers.decodeLowInt(range);
            final long hi = Numbers.decodeHighInt(range);
            if (isWholeGroupInRange(rowGroup, minValue, maxValue)) {
                // Every row in the group is inside the caller's window, so
                // every row of the key's run is too, and the directory already
                // says how many that is. The whole answer, with no decode at
                // all -- which is what makes count() over a covering index
                // cheap.
                return hi - lo;
            }
            if (packedPayload) {
                // The run ascends, so the rows inside the window are a
                // contiguous slice of it and its LENGTH is the answer. Two
                // binary searches in the packed domain, and nothing is widened
                // at all -- against widening the whole run and counting it.
                // The block is indexed KEY-RELATIVE, so the directory's group
                // ordinals become a length here. A flat group has no per-key
                // block: its ordinals stay group-relative and the directory's
                // bounds are used as they are.
                if (isFlatGroup(rowGroup)) {
                    final long at = flatSeekFirstAtLeast(rowGroup, lo, hi, minValue);
                    return flatSeekFirstAbove(rowGroup, at, hi, maxValue) - at;
                }
                final long block = rowIdBlock(rowGroup, key);
                final long n = hi - lo;
                final long from = packedSeekFirstAtLeast(block, 0, n, minValue);
                return packedSeekFirstAbove(block, from, n, maxValue) - from;
            }
            // Only the key's own rows, and only row_id: the run is the key's by
            // construction, so nothing here needs key_id to filter on.
            final long rowIdPtr = decodeRowIdRange(rowGroup, key, lo, hi);
            long n = 0;
            for (long i = 0, count = hi - lo; i < count; i++) {
                final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                if (rowId >= minValue && rowId <= maxValue) {
                    n++;
                }
            }
            return n;
        }

        long selectInGroup(int rowGroup, int key, long minValue, long maxValue, long j) {
            final long range = imReader.getKeyRowRangeInGroup(rowGroup, key);
            if (range == IndexMetaFileReader.KEY_ABSENT) {
                return Numbers.LONG_NULL;
            }
            final long lo = Numbers.decodeLowInt(range);
            final long hi = Numbers.decodeHighInt(range);
            if (packedPayload) {
                // The matching rows are a contiguous ascending slice, so the
                // j-th of them is one indexed read at from + j. No scan, and
                // nothing widened but the single value returned.
                if (isFlatGroup(rowGroup)) {
                    final long at0 = flatSeekFirstAtLeast(rowGroup, lo, hi, minValue);
                    final long end = flatSeekFirstAbove(rowGroup, at0, hi, maxValue);
                    final long at = at0 + j;
                    return at < end ? flatRowIdAt(rowGroup, at) : Numbers.LONG_NULL;
                }
                final long block = rowIdBlock(rowGroup, key);
                final long n = hi - lo;
                final long from = packedSeekFirstAtLeast(block, 0, n, minValue);
                final long to = packedSeekFirstAbove(block, from, n, maxValue);
                final long at = from + j;
                return at < to ? CoveringCompressor.readLongAt(block, (int) at) : Numbers.LONG_NULL;
            }
            final long rowIdPtr = decodeRowIdRange(rowGroup, key, lo, hi);
            long seen = 0;
            for (long i = 0, count = hi - lo; i < count; i++) {
                final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                if (rowId < minValue || rowId > maxValue) {
                    continue;
                }
                if (seen++ == j) {
                    return rowId;
                }
            }
            return Numbers.LONG_NULL;
        }

        /**
         * Decodes {@code row_id} alone for {@code [lo, hi)} of {@code rowGroup},
         * returning its chunk pointer.
         * <p>
         * Replaces decoding the whole group's {@code key_id} and {@code row_id}
         * and filtering: since format version 4 the directory gives the key's
         * exact run, so both the wider window and the {@code key_id} column
         * that filtered it are avoidable. With 16 keys to a group that is 16x
         * less to decompress, and decompression is what a lookup costs.
         */
        private long decodeRowIdRange(int rowGroup, int key, long lo, long hi) {
            if (packedPayload) {
                // There is no row_id column to project: the ids are packed
                // inside the group's blob and are widened straight out of it.
                // Deliberately NOT counted through onRowGroupDecoded -- no
                // parquet row group is decoded here, and the counters are what
                // the pruning assertions read, so counting a decode that did
                // not happen would make them assert the wrong thing.
                return unpackRowIds(rowGroup, key, (int) lo, (int) (hi - lo));
            }
            projection.clear();
            projection.add(imReader.getRowIdColumn());
            projection.add(ColumnType.LONG);
            rowGroupBuffers.reopen();
            decoder().decodeRowGroup(rowGroupBuffers, projection, rowGroup, (int) lo, (int) hi);
            onRowGroupDecoded(hi - lo);
            return rowGroupBuffers.getChunkDataPtr(0);
        }

        long decodeKeyIdColumn(int rowGroup, long rows) {
            projection.clear();
            projection.add(imReader.getKeyIdColumn());
            projection.add(ColumnType.INT);
            rowGroupBuffers.reopen();
            decoder().decodeRowGroup(rowGroupBuffers, projection, rowGroup, 0, (int) rows);
            return rowGroupBuffers.getChunkDataPtr(0);
        }

        private long decodeGroup(int rowGroup) {
            final long rows = imReader.getRowGroupNumRows(rowGroup);
            final DirectIntList columns = coveringProjection(null);
            rowGroupBuffers.reopen();
            decoder().decodeRowGroup(rowGroupBuffers, columns, rowGroup, 0, (int) rows);
            onRowGroupDecoded();
            return rows;
        }
    }

    /**
     * Resolves {@code key} to its inclusive index row-group run through the
     * {@code _im} directory. Pruning level 1: exact, and it reads no byte of
     * the index parquet.
     * <p>
     * The directory answers "which row groups COULD hold k", not "does k
     * exist": the key space is dense and occupancy sparse, so a key falling
     * inside a packed group's key range returns a range whether or not it has
     * postings. Confirming absence costs one row-group decode, which the
     * cursor performs anyway.
     */
    protected long rowGroupRangeForKey(int key) {
        return imReader.getRowGroupRangeForKey(key);
    }

    /**
     * The covered-value half of both cursors: everything depending on the
     * decoded chunks and the current row, but not on traversal order.
     * <p>
     * Subclasses own traversal and must call {@link #setEmittedRow(long)} with
     * the index of the row they are about to return, because every accessor
     * reads that row of the decoded group.
     * <p>
     * Only fixed-width covered types are reachable: the seal refuses
     * {@code isVarSize} and symbol covered columns outright, so a covered
     * string, varchar, binary or array cannot exist in an index parquet. Those
     * accessors throw rather than returning null -- an unreachable branch that
     * returns a plausible value is how a silent wrong answer ships if the
     * seal's restriction is ever relaxed without revisiting this class.
     */
    protected abstract class AbstractCoveringCursor implements CoveringRowCursor {
        // Owned per cursor, not per reader. getDetachedCursor hands N workers N
        // cursors over ONE frozen reader, and they decode concurrently: shared
        // buffers would interleave two groups in one allocation, and a shared
        // slot->ordinal map would have each cursor's projection overwrite the
        // other's. Only the _im and parquet mappings are shared, and those are
        // immutable while frozen.
        // The decoder is per-cursor for the same reason the buffers are, and
        // it is the one that bites: ParquetFileDecoder caches a lazily-created
        // native decode context, so N workers decoding through one instance
        // race on it. That does not fail loudly -- it returns another group's
        // rows, which the concurrency test caught as two cursors disagreeing
        // about a posting 15000 rows in.
        protected final ParquetFileDecoder decoder = new ParquetFileDecoder();
        protected final DirectIntList projection = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT);
        protected final RowGroupBuffers rowGroupBuffers =
                new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
        protected int[] coverChunkOrdinal;
        protected long emittedRow = -1;
        /** Row group {@link #emittedRow} is an ordinal within, under the packed arm. */
        protected int packedRowGroup = -1;
        /** Key the emitted row belongs to, which selects its covered block. */
        protected int packedKey = -1;
        /** That key's first group ordinal, so a covered index can be made key-relative. */
        protected long packedKeyStart;
        /**
         * The {@code key_id}-only cursor pruning level 3 binary searches, built
         * on the first row group this cursor bounds.
         * <p>
         * Owned HERE rather than by the subclasses so that
         * {@link #freeResources()} -- the one call a closing READER makes
         * against its pooled cursor -- reaches it. Left to the subclasses it was
         * released only by {@code close()}, which nothing obliges a caller of a
         * pooled cursor to make, and the probe owns a second decoder, its own
         * buffers and its own projection.
         */
        protected CountingCursor keyProbe;
        /**
         * Where a packed group's row ids are widened back to int64 so the rest
         * of the cursor can read them as it always has.
         * <p>
         * Widening a whole key run at bind, rather than one row at a time in
         * {@code hasNext}, is deliberate. The per-row loop is the hot path and
         * an earlier attempt to put a width branch in it regressed four
         * benchmark cells; a batch widen keeps that loop untouched and is what
         * {@link BitpackUtils#unpackValuesFrom}'s AVX2 path exists for. It is
         * NOT a parquet decode -- no page header, no JNI decode context, no
         * copy of the group's other columns -- and it never runs for a
         * per-posting file.
         */
        private long unpackBuf;
        private long unpackBufSize;
        private boolean decoderBound;

        /**
         * Widens {@code count} of {@code rowGroup}'s packed row ids, starting at
         * ordinal {@code from}, and returns the address holding them as int64.
         */
        /**
         * Grows the widen buffer to hold {@code count} row ids.
         * <p>
         * Frees and allocates rather than reallocating, because realloc PRESERVES
         * the old contents and the next widen overwrites every byte of them. That
         * copy measured at 21% of a wide-key scan: the batch doubles from
         * {@link #PACKED_WIDEN_BATCH_MIN} on each key bind, so a 25,000-posting
         * key grew through nine sizes and copied at each one, and
         * {@code indexScanRead} builds a fresh reader per operation so it
         * repeated for every key on every op.
         */
        protected void ensureUnpackCapacity(int count) {
            final long needed = (long) count * Long.BYTES;
            if (needed <= unpackBufSize) {
                return;
            }
            if (unpackBuf != 0) {
                Unsafe.free(unpackBuf, unpackBufSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            }
            unpackBuf = Unsafe.malloc(needed, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            unpackBufSize = needed;
        }

        protected long unpackRowIds(int rowGroup, int key, int from, int count) {
            if (isFlatGroup(rowGroup)) {
                // Group-wide array: the key's run starts where the _im says it
                // does, and `from` is an offset within that run.
                final long range = imReader.getKeyRowRangeInGroup(rowGroup, key);
                ensureUnpackCapacity(count);
                BitpackUtils.unpackValuesFrom(
                        packedDataAddrs[rowGroup],
                        Numbers.decodeLowInt(range) + from,
                        count,
                        packedBitWidths[rowGroup],
                        packedBases[rowGroup],
                        unpackBuf
                );
                return unpackBuf;
            }
            final long block = rowIdBlock(rowGroup, key);
            if (block == 0) {
                // The seal writes this arm only uncompressed, uncovered and
                // PLAIN, so the blob is addressable by construction. A file
                // where it is not carries row ids nothing here can read, and
                // decoding the BINARY column as though it held int64 row ids
                // would be a wrong answer rather than a slow one.
                throw CairoException.critical(0)
                        .put("covering index packed payload is not addressable [rowGroup=").put(rowGroup)
                        .put(", column=").put(columnName).put(']');
            }
            ensureUnpackCapacity(count);
            // Batched: one header parse for the whole run, then an AVX2 widen
            // over the residuals. Per-value readLongAt re-parses the block
            // header every call and halved a wide-key drain.
            CoveringCompressor.readLongsInto(block, from, count, unpackBuf);
            return unpackBuf;
        }

        protected CountingCursor probe() {
            if (keyProbe == null) {
                keyProbe = new CountingCursor();
            }
            return keyProbe;
        }

        /**
         * Binds this cursor's decoder to the reader's parquet mapping. The
         * mapping is immutable while the reader is bound, so every cursor may
         * hold its own decoder over the same bytes.
         * <p>
         * <b>Each cursor parses the footer itself, deliberately.</b>
         * {@code ParquetFileDecoder.of(ParquetFileDecoder)} exists to share one
         * parse -- a shallow copy keeping its own decode context -- and a
         * reader-owned source decoder fits here, since a frozen reader outlives
         * every cursor it serves. It was written and measured: binding a
         * detached cursor and taking its first step over a 400k-row index cost
         * 1096 ns/cursor parsing per cursor against 1150 ns/cursor sharing one
         * parse, so sharing is not faster at this footer size. It was reverted
         * rather than kept, because what it does buy is a lifetime coupling
         * between the reader's decoder and every cursor's, plus a synchronized
         * lazy init on the very path getDetachedCursor serves from N workers --
         * real cost against an unobservable saving. Revisit only with a
         * measurement showing the parse matters.
         */
        protected ParquetFileDecoder decoder() {
            if (!decoderBound) {
                // Copies the reader's already-parsed footer and keeps a private
                // decode context, so N cursors decode concurrently without
                // sharing the context they would race on.
                decoder.of(sourceDecoder);
                decoderBound = true;
            }
            return decoder;
        }

        /**
         * Releases everything this cursor owns. Called when the READER closes
         * for its pooled cursor, and by {@link #close()} for a detached one,
         * which no reader will ever come back for.
         */
        protected void freeResources() {
            keyProbe = Misc.free(keyProbe);
            Misc.free(decoder);
            Misc.free(rowGroupBuffers);
            Misc.free(projection);
            if (unpackBuf != 0) {
                Unsafe.free(unpackBuf, unpackBufSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                unpackBuf = 0;
                unpackBufSize = 0;
            }
            decoderBound = false;
        }

        /**
         * Builds the decode projection for a covering cursor: {@code key_id},
         * {@code row_id}, then one entry per requested cover slot, and records
         * where each slot's chunk lands.
         * <p>
         * <b>Three index spaces meet here and must not be confused.</b>
         * {@code requiredCoverColumns} are COVER SLOTS.
         * {@link IndexMetaFileReader#getCoverColumnIndex(int)} maps a slot to a
         * DESCRIPTOR INDEX, which is also the parquet column index -- that is what
         * the decoder wants. A descriptor's {@code ID} is the covered column's
         * WRITER INDEX and is not a lookup key on this path at all.
         * <p>
         * Chunk ordinals follow the projection's order, so slot {@code s} lands at
         * {@code 2 + its position in requiredCoverColumns}, never at {@code s}.
         * The two are equal only when the caller asks for a dense prefix of the
         * slots, which is exactly the case a wrong mapping would still pass.
         */
        protected DirectIntList coveringProjection(int[] requiredCoverColumns) {
            return coveringProjection(requiredCoverColumns, true);
        }

        /**
         * @param includeKeyId whether to decode {@code key_id} alongside the
         *                     values. A cursor bounded by the {@code _im} key
         *                     directory must pass {@code false}: the window it
         *                     decodes IS the key's run, so every row in it
         *                     carries the key by construction and the column
         *                     would be decompressed only to re-derive what the
         *                     directory already said. It is a third of the base
         *                     decode -- 4 bytes a row against {@code row_id}'s
         *                     8 -- and decompression dominates a lookup.
         *                     Callers that scan a whole group instead, having no
         *                     range to bound them, still need it.
         */
        protected DirectIntList coveringProjection(int[] requiredCoverColumns, boolean includeKeyId) {
            projection.clear();
            if (includeKeyId) {
                projection.add(imReader.getKeyIdColumn());
                projection.add(ColumnType.INT);
            }
            projection.add(imReader.getRowIdColumn());
            projection.add(ColumnType.LONG);

            final int coverCount = imReader.getColumnCount() - imReader.getFirstCoverColumn();
            if (coverChunkOrdinal == null || coverChunkOrdinal.length < coverCount) {
                coverChunkOrdinal = new int[Math.max(coverCount, 8)];
            }
            Arrays.fill(coverChunkOrdinal, 0, coverChunkOrdinal.length, -1);
            if (requiredCoverColumns == null) {
                return projection;
            }
            int ordinal = includeKeyId ? 2 : 1;
            for (int i = 0; i < requiredCoverColumns.length; i++) {
                final int slot = requiredCoverColumns[i];
                if (slot < 0 || slot >= coverCount) {
                    throw CairoException.critical(0)
                            .put("cover slot out of range [slot=").put(slot)
                            .put(", coverCount=").put(coverCount)
                            .put(", column=").put(columnName).put(']');
                }
                if (coverChunkOrdinal[slot] >= 0) {
                    continue; // asked for twice; one chunk serves both
                }
                final int descriptor = imReader.getCoverColumnIndex(slot);
                projection.add(descriptor);
                projection.add(imReader.getColumnType(descriptor));
                coverChunkOrdinal[slot] = ordinal++;
            }
            return projection;
        }

        @Override
        public ArrayView getCoveredArray(int includeIdx, int columnType) {
            throw unsupportedCoveredType("ARRAY", includeIdx);
        }

        @Override
        public BinarySequence getCoveredBin(int includeIdx) {
            throw unsupportedCoveredType("BINARY", includeIdx);
        }

        @Override
        public long getCoveredBinLen(int includeIdx) {
            throw unsupportedCoveredType("BINARY", includeIdx);
        }

        @Override
        public byte getCoveredByte(int includeIdx) {
            if (packedPayload) {
                return CoveringCompressor.readByteAt(coveredBlock(includeIdx), coveredIndex());
            }
            return Unsafe.getUnsafe().getByte(coveredAddress(includeIdx, 1));
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            if (packedPayload) {
                return CoveringCompressor.readDoubleAt(coveredBlock(includeIdx), coveredIndex());
            }
            return Unsafe.getUnsafe().getDouble(coveredAddress(includeIdx, 8));
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            if (packedPayload) {
                return CoveringCompressor.readFloatAt(coveredBlock(includeIdx), coveredIndex());
            }
            return Unsafe.getUnsafe().getFloat(coveredAddress(includeIdx, 4));
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            if (packedPayload) {
                return CoveringCompressor.readIntAt(coveredBlock(includeIdx), coveredIndex());
            }
            return Unsafe.getUnsafe().getInt(coveredAddress(includeIdx, 4));
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            if (packedPayload) {
                return CoveringCompressor.readLongAt(coveredBlock(includeIdx), coveredIndex());
            }
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 8));
        }

        @Override
        public long getCoveredLong128Hi(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 16) + 8);
        }

        @Override
        public long getCoveredLong128Lo(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 16));
        }

        @Override
        public long getCoveredLong256_0(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32));
        }

        @Override
        public long getCoveredLong256_1(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32) + 8);
        }

        @Override
        public long getCoveredLong256_2(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32) + 16);
        }

        @Override
        public long getCoveredLong256_3(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32) + 24);
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            if (packedPayload) {
                return CoveringCompressor.readShortAt(coveredBlock(includeIdx), coveredIndex());
            }
            return Unsafe.getUnsafe().getShort(coveredAddress(includeIdx, 2));
        }

        @Override
        public CharSequence getCoveredStrA(int includeIdx) {
            throw unsupportedCoveredType("STRING", includeIdx);
        }

        @Override
        public CharSequence getCoveredStrB(int includeIdx) {
            throw unsupportedCoveredType("STRING", includeIdx);
        }

        @Override
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            throw unsupportedCoveredType("VARCHAR", includeIdx);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            throw unsupportedCoveredType("VARCHAR", includeIdx);
        }

        /**
         * A slot is available when the caller asked for it AND a row has been
         * emitted. Answering true for an unrequested slot would hand back
         * another column's bytes, so this keys on the projection actually
         * built, not on what the index happens to cover.
         */
        /**
         * Marks every cover slot unavailable.
         * <p>
         * The direct-read path serves row ids straight from the mapping and
         * never builds a projection, so without this it inherits the PREVIOUS
         * bind's ordinals: {@link #isCoveredAvailable} would answer true for a
         * slot this bind never projected, and {@code coveredAddress} would read
         * a chunk pointer that belongs to another lookup. Production cannot
         * reach it today -- a caller that can ask for a covered value always
         * passes a non-empty cover set, which skips the direct path -- but the
         * cursor should not depend on its callers for that.
         */
        protected void clearCoverOrdinals() {
            if (coverChunkOrdinal != null) {
                Arrays.fill(coverChunkOrdinal, -1);
            }
        }

        @Override
        public boolean isCoveredAvailable(int includeIdx) {
            if (emittedRow < 0 || includeIdx < 0) {
                return false;
            }
            if (packedPayload) {
                // The packed arm builds no projection -- covered values are
                // addressed in the mapping rather than decoded into buffers --
                // so there are no chunk ordinals to key on. What bounds
                // availability is what the index actually covers.
                return includeIdx < imReader.getColumnCount() - imReader.getFirstCoverColumn();
            }
            return coverChunkOrdinal != null
                    && includeIdx < coverChunkOrdinal.length
                    && coverChunkOrdinal[includeIdx] >= 0;
        }

        /**
         * Refused on a forward cursor, matching
         * {@code AbstractPostingIndexReader}: reaching the last posting
         * forwards is O(n) over the key's whole run, and the caller
         * (LATEST ON's covering path) always has a backward reader available.
         * The backward cursor overrides it.
         */
        @Override
        public long seekToLast() {
            throw new UnsupportedOperationException(
                    "seekToLast: use a backward index reader; forward iteration is O(n)");
        }

        /**
         * The compressed covered block holding {@code includeIdx}'s values for
         * the row last emitted, under the packed arm.
         */
        protected long coveredBlock(int includeIdx) {
            if (!isCoveredAvailable(includeIdx)) {
                throw CairoException.critical(0)
                        .put("covered slot was not projected [slot=").put(includeIdx)
                        .put(", column=").put(columnName).put(']');
            }
            final long blob = coverBlobDataAddr(packedRowGroup, includeIdx);
            if (blob == 0) {
                throw CairoException.critical(0)
                        .put("covering index packed cover blob is not addressable [slot=").put(includeIdx)
                        .put(", rowGroup=").put(packedRowGroup)
                        .put(", column=").put(columnName).put(']');
            }
            final long block = PostingIndexUtils.coverPerKeyBlock(
                    blob, imReader.getRowGroupFirstKey(packedRowGroup), packedKey);
            if (block == 0) {
                throw CairoException.critical(0)
                        .put("covering index packed cover block is absent for a key that emitted a row [slot=")
                        .put(includeIdx).put(", key=").put(packedKey)
                        .put(", rowGroup=").put(packedRowGroup)
                        .put(", column=").put(columnName).put(']');
            }
            return block;
        }

        /**
         * Index of the emitted row WITHIN its key's block. The blocks are per
         * key, so a group ordinal does not address them -- the key's first
         * ordinal has to come off first.
         */
        protected int coveredIndex() {
            return (int) (emittedRow - packedKeyStart);
        }

        /**
         * Address of {@code includeIdx}'s value for the row last emitted.
         * {@code width} is the fixed element width, which is what makes this a
         * multiply rather than an offset lookup -- correct only because every
         * reachable covered type is fixed-width.
         */
        protected long coveredAddress(int includeIdx, int width) {
            if (!isCoveredAvailable(includeIdx)) {
                throw CairoException.critical(0)
                        .put("covered slot was not projected [slot=").put(includeIdx)
                        .put(", column=").put(columnName).put(']');
            }
            if (packedPayload) {
                // emittedRow is the GROUP ordinal here, which is what addresses
                // the covered blob -- the same ordinal the _im key directory
                // gives for the row id. The widened row-id batch has its own
                // indices and is not what this multiplies.
                final long blob = coverBlobDataAddr(packedRowGroup, includeIdx);
                if (blob == 0) {
                    throw CairoException.critical(0)
                            .put("covering index packed cover blob is not addressable [slot=").put(includeIdx)
                            .put(", rowGroup=").put(packedRowGroup)
                            .put(", column=").put(columnName).put(']');
                }
                return blob + emittedRow * width;
            }
            return rowGroupBuffers.getChunkDataPtr(coverChunkOrdinal[includeIdx]) + emittedRow * width;
        }

        protected void setEmittedRow(long row) {
            this.emittedRow = row;
        }

        private CairoException unsupportedCoveredType(String type, int includeIdx) {
            return CairoException.critical(0)
                    .put("parquet covering index does not carry a covered ").put(type)
                    .put(" [slot=").put(includeIdx)
                    .put(", column=").put(columnName)
                    .put("]; the seal refuses var-size and symbol covered columns");
        }
    }

    /**
     * The sentinel is {@link Numbers#LONG_NULL}, NOT {@code -1}: the sole caller
     * tests {@code c != Numbers.LONG_NULL} and then does {@code total += c}, so
     * {@code -1} does not signal a fallback -- it silently subtracts one from a
     * {@code count(*)} answer.
     */
    @Override
    public long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped) {
        if (key < 0 || maxValueClamped < minValue) {
            return Numbers.LONG_NULL;
        }
        long total = nullPrefixCount(key, minValue, nullMaxValue);
        final long range = rowGroupRangeForKey(key);
        if (range == IndexMetaFileReader.KEY_ABSENT) {
            return total;
        }
        final int rgLo = Numbers.decodeLowInt(range);
        final int rgHi = Numbers.decodeHighInt(range);
        try (CountingCursor counter = new CountingCursor()) {
            for (int rg = rgLo; rg <= rgHi; rg++) {
                final long rows = imReader.getRowGroupNumRows(rg);
                if (rows <= 0 || isRowGroupPruned(rg, minValue, maxValueClamped)) {
                    continue;
                }
                if (isWholeGroupInRange(rg, minValue, maxValueClamped) && isGroupDedicatedTo(rg, key, rgLo, rgHi)) {
                    // Every row in this group belongs to this key and falls
                    // inside the window, so its row count IS the answer for it.
                    // No decode: this is the whole point of the primitive.
                    total += rows;
                    continue;
                }
                total += counter.countInGroup(rg, key, minValue, maxValueClamped);
            }
        }
        return total;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnNameTxn;
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        // Direction is the subclass's business: the forward reader overrides
        // this, and the backward one still refuses until Task 6. Leaving the
        // base throwing rather than defaulting to ascending keeps a backward
        // caller from silently receiving forward order.
        throw CairoException.critical(0)
                .put("parquet-form posting index cursor is not implemented for this direction [column=")
                .put(columnName).put(", indexTxn=").put(indexTxn).put(']');
    }

    /**
     * Negative means "no entry". Unlike the two {@code LONG_NULL} sentinels this
     * one IS negative by contract, and {@link AbstractPostingIndexReader} spells
     * it {@code -1}.
     */
    @Override
    public long getEntryMaxValue() {
        // Highest row id the sealed index covers, or -1 when it covers nothing.
        // Negative by contract, and callers branch on the sign to decide
        // whether to clamp their walk -- returning 0 for an empty index would
        // clamp every cursor to row 0 instead of leaving it unclamped.
        final int groups = imReader.getIndexRowGroupCount();
        if (groups <= 0) {
            return -1;
        }
        long max = -1;
        for (int rg = 0; rg < groups; rg++) {
            if (imReader.getRowGroupNumRows(rg) <= 0) {
                continue;
            }
            final long m = imReader.getRowGroupRowIdMax(rg);
            if (m > max) {
                max = m;
            }
        }
        return max;
    }

    /**
     * The committed {@code IM_FILE_SIZE} of the {@code _im} this reader is bound
     * to, as the published token names it.
     */
    public long getImFileSize() {
        return imFileSize;
    }

    /**
     * The {@code index_txn} naming the artifact pair this reader is bound to.
     * {@code TableReader} compares it with the partition's published token to
     * decide whether a cached reader still describes the right generation: a
     * token-only publish moves neither {@code columnNameTxn} nor
     * {@code partitionTxn}, so nothing else here would notice it.
     */
    public long getIndexTxn() {
        return indexTxn;
    }

    /**
     * Native-mmap-shaped and meaningless for a parquet-backed reader, so
     * {@code 0}. Audited callers, both of which tolerate it:
     * {@code LatestByAllIndexedRecordCursor}, whose factory is gated on
     * {@code IndexType.BITMAP} at both construction sites in
     * {@code SqlCodeGenerator} so a POSTING reader never reaches it; and
     * {@code TouchTableFunctionFactory}, whose {@code touchMemory} returns 0
     * pages for {@code baseAddress == 0}, degrading {@code touch_table()} over
     * this index to a no-op rather than dereferencing anything.
     */
    @Override
    public long getKeyBaseAddress() {
        return 0;
    }

    @Override
    public int getKeyCount() {
        // KEY_SPACE_SIZE: the exclusive upper bound on key ids, equal to the
        // native reader's keyCountIncludingNulls. NOT a distinct-key count --
        // occupancy is sparse, and a distinct count would make every key above
        // the first report absent with no error anywhere.
        return imReader.getKeySpaceSize();
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getKeyMemorySize() {
        return 0;
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    /**
     * The table {@code _txn} this reader is pinned at, for the snapshot
     * isolation Task 4 onwards needs.
     */
    public long getPinnedTableTxn() {
        return pinnedTableTxn;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getValueBaseAddress() {
        return 0;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getValueMemorySize() {
        return 0;
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Always throws. The nine-argument form carries no {@code index_txn} and so
     * cannot name {@code <col>.pidx.<indexTxn>.parquet} or its {@code _im}; use
     * {@link #ofParquet} instead. Unreachable from production --
     * {@code TableReader.getIndexReader} rebinds a parquet-form reader through
     * {@code ofParquet}, and {@code reloadColumnAt} drops one rather than
     * rebinding it -- so this is a programming-error guard, not a code path.
     */
    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        throw CairoException.critical(0)
                .put("a parquet-form posting index reader must be bound through ofParquet, which carries the index txn [column=")
                .put(columnName).put(']');
    }

    /**
     * Binds this reader to {@code <col>.pidx.<indexTxn>._im} and
     * {@code <col>.pidx.<indexTxn>.parquet} in the partition directory
     * {@code path} names.
     *
     * @param path       positioned at the partition directory; restored on return
     * @param indexTxn   the {@code index_txn} the partition's {@code _pm} publishes
     *                   for this column
     * @param imFileSize the {@code _im} size the same token publishes, cross-checked
     *                   against the one the file itself commits
     */
    public void ofParquet(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long indexTxn,
            long imFileSize
    ) {
        close();
        this.ff = configuration.getFilesFacade();
        this.columnName = columnName;
        this.columnNameTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.columnTop = columnTop;
        this.partitionTimestamp = partitionTimestamp;
        this.indexTxn = indexTxn;
        this.decodedRowGroupCount.set(0);
        this.decodedRowCount.set(0);
        this.imFileSize = imFileSize;
        final int plen = path.size();
        try {
            final LPSZ imFile = ParquetIndexSeal.indexMetaFileName(path, columnName, indexTxn);
            if (IndexMetaFileReader.openAndMapRO(ff, imFile, imReader) == 0) {
                // The token names this pair, so a missing or uncommitted _im is
                // not a "nothing published yet" state the way it is for a
                // writer-side probe: it is the artifact this snapshot was told
                // to read.
                throw CairoException.critical(0)
                        .put("could not read the covering index _im named by the partition metadata [file=")
                        .put(imFile).put(']').put(RECOVERY_HINT);
            }
            this.packedPayload = imReader.getPayloadKind() == IndexMetaFileWriter.PAYLOAD_ROW_PER_KEY;
            if (imReader.getPayloadKind() != IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING && !packedPayload) {
                // Reading a payload kind with the wrong arm's reader is a
                // wrong-answer class, not a crash, so an unknown one has to be
                // refused rather than attempted.
                throw CairoException.critical(0)
                        .put("unsupported covering index payload kind [payloadKind=").put(imReader.getPayloadKind())
                        .put(", file=").put(imFile).put(']').put(RECOVERY_HINT);
            }
            if (packedPayload && imReader.getRowIdBlobColumn() < 0) {
                // The blob column is where every row id in the file lives, so a
                // packed payload that does not name one carries no postings any
                // reader could find.
                throw CairoException.critical(0)
                        .put("covering index packed payload names no blob column [file=")
                        .put(imFile).put(']').put(RECOVERY_HINT);
            }
            if (imReader.getFileSize() != imFileSize) {
                // The token records the _im size the seal committed. A file
                // committing a different one under the same index_txn is
                // corruption: the artifacts are named by index_txn, so nothing
                // legitimate can rewrite one in place at a different size.
                throw CairoException.critical(0)
                        .put("covering index _im size disagrees with the published token [tokenImFileSize=").put(imFileSize)
                        .put(", imFileSize=").put(imReader.getFileSize())
                        .put(", file=").put(imFile).put(']').put(RECOVERY_HINT);
            }
            path.trimTo(plen);
            final LPSZ pidxFile = ParquetIndexSeal.indexParquetFileName(path, columnName, indexTxn);
            // The _im's recorded size, never ff.length(): the file on disk can
            // carry bytes past the committed footer, and mapping those would
            // hand the decoder a footer the seal never committed.
            final long size = imReader.getPidxFileSize();
            if (size <= 0) {
                throw CairoException.critical(0)
                        .put("covering index parquet size is not addressable [pidxFileSize=").put(size)
                        .put(", file=").put(pidxFile).put(']').put(RECOVERY_HINT);
            }
            pidxAddr = TableUtils.mapRO(ff, pidxFile, LOG, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            pidxSize = size;
            sourceDecoder.of(pidxAddr, pidxSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            open = true;
        } catch (Throwable th) {
            close();
            throw th;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void populateCacheForKey(int key) {
        // No-op, and correctly so. The native reader warms a genLookup cache
        // that its cursor would otherwise rebuild by walking the chain; the
        // _im directory answers the same question with a header lookup, so
        // there is nothing to pre-compute. The contract permits this: the
        // method promises the cursor will not be slower afterwards, not that
        // work happened.
    }

    @Override
    public void reloadConditionally() {
        // Nothing can move under this reader. The artifact pair is named by
        // index_txn and is never rewritten in place, so a reseal produces a NEW
        // pair rather than growing this one -- unlike the native chain, whose
        // value file grows and whose generation count advances beneath a bound
        // reader. A moved token is therefore a rebind, not a reload, and
        // TableReader.getIndexReader does it by comparing getIndexTxn() with
        // the partition's published token and calling ofParquet.
    }

    @Override
    public long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k) {
        if (key < 0 || k < 0 || maxValueClamped < minValue) {
            return Numbers.LONG_NULL;
        }
        // The implicit-null prefix comes first in row order and is not in the
        // index at all, so the k-th match may land inside it.
        final long nulls = nullPrefixCount(key, minValue, nullMaxValue);
        if (k < nulls) {
            // ABSOLUTE, not relative: the prefix rows this counts start at
            // minValue, not at 0, and the caller (CoveringIndexRecordCursorFactory's
            // firstAbs/lastAbs) bounds a chunk with what comes back. Returning k
            // agrees with the native reader only when minValue is 0, which is
            // false of every page frame that starts mid-partition -- the same
            // blind spot the cursors' relative ids have, in the opposite
            // direction.
            return minValue + k;
        }
        long remaining = k - nulls;
        final long range = rowGroupRangeForKey(key);
        if (range == IndexMetaFileReader.KEY_ABSENT) {
            return Numbers.LONG_NULL;
        }
        final int rgLo = Numbers.decodeLowInt(range);
        final int rgHi = Numbers.decodeHighInt(range);
        try (CountingCursor counter = new CountingCursor()) {
            for (int rg = rgLo; rg <= rgHi; rg++) {
                final long rows = imReader.getRowGroupNumRows(rg);
                if (rows <= 0 || isRowGroupPruned(rg, minValue, maxValueClamped)) {
                    continue;
                }
                final long inGroup;
                if (isWholeGroupInRange(rg, minValue, maxValueClamped) && isGroupDedicatedTo(rg, key, rgLo, rgHi)) {
                    inGroup = rows;
                } else {
                    inGroup = counter.countInGroup(rg, key, minValue, maxValueClamped);
                }
                if (remaining < inGroup) {
                    // The k-th match is in THIS group. One decode, wherever the
                    // groups it skipped were countable from metadata.
                    return counter.selectInGroup(rg, key, minValue, maxValueClamped, remaining);
                }
                remaining -= inGroup;
            }
        }
        // k is past the end of the clamped match set. LONG_NULL, never -1:
        // the caller consumes a -1 as an absolute row id.
        return Numbers.LONG_NULL;
    }

    @Override
    public void setFrozen(boolean frozen) {
        this.frozen = frozen;
    }

    @Override
    public void setPinnedTableTxn(long pinnedTableTxn) {
        this.pinnedTableTxn = pinnedTableTxn;
    }
}
