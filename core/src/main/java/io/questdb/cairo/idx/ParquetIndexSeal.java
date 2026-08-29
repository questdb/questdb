/*******************************************************************************
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Seals a covering posting index as a key-aligned parquet file plus its
 * {@code _im} sidecar, in place of the native {@code .pv} / {@code .pc*}
 * files. Selected by
 * {@code cairo.posting.index.parquet.partition.format=parquet}; the seal point
 * is {@code TableWriter.indexParquetColumn}, which already holds everything
 * this needs.
 * <p>
 * The index parquet's schema is {@code key_id INT32, row_id INT64} followed by
 * the covered columns in cover-slot order, and its rows are the partition's
 * postings ordered by key id then row id. Row groups are <b>key-aligned</b>: a
 * key is never split across a row group it shares with another key, and a key
 * with more postings than the target takes consecutive dedicated groups. The
 * {@code _im} writer refuses a file that breaks this, and it must:
 * {@code RG_FIRST_KEY} resolves an exact match to the <i>first</i> row group
 * carrying the key, so a key that both ends one shared group and starts the
 * next silently loses the postings in the earlier group, and nothing at read
 * time can detect the loss.
 * <p>
 * The streaming writer's own fixed row-group threshold is therefore given a
 * bound it cannot reach, and every boundary comes from
 * {@link PartitionEncoder#flushRowGroup}. Zero would select the default rather
 * than disable the threshold, which is why the bound is an explicit large
 * value.
 */
public final class ParquetIndexSeal {

    public static final String IM_SUFFIX = "._im";
    public static final String PIDX_INFIX = ".pidx.";
    public static final String PIDX_SUFFIX = ".parquet";
    private static final int COL_DATA_ENTRY_LONGS = 7;
    // Descriptor index of cover slot 0: the synthetic columns come first.
    private static final int FIRST_COVER_COLUMN = 2;
    // Width of the _im IM_FILE_SIZE field at offset 0, patched last as the
    // file's commit signal.
    private static final int IM_FILE_SIZE_BYTES = 8;
    private static final int KEY_ID_COLUMN = 0;
    private static final int ROW_ID_COLUMN = 1;
    // Descriptor index of the packed payload's blob column. It takes the slot
    // row_id occupies under the per-posting arm, because exactly one of the two
    // is written and the cover slots start after it either way.
    private static final int ROW_ID_BLOB_COLUMN = 1;
    // A QuestDB BINARY value is an i64 length followed by that many bytes, and
    // the aux vector holds one i64 offset per row pointing at the length.
    private static final int BINARY_HEADER_SIZE = Long.BYTES;
    // Bytes the streaming writer prefixes each drained buffer with:
    // [data length][rows written to row groups].
    private static final int STREAM_BUFFER_HEADER_SIZE = 16;
    // Field id of a column belonging to no QuestDB column. The _im writer
    // requires exactly this on the synthetic columns.
    private static final int SYNTHETIC_COLUMN_ID = -1;
    // Rows a row group aims for. Only a key boundary at or past this closes a
    // group, so groups land near it rather than exactly on it.
    /**
     * Upper bound on how many distinct keys share one index row group.
     * <p>
     * A lookup pays for the group it lands in, not for its own rows, so the
     * cost driver is how many OTHER keys are packed alongside it. A row cap
     * alone cannot express that: at 1,000 rows per key a 100k-row group holds
     * 100 keys, while at 25,000 rows per key it holds 4. Capping keys bounds
     * the waste directly and leaves wide-key partitions, which were already
     * well packed, untouched.
     */
    /**
     * Bit 26 of the packed parquet column config: the column holds no nulls and
     * may be written parquet-REQUIRED, which spares every row a definition
     * level and lets a reader address the values in the mapping directly.
     * <p>
     * {@code row_id} is written for every posting and is never null, so it
     * qualifies. {@code key_id} does NOT get it: the delta encoder needs a
     * definition-level stream and refuses Required, and key_id is never read
     * back anyway.
     */
    private static final int PARQUET_CONFIG_REQUIRED_FLAG = 1 << 26;
    private static final long TARGET_ROW_GROUP_ROWS = 100_000;
    // The streaming writer's fixed threshold stays live and would split a key
    // wherever it fired, so it is put beyond any partition's posting count.
    private static final long WRITER_ROW_GROUP_ROWS = Long.MAX_VALUE;

    private ParquetIndexSeal() {
    }

    /**
     * The packed payload's two native buffers: the BINARY aux vector and the
     * blob data it points into.
     */
    private static final class PackedPayload {
        private long auxAddr;
        private long auxSize;
        private long dataAddr;
        private long dataSize;
        /** Allocation size, which exceeds dataSize once the codec beats its bound. */
        private long dataBound;
        // One key id per row group -- the group's first key -- since a parquet
        // row is a group here. See writeIndexArtifacts for why the column is
        // still written at all.
        private long keyIdAddr;
        private long keyIdSize;
        // Per cover slot, the BINARY aux vector and blob data carrying that
        // column's values one blob per row group.
        private long[] coverAuxAddrs;
        private long[] coverAuxSizes;
        private long[] coverDataAddrs;
        private long[] coverDataSizes;
        /** Allocation size, which exceeds coverDataSizes once the codec beats its bound. */
        private long[] coverDataBounds;

        private void free() {
            freeIfSet(dataAddr, dataBound);
            dataAddr = 0;
            freeIfSet(auxAddr, auxSize);
            auxAddr = 0;
            freeIfSet(keyIdAddr, keyIdSize);
            keyIdAddr = 0;
            if (coverDataAddrs != null) {
                for (int i = 0; i < coverDataAddrs.length; i++) {
                    freeIfSet(coverDataAddrs[i], coverDataBounds[i]);
                    coverDataAddrs[i] = 0;
                    freeIfSet(coverAuxAddrs[i], coverAuxSizes[i]);
                    coverAuxAddrs[i] = 0;
                }
            }
        }
    }

    /**
     * Path of the {@code _im} sidecar of the index parquet {@code indexTxn}
     * names. The path is left holding the name.
     */
    public static LPSZ indexMetaFileName(Path path, CharSequence indexColumnName, long indexTxn) {
        return path.concat(indexColumnName).put(PIDX_INFIX).put(indexTxn).put(IM_SUFFIX).$();
    }

    /**
     * Path of the index parquet {@code indexTxn} names. The path is left
     * holding the name.
     */
    public static LPSZ indexParquetFileName(Path path, CharSequence indexColumnName, long indexTxn) {
        return path.concat(indexColumnName).put(PIDX_INFIX).put(indexTxn).put(PIDX_SUFFIX).$();
    }

    /**
     * Writes {@code <col>.pidx.<indexTxn>.parquet} and
     * {@code <col>.pidx.<indexTxn>._im} into the partition directory
     * {@code path} names.
     *
     * @param configuration          source of the parquet encoder settings
     * @param ff                     files facade
     * @param path                   positioned at the partition directory; restored on return
     * @param indexColumnName        name of the indexed SYMBOL column
     * @param indexTxn               txn the artifacts are named with
     * @param keySpaceSize           exclusive upper bound on key ids, that is the native
     *                               reader's {@code keyCountIncludingNulls}. Not a count of
     *                               distinct keys present: occupancy is sparse, and a
     *                               distinct count would make every key above the first
     *                               resolve as absent with no error at all
     * @param rowKeys                one index key per indexed row, in row order, the first
     *                               of them belonging to row {@code firstRowId}
     * @param firstRowId             row id of {@code rowKeys} entry 0
     * @param partitionSize           the partition's row count, against which a covered
     *                                column's top decides whether it is all null here
     * @param coveredNames           covered column names in cover-slot order, null for a
     *                               slot whose column has been dropped
     * @param coveredTypes           covered column types in cover-slot order,
     *                               {@link ColumnType#UNDEFINED} for a dropped column
     * @param coveredWriterIndices   covered column writer indices in cover-slot order
     * @param coveredAddrs           base address of each covered column's row-ordered
     *                               values, addressed by absolute row id, or 0 where the
     *                               caller opened no mapping for the slot
     * @param coveredColumnTops      each covered column's top in this partition; a top at or
     *                               above {@code partitionSize} means the column is all null
     *                               here, which is the one case a 0 address is legal for
     * @param dataRowGroupBoundaries {@code data.parquet}'s cumulative row counts, one more
     *                               entry than it has row groups, starting at 0
     * @return the committed size of the {@code _im} sidecar, which is the third
     * field of the {@code _pm} covering-index entry the caller must publish, or
     * 0 when the partition holds no indexed row and nothing was written
     */
    public static long seal(
            CairoConfiguration configuration,
            FilesFacade ff,
            Path path,
            CharSequence indexColumnName,
            long indexTxn,
            int keySpaceSize,
            DirectIntList rowKeys,
            long firstRowId,
            long partitionSize,
            ObjList<CharSequence> coveredNames,
            IntList coveredTypes,
            IntList coveredWriterIndices,
            LongList coveredAddrs,
            LongList coveredColumnTops,
            LongList dataRowGroupBoundaries
    ) {
        final int plen = path.size();
        final long rowCount = rowKeys.size();
        if (rowCount == 0) {
            return 0;
        }
        validateCoveredColumns(coveredNames, coveredTypes, coveredAddrs, coveredColumnTops, partitionSize);

        final int coverCount = coveredNames.size();
        final IntList groupFirstKeys = new IntList();
        // Flat per-key start offsets for every group, back to back, plus the
        // per-group entry counts that say where to cut it.
        final IntList keyDirEntries = new IntList();
        final IntList groupKeyDirCounts = new IntList();
        final LongList groupRowCounts = new LongList();
        final LongList groupRowIdMaxs = new LongList();
        final LongList groupRowIdMins = new LongList();
        final LongList sortedCoverAddrs = new LongList();
        final LongList sortedCoverSizes = new LongList();

        // Arm B packs the row ids into one blob per row group, which makes a
        // parquet row a GROUP rather than a posting. Every column in a row group
        // shares one row count, so the covered columns become per-group blobs
        // too -- one BINARY column per cover slot, each row holding that group's
        // values. They HAVE to: a covering index is the case this feature
        // exists for, and refusing covers left the packed arm unable to serve it.
        //
        // Ignored under a compressing codec: a compressed chunk cannot be
        // addressed in the mapping at all, so a blob could only be reached by
        // decompressing the page holding it -- which is the cost the arm exists
        // to remove, and would leave a file whose only reader has no fast path.
        // PAYLOAD_KIND in the _im records which arm actually ran.
        final boolean packedPayload = configuration.isPostingIndexParquetPackedPayload()
                && configuration.getPostingIndexParquetCompressionCodec() == ParquetCompression.COMPRESSION_UNCOMPRESSED;

        final long keyIdsSize = rowCount * Integer.BYTES;
        final long rowIdsSize = rowCount * Long.BYTES;
        long imFileSize;
        long keyIdsAddr = 0;
        long rowIdsAddr = 0;
        PackedPayload payload = null;
        try {
            keyIdsAddr = Unsafe.malloc(keyIdsSize, MemoryTag.NATIVE_TABLE_WRITER);
            rowIdsAddr = Unsafe.malloc(rowIdsSize, MemoryTag.NATIVE_TABLE_WRITER);
            for (int slot = 0; slot < coverCount; slot++) {
                final int type = coveredTypes.getQuick(slot);
                final long size = rowCount * ColumnType.sizeOf(type);
                sortedCoverSizes.add(size);
                final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_TABLE_WRITER);
                sortedCoverAddrs.add(addr);
                if (coveredAddrs.getQuick(slot) == 0) {
                    // The column is all null in this partition, so there is
                    // nothing to gather: fill the whole chunk with the type's
                    // null and let the gather pass skip it. This is what the
                    // native seal emits for the same partition.
                    TableUtils.setNull(type, addr, rowCount);
                }
            }

            sortPostingsByKey(
                    rowKeys, firstRowId, keySpaceSize, keyIdsAddr, rowIdsAddr,
                    coveredTypes, coveredAddrs, sortedCoverAddrs
            );
            planRowGroups(
                    keyIdsAddr, rowIdsAddr, rowCount,
                    groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs,
                    keyDirEntries, groupKeyDirCounts,
                    configuration.getPostingIndexParquetMaxKeysPerRowGroup(),
                    configuration.getPostingIndexParquetMinRowsPerRowGroup()
            );
            if (packedPayload) {
                payload = buildPackedPayload(rowIdsAddr, groupFirstKeys, groupRowCounts,
                        groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts,
                        coveredTypes, sortedCoverAddrs);
            }
            imFileSize = writeIndexArtifacts(
                    configuration, ff, path, plen, indexColumnName, indexTxn, keySpaceSize,
                    rowCount, keyIdsAddr, keyIdsSize, rowIdsAddr, rowIdsSize,
                    coveredNames, coveredTypes, coveredWriterIndices,
                    sortedCoverAddrs, sortedCoverSizes,
                    groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs,
                    keyDirEntries, groupKeyDirCounts,
                    dataRowGroupBoundaries, payload
            );
        } finally {
            if (payload != null) {
                payload.free();
            }
            for (int slot = 0, n = sortedCoverAddrs.size(); slot < n; slot++) {
                freeIfSet(sortedCoverAddrs.getQuick(slot), sortedCoverSizes.getQuick(slot));
            }
            freeIfSet(rowIdsAddr, rowIdsSize);
            freeIfSet(keyIdsAddr, keyIdsSize);
            path.trimTo(plen);
        }
        return imFileSize;
    }

    private static void addChunkColumn(DirectLongList columnData, long dataAddr, long dataSize) {
        addChunkColumn(columnData, dataAddr, dataSize, 0, 0);
    }

    /**
     * @param auxAddr aux (secondary) vector address, or 0 for a fixed-width
     *                column. A BINARY column's aux holds one i64 offset per row.
     */
    private static void addChunkColumn(DirectLongList columnData, long dataAddr, long dataSize, long auxAddr, long auxSize) {
        columnData.add(0);
        columnData.add(dataAddr);
        columnData.add(dataSize);
        columnData.add(auxAddr);
        columnData.add(auxSize);
        columnData.add(0);
        columnData.add(0);
    }

    private static void addSchemaColumn(
            DirectUtf8Sink columnNames,
            DirectLongList columnMetadata,
            CharSequence name,
            int writerIndex,
            int columnType
    ) {
        addSchemaColumn(columnNames, columnMetadata, name, writerIndex, columnType, 0);
    }

    /**
     * @param parquetConfig packed per-column parquet config, or 0 for the
     *                      writer's defaults. Pack it with
     *                      {@link TableUtils#packParquetConfig} -- a bare
     *                      encoding id is IGNORED, because the Rust side reads
     *                      the override only when the packed EXPLICIT bit is
     *                      set, and says nothing when it is not.
     */
    private static void addSchemaColumn(
            DirectUtf8Sink columnNames,
            DirectLongList columnMetadata,
            CharSequence name,
            int writerIndex,
            int columnType,
            int parquetConfig
    ) {
        final int start = columnNames.size();
        columnNames.put(name);
        columnMetadata.add(columnNames.size() - start);
        columnMetadata.add((long) writerIndex << 32 | (columnType & 0xFFFFFFFFL));
        columnMetadata.add(parquetConfig);
    }

    private static long appendStreamedBuffer(FilesFacade ff, long fd, long buffer, long fileOffset, LPSZ fileName) {
        if (buffer == 0) {
            return fileOffset;
        }
        final long dataSize = Unsafe.getLong(buffer);
        if (dataSize <= 0) {
            return fileOffset;
        }
        writeFully(ff, fd, buffer + STREAM_BUFFER_HEADER_SIZE, dataSize, fileOffset, fileName);
        return fileOffset + dataSize;
    }

    /**
     * Closes a row group over sorted rows {@code [lo, hi)}, recording its first
     * key and the smallest and largest row id it holds. The row ids are scanned
     * rather than derived from the range: postings are ordered by key first, so
     * a group's row ids are neither contiguous nor monotonic across keys.
     */
    private static void closeRowGroup(
            long keyIdsAddr,
            long rowIdsAddr,
            long lo,
            long hi,
            IntList groupFirstKeys,
            LongList groupRowCounts,
            LongList groupRowIdMins,
            LongList groupRowIdMaxs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts
    ) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long i = lo; i < hi; i++) {
            final long rowId = Unsafe.getLong(rowIdsAddr + i * Long.BYTES);
            if (rowId < min) {
                min = rowId;
            }
            if (rowId > max) {
                max = rowId;
            }
        }
        final int firstKey = Unsafe.getInt(keyIdsAddr + lo * Integer.BYTES);
        groupFirstKeys.add(firstKey);
        groupRowCounts.add(hi - lo);
        groupRowIdMins.add(min);
        groupRowIdMaxs.add(max);
        appendKeyDirectory(keyIdsAddr, lo, hi, firstKey, keyDirEntries, groupKeyDirCounts);
    }

    /**
     * Appends this group's key directory: the start offset within the group of
     * every key id from {@code firstKey} up to the largest it holds, then the
     * group's row count as a terminator, so key {@code k} occupies
     * {@code [d[k - firstKey], d[k - firstKey + 1])}.
     * <p>
     * A key id in that span that the group does not hold gets the same offset
     * as the next one that follows it, so its range is empty and it reads as
     * absent - which is what the sparse key space needs, since occupancy has
     * gaps.
     * <p>
     * This is what lets a reader resolve a key's rows from {@code _im} alone.
     * Format version 3 had no directory, so the reader decoded the group's
     * whole {@code key_id} column and binary searched it, once per key
     * looked up: 2.7 ms on a 100k-row group, paid per key rather than per row
     * returned.
     */
    private static void appendKeyDirectory(
            long keyIdsAddr,
            long lo,
            long hi,
            int firstKey,
            IntList keyDirEntries,
            IntList groupKeyDirCounts
    ) {
        final int before = keyDirEntries.size();
        int expected = firstKey;
        for (long i = lo; i < hi; i++) {
            final int key = Unsafe.getInt(keyIdsAddr + i * Integer.BYTES);
            // The group is key-major, so a key's rows are one contiguous run
            // and this fires once per distinct key, in ascending order.
            while (expected <= key) {
                keyDirEntries.add((int) (i - lo));
                expected++;
            }
        }
        keyDirEntries.add((int) (hi - lo));
        groupKeyDirCounts.add(keyDirEntries.size() - before);
    }

    private static long drainStreamedRowGroups(FilesFacade ff, long fd, long writerPtr, long fileOffset, LPSZ fileName) {
        long buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, 0, 0);
        while (buffer != 0) {
            fileOffset = appendStreamedBuffer(ff, fd, buffer, fileOffset, fileName);
            buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, 0, 0);
        }
        return fileOffset;
    }

    private static void freeIfSet(long addr, long size) {
        if (addr != 0) {
            Unsafe.free(addr, size, MemoryTag.NATIVE_TABLE_WRITER);
        }
    }

    /**
     * Bits a group's row ids pack to: enough for the widest offset from the
     * group's own minimum. Per group rather than per partition, so a partition
     * whose row ids span a wide range still packs each group at the width that
     * group needs.
     */
    private static int groupBitWidth(LongList groupRowIdMins, LongList groupRowIdMaxs, int group) {
        final int needed = BitpackUtils.bitsNeeded(groupRowIdMaxs.getQuick(group) - groupRowIdMins.getQuick(group));
        // Natural width. Rounding up to 8/16/32/64 would let the AVX2 unpack use
        // its dedicated widen paths, and that is worth real throughput -- but it
        // costs a third of the file, and storage is the priority here.
        //
        // Measured both ways on the same fixtures, packed arm, controls stable:
        //   S4 2M postings   aligned 7,829 KB (32 bits)   natural 5,143 KB (21 bits)
        //   S7 scan          aligned 34.4 ops/s           natural 26.4 (-23%)
        //   S7 point read    aligned 908 ops/s            natural 908 (unchanged)
        // So the alignment bought high-cardinality SCANS and nothing else, and
        // 34% of the index is a steep price for it.
        //
        // questdb.idx.packed.align=true restores the aligned widths.
        if (!Boolean.getBoolean("questdb.idx.packed.align")) {
            return needed;
        }
        if (needed <= 8) {
            return 8;
        }
        if (needed <= 16) {
            return 16;
        }
        return needed <= 32 ? 32 : 64;
    }

    /**
     * Builds the packed payload column: one QuestDB BINARY value per row group,
     * holding that group's row ids frame-of-reference packed at the width the
     * group needs.
     * <p>
     * This is what {@code PAYLOAD_KIND 1} exists for. The per-posting arm stores
     * {@code row_id} PLAIN at 8 bytes a posting; here a 2M-row partition packs
     * to 21 bits, which is the native chain's figure and the whole measured gap.
     * <p>
     * The row ids are copied nowhere: {@code rowIdsAddr} is already key-major
     * and ascending within each key, and groups are contiguous runs of it, so
     * group {@code g} packs straight from {@code rowIdsAddr + postingLo * 8}.
     *
     * @return the built payload; the caller owns it and must {@link PackedPayload#free}
     */
    /** A group's per-key boundaries, which is what a per-key block layout needs. */
    private static final class GroupPlan {
        private int keySpan;
        private int blobSize;
        private int[] keyStarts;
    }

    /**
     * Reads a group's key directory into a plain array.
     * <p>
     * The seal used to choose here between a per-group and a per-key frame of
     * reference by comparing their sizes. Per-key BLOCKS superseded both: a
     * block per key gets the frame of reference for free and adds linear
     * prediction on top, which a shared bit width could never express.
     */
    private static GroupPlan planGroup(
            int keySpan,
            IntList keyDirEntries,
            int keyDirBase
    ) {
        final GroupPlan plan = new GroupPlan();
        plan.keySpan = keySpan;
        plan.keyStarts = new int[keySpan + 1];
        for (int i = 0; i <= keySpan; i++) {
            plan.keyStarts[i] = keyDirEntries.getQuick(keyDirBase + i);
        }
        return plan;
    }

    private static PackedPayload buildPackedPayload(
            long rowIdsAddr,
            IntList groupFirstKeys,
            LongList groupRowCounts,
            LongList groupRowIdMins,
            LongList groupRowIdMaxs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts,
            IntList coveredTypes,
            LongList sortedCoverAddrs
    ) {
        final int groupCount = groupRowCounts.size();
        // Planned once and kept: planning walks every key's run to find its own
        // extent, which is too much to repeat in the write pass.
        final GroupPlan[] plans = new GroupPlan[groupCount];
        long dataSize = 0;
        long postingAt = 0;
        int keyDirAt = 0;
        int maxRows = 0;
        for (int g = 0; g < groupCount; g++) {
            final int rows = (int) groupRowCounts.getQuick(g);
            maxRows = Math.max(maxRows, rows);
            // The directory holds one entry per key id in the group's span plus
            // a terminator, so the span is one less than the count.
            final int keySpan = groupKeyDirCounts.getQuick(g) - 1;
            plans[g] = planGroup(keySpan, keyDirEntries, keyDirAt);
            // Upper bound: header plus a linear-prediction block per present key.
            long bound = PostingIndexUtils.coverPerKeyHeaderSize(keySpan);
            for (int i = 0; i < keySpan; i++) {
                final int lo = keyDirEntries.getQuick(keyDirAt + i);
                final int hi = keyDirEntries.getQuick(keyDirAt + i + 1);
                if (lo < hi) {
                    bound += CoveringCompressor.maxCompressedSize(hi - lo, ColumnType.TIMESTAMP);
                }
            }
            plans[g].blobSize = (int) bound;
            dataSize += BINARY_HEADER_SIZE + bound;
            postingAt += rows;
            keyDirAt += groupKeyDirCounts.getQuick(g);
        }

        final PackedPayload payload = new PackedPayload();
        payload.auxSize = (long) groupCount * Long.BYTES;
        payload.dataSize = dataSize;
        payload.keyIdSize = (long) groupCount * Integer.BYTES;
        try {
            payload.auxAddr = Unsafe.malloc(payload.auxSize, MemoryTag.NATIVE_TABLE_WRITER);
            // Zero-filled: encodePackedPayloadBlob ORs the packed values into
            // the destination and writes no padding bits of its own.
            payload.dataAddr = Unsafe.calloc(payload.dataSize, MemoryTag.NATIVE_TABLE_WRITER);
            payload.keyIdAddr = Unsafe.malloc(payload.keyIdSize, MemoryTag.NATIVE_TABLE_WRITER);

            payload.dataBound = dataSize;
            long dataOffset = 0;
            long postingLo = 0;
            final long rowIdWorkspaceSize = (long) maxRows * Long.BYTES;
            final long rowIdWorkspace = Unsafe.malloc(rowIdWorkspaceSize, MemoryTag.NATIVE_TABLE_WRITER);
            int maxSpan = 0;
            long scratchSize = 0;
            for (int g = 0; g < groupCount; g++) {
                maxSpan = Math.max(maxSpan, plans[g].keySpan);
                scratchSize = Math.max(scratchSize, plans[g].blobSize);
            }
            final long blockScratch = Unsafe.malloc(Math.max(scratchSize, 1), MemoryTag.NATIVE_TABLE_WRITER);
            final int[] blockOffsets = new int[Math.max(maxSpan, 1)];
            final int[] blockSizes = new int[Math.max(maxSpan, 1)];
            try {
            for (int g = 0; g < groupCount; g++) {
                Unsafe.getUnsafe().putInt(payload.keyIdAddr + (long) g * Integer.BYTES, groupFirstKeys.getQuick(g));
                final int rows = (int) groupRowCounts.getQuick(g);
                final GroupPlan plan = plans[g];

                Unsafe.getUnsafe().putLong(payload.auxAddr + (long) g * Long.BYTES, dataOffset);
                final long blob = payload.dataAddr + dataOffset + BINARY_HEADER_SIZE;
                // Compressed into scratch first, because the layout depends on
                // whether the sizes come out equal and that is not known until
                // every block is built.
                int scratchAt = 0;
                int uniformSize = -1;
                boolean uniform = true;
                for (int i = 0; i < plan.keySpan; i++) {
                    final int lo = plan.keyStarts[i];
                    final int hi = plan.keyStarts[i + 1];
                    blockOffsets[i] = scratchAt;
                    if (lo >= hi) {
                        blockSizes[i] = 0;
                        continue;
                    }
                    final int size = CoveringCompressor.compressLongsLinearPred(
                            rowIdsAddr + (postingLo + lo) * Long.BYTES,
                            hi - lo,
                            blockScratch + scratchAt,
                            rowIdWorkspace
                    );
                    blockSizes[i] = size;
                    scratchAt += size;
                    if (uniformSize < 0) {
                        uniformSize = size;
                    } else if (uniformSize != size) {
                        uniform = false;
                    }
                }

                // A per-key block costs a 29-byte linear-prediction header
                // before it stores a single row id, so a group whose keys are
                // narrower than that header spends more describing its row ids
                // than the raw ids would occupy. At two postings a key -- an
                // ordinary high-cardinality symbol -- the per-key layout
                // measured 1.63x the per-posting arm it exists to beat.
                //
                // One frame-of-reference array for the whole group has no
                // per-key header at all: the _im directory already gives each
                // key its ordinal range, so nothing inside the blob has to
                // name a key. It cannot exploit a key's own progression the
                // way a per-key block does, which is why it is costed rather
                // than preferred -- wide keys still want their own blocks.
                final long groupRowIds = rowIdsAddr + postingLo * Long.BYTES;
                long flatMin = Long.MAX_VALUE;
                long flatMax = Long.MIN_VALUE;
                for (int i = 0; i < rows; i++) {
                    final long v = Unsafe.getUnsafe().getLong(groupRowIds + (long) i * Long.BYTES);
                    flatMin = Math.min(flatMin, v);
                    flatMax = Math.max(flatMax, v);
                }
                final int flatBitWidth = rows == 0 ? 0 : BitpackUtils.bitsNeeded(flatMax - flatMin);
                final int flatSize = rows == 0
                        ? Integer.MAX_VALUE
                        : PostingIndexUtils.packedPayloadBlobSize(rows, flatBitWidth);
                final int tableSize = PostingIndexUtils.coverPerKeyHeaderSize(plan.keySpan) + scratchAt;
                final int uniformBlobSize = uniform && uniformSize > 0
                        ? PostingIndexUtils.packedUniformBlobSize(plan.keySpan, uniformSize)
                        : Integer.MAX_VALUE;

                final int at;
                if (flatSize < tableSize && flatSize < uniformBlobSize) {
                    PostingIndexUtils.encodePackedPayloadBlob(blob, groupRowIds, rows, flatMin, flatBitWidth);
                    at = flatSize;
                } else if (uniformBlobSize <= tableSize) {
                    // Equal sizes: the block address is arithmetic, so the
                    // offset table -- and the random load per key it costs --
                    // is dropped entirely. Absent keys keep their slot so the
                    // arithmetic stays valid; the _im says they hold no row.
                    Unsafe.getUnsafe().putByte(blob, PostingIndexUtils.PACKED_MODE_PER_KEY_UNIFORM);
                    Unsafe.getUnsafe().putInt(blob + PostingIndexUtils.COVER_PER_KEY_SPAN_OFFSET, plan.keySpan);
                    Unsafe.getUnsafe().putInt(blob + PostingIndexUtils.PACKED_UNIFORM_BLOCK_SIZE_OFFSET, uniformSize);
                    final long data = blob + PostingIndexUtils.PACKED_UNIFORM_DATA_OFFSET;
                    Vect.memset(data, (long) plan.keySpan * uniformSize, 0);
                    for (int i = 0; i < plan.keySpan; i++) {
                        if (blockSizes[i] > 0) {
                            Vect.memcpy(data + (long) i * uniformSize, blockScratch + blockOffsets[i], uniformSize);
                        }
                    }
                    at = PostingIndexUtils.packedUniformBlobSize(plan.keySpan, uniformSize);
                } else {
                    Unsafe.getUnsafe().putByte(blob, PostingIndexUtils.PACKED_MODE_PER_KEY_BLOCKS);
                    Unsafe.getUnsafe().putInt(blob + PostingIndexUtils.COVER_PER_KEY_SPAN_OFFSET, plan.keySpan);
                    final long table = blob + PostingIndexUtils.COVER_PER_KEY_TABLE_OFFSET;
                    int cursor = PostingIndexUtils.coverPerKeyHeaderSize(plan.keySpan);
                    for (int i = 0; i < plan.keySpan; i++) {
                        if (blockSizes[i] == 0) {
                            Unsafe.getUnsafe().putInt(table + (long) i * Integer.BYTES, 0);
                            continue;
                        }
                        Unsafe.getUnsafe().putInt(table + (long) i * Integer.BYTES, cursor);
                        Vect.memcpy(blob + cursor, blockScratch + blockOffsets[i], blockSizes[i]);
                        cursor += blockSizes[i];
                    }
                    at = cursor;
                }
                Unsafe.getUnsafe().putLong(payload.dataAddr + dataOffset, at);
                dataOffset += BINARY_HEADER_SIZE + at;
                postingLo += rows;
            }
            // What the blobs actually occupy, not what was reserved.
            payload.dataSize = dataOffset;
            } finally {
                freeIfSet(blockScratch, Math.max(scratchSize, 1));
                freeIfSet(rowIdWorkspace, rowIdWorkspaceSize);
            }
            buildCoverBlobs(payload, groupRowCounts, coveredTypes, sortedCoverAddrs,
                    keyDirEntries, groupKeyDirCounts);
            return payload;
        } catch (Throwable e) {
            payload.free();
            throw e;
        }
    }

    /**
     * Builds one BINARY column per cover slot: each row group's slice of that
     * covered column, as a blob addressed by the same group ordinal that
     * addresses the row id.
     * <p>
     * The values are already in posting order -- the seal's key-major sort
     * gathered them alongside the row ids -- so a group's slice is a contiguous
     * copy, and the blobs together hold exactly what the per-posting arm would
     * have written as a column.
     */
    /**
     * Builds one BINARY column per cover slot: each row group's slice of that
     * covered column, COMPRESSED with the same codec the native chain uses --
     * ALP for DOUBLE and FLOAT, linear-prediction for the designated timestamp,
     * frame-of-reference for the integer widths.
     * <p>
     * The blob IS the compressed block, with no wrapper: the block is
     * self-describing and {@link CoveringCompressor}'s {@code readXxxAt}
     * decoders address a value by index inside it, so a covered read stays O(1)
     * and never decodes the group. That is what makes this affordable -- a
     * parquet page codec would have compressed too, but only by making every
     * covered read decompress the page it lands in.
     * <p>
     * Storing them raw, as this did first, was 8 bytes a posting against the
     * native chain's 1.98 for the same DOUBLE column, and that gap was the whole
     * reason the parquet form was larger than native overall.
     */
    private static void buildCoverBlobs(
            PackedPayload payload,
            LongList groupRowCounts,
            IntList coveredTypes,
            LongList sortedCoverAddrs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts
    ) {
        final int coverCount = coveredTypes.size();
        payload.coverAuxAddrs = new long[coverCount];
        payload.coverAuxSizes = new long[coverCount];
        payload.coverDataAddrs = new long[coverCount];
        payload.coverDataSizes = new long[coverCount];
        payload.coverDataBounds = new long[coverCount];
        if (coverCount == 0) {
            return;
        }
        final int groupCount = groupRowCounts.size();

        int maxRows = 0;
        for (int g = 0; g < groupCount; g++) {
            maxRows = Math.max(maxRows, (int) groupRowCounts.getQuick(g));
        }
        // The ALP paths need a long workspace per value and a byte per value for
        // the exception map; both are sized by the widest group.
        final long longWorkspaceSize = (long) maxRows * Long.BYTES;
        long longWorkspace = 0;
        long exceptionWorkspace = 0;
        try {
            longWorkspace = Unsafe.malloc(longWorkspaceSize, MemoryTag.NATIVE_TABLE_WRITER);
            exceptionWorkspace = Unsafe.malloc(maxRows, MemoryTag.NATIVE_TABLE_WRITER);

            for (int slot = 0; slot < coverCount; slot++) {
                final int type = coveredTypes.getQuick(slot);
                final int width = ColumnType.sizeOf(type);
                final int shift = Numbers.msb(width);
                // Keyed on the TYPE, matching CoveringCompressor.maxCompressedSize,
                // which also treats TIMESTAMP apart from LONG. A covered
                // TIMESTAMP that is not the designated one still encodes
                // losslessly this way -- linear prediction just compresses it
                // less well -- so the choice cannot produce a wrong value.
                final boolean isTs = ColumnType.tagOf(type) == ColumnType.TIMESTAMP;

                // Upper bound: the codec never exceeds it, and the blobs are
                // written contiguously from the front, so the tail is simply
                // unused and the chunk is handed the ACTUAL length.
                long bound = 0;
                int boundKeyDirAt = 0;
                for (int g = 0; g < groupCount; g++) {
                    final int keySpan = groupKeyDirCounts.getQuick(g) - 1;
                    bound += BINARY_HEADER_SIZE + PostingIndexUtils.coverPerKeyHeaderSize(keySpan);
                    for (int i = 0; i < keySpan; i++) {
                        final int lo = keyDirEntries.getQuick(boundKeyDirAt + i);
                        final int hi = keyDirEntries.getQuick(boundKeyDirAt + i + 1);
                        if (lo < hi) {
                            bound += CoveringCompressor.maxCompressedSize(hi - lo, type);
                        }
                    }
                    boundKeyDirAt += groupKeyDirCounts.getQuick(g);
                }
                payload.coverAuxSizes[slot] = (long) groupCount * Long.BYTES;
                payload.coverAuxAddrs[slot] = Unsafe.malloc(payload.coverAuxSizes[slot], MemoryTag.NATIVE_TABLE_WRITER);
                payload.coverDataAddrs[slot] = Unsafe.malloc(bound, MemoryTag.NATIVE_TABLE_WRITER);
                payload.coverDataSizes[slot] = bound;

                final long src = sortedCoverAddrs.getQuick(slot);
                long dataOffset = 0;
                long postingLo = 0;
                int keyDirAt = 0;
                for (int g = 0; g < groupCount; g++) {
                    final int rows = (int) groupRowCounts.getQuick(g);
                    final int keySpan = groupKeyDirCounts.getQuick(g) - 1;
                    Unsafe.getUnsafe().putLong(payload.coverAuxAddrs[slot] + (long) g * Long.BYTES, dataOffset);

                    final long blob = payload.coverDataAddrs[slot] + dataOffset + BINARY_HEADER_SIZE;
                    Unsafe.getUnsafe().putByte(blob, PostingIndexUtils.COVER_MODE_PER_KEY);
                    Unsafe.getUnsafe().putInt(blob + PostingIndexUtils.COVER_PER_KEY_SPAN_OFFSET, keySpan);
                    final long table = blob + PostingIndexUtils.COVER_PER_KEY_TABLE_OFFSET;
                    int at = PostingIndexUtils.coverPerKeyHeaderSize(keySpan);
                    for (int i = 0; i < keySpan; i++) {
                        final int lo = keyDirEntries.getQuick(keyDirAt + i);
                        final int hi = keyDirEntries.getQuick(keyDirAt + i + 1);
                        if (lo >= hi) {
                            // No rows: offset stays 0, which the reader reads as
                            // absent. A real block cannot start there.
                            Unsafe.getUnsafe().putInt(table + (long) i * Integer.BYTES, 0);
                            continue;
                        }
                        Unsafe.getUnsafe().putInt(table + (long) i * Integer.BYTES, at);
                        // One block per KEY: homogeneous values, so ALP's
                        // exponent and FoR's minimum fit them tightly.
                        at += CoveringCompressor.compressCoveredBlock(
                                src + (postingLo + lo) * width,
                                hi - lo,
                                shift,
                                type,
                                isTs,
                                blob + at,
                                longWorkspace,
                                exceptionWorkspace
                        );
                    }
                    Unsafe.getUnsafe().putLong(payload.coverDataAddrs[slot] + dataOffset, at);
                    dataOffset += BINARY_HEADER_SIZE + at;
                    postingLo += rows;
                    keyDirAt += groupKeyDirCounts.getQuick(g);
                }
                // What the chunk actually holds, not what was reserved.
                payload.coverDataSizes[slot] = dataOffset;
                payload.coverDataBounds[slot] = bound;
            }
        } finally {
            freeIfSet(exceptionWorkspace, maxRows);
            freeIfSet(longWorkspace, longWorkspaceSize);
        }
    }

    /**
     * Lays out the row groups over the key-sorted postings, closing a group only
     * at a key boundary. A key with more postings than the target takes
     * consecutive groups of its own, which keeps every group single-keyed rather
     * than letting its tail share one with the next key.
     */
    private static void planRowGroups(
            long keyIdsAddr,
            long rowIdsAddr,
            long rowCount,
            IntList groupFirstKeys,
            LongList groupRowCounts,
            LongList groupRowIdMins,
            LongList groupRowIdMaxs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts,
            int maxKeysPerRowGroup,
            int minRowsPerRowGroup
    ) {
        long groupLo = 0;
        long keyLo = 0;
        int keysInGroup = 0;
        while (keyLo < rowCount) {
            final int key = Unsafe.getInt(keyIdsAddr + keyLo * Integer.BYTES);
            long keyHi = keyLo + 1;
            while (keyHi < rowCount && Unsafe.getInt(keyIdsAddr + keyHi * Integer.BYTES) == key) {
                keyHi++;
            }
            // Adding this key would overflow the target, so close what is open
            // at the boundary that precedes it rather than inside it.
            if (groupLo < keyLo && keyHi - groupLo > TARGET_ROW_GROUP_ROWS) {
                closeRowGroup(keyIdsAddr, rowIdsAddr, groupLo, keyLo, groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts);
                groupLo = keyLo;
                keysInGroup = 0;
            }
            // A key of its own is larger than the target: give it consecutive
            // dedicated groups. Every one of them holds only this key, which the
            // key-alignment invariant permits.
            while (keyHi - groupLo > TARGET_ROW_GROUP_ROWS) {
                final long splitHi = groupLo + TARGET_ROW_GROUP_ROWS;
                closeRowGroup(keyIdsAddr, rowIdsAddr, groupLo, splitHi, groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts);
                groupLo = splitHi;
                keysInGroup = 0;
            }
            keysInGroup++;
            // Closing on either bound, and both close at a key boundary, so the
            // key-alignment invariant holds however the group filled up.
            //
            // The key cap only applies once the group is worth addressing.
            // Without the floor, narrow keys -- 500k of them over 2M rows is 4
            // rows each -- close a group every 64 rows and bury the partition
            // in per-group metadata.
            if (keyHi - groupLo >= TARGET_ROW_GROUP_ROWS
                    || (keysInGroup >= maxKeysPerRowGroup && keyHi - groupLo >= minRowsPerRowGroup)) {
                closeRowGroup(keyIdsAddr, rowIdsAddr, groupLo, keyHi, groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts);
                groupLo = keyHi;
                keysInGroup = 0;
            }
            keyLo = keyHi;
        }
        if (groupLo < rowCount) {
            closeRowGroup(keyIdsAddr, rowIdsAddr, groupLo, rowCount, groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts);
        }
    }

    /**
     * Counting-sorts the row-ordered postings into key order, gathering each
     * covered column's values along the way. Row ids stay ascending within a
     * key, because the second pass walks the rows in row order.
     */
    private static void sortPostingsByKey(
            DirectIntList rowKeys,
            long firstRowId,
            int keySpaceSize,
            long keyIdsAddr,
            long rowIdsAddr,
            IntList coveredTypes,
            LongList coveredAddrs,
            LongList sortedCoverAddrs
    ) {
        final long rowCount = rowKeys.size();
        final long cursorSize = (long) keySpaceSize * Long.BYTES;
        final long cursorAddr = Unsafe.calloc(cursorSize, MemoryTag.NATIVE_TABLE_WRITER);
        try {
            for (long r = 0; r < rowCount; r++) {
                final int key = rowKeys.get(r);
                if (key < 0 || key >= keySpaceSize) {
                    throw CairoException.critical(0)
                            .put("index key outside the key space [key=").put(key)
                            .put(", keySpaceSize=").put(keySpaceSize).put(']');
                }
                final long slot = cursorAddr + (long) key * Long.BYTES;
                Unsafe.putLong(slot, Unsafe.getLong(slot) + 1);
            }
            long running = 0;
            for (int key = 0; key < keySpaceSize; key++) {
                final long slot = cursorAddr + (long) key * Long.BYTES;
                final long count = Unsafe.getLong(slot);
                Unsafe.putLong(slot, running);
                running += count;
            }
            final int coverCount = sortedCoverAddrs.size();
            for (long r = 0; r < rowCount; r++) {
                final int key = rowKeys.get(r);
                final long slot = cursorAddr + (long) key * Long.BYTES;
                final long pos = Unsafe.getLong(slot);
                Unsafe.putLong(slot, pos + 1);
                Unsafe.putInt(keyIdsAddr + pos * Integer.BYTES, key);
                Unsafe.putLong(rowIdsAddr + pos * Long.BYTES, firstRowId + r);
                for (int c = 0; c < coverCount; c++) {
                    final long coveredAddr = coveredAddrs.getQuick(c);
                    if (coveredAddr == 0) {
                        // All-null slot, pre-filled by the caller.
                        continue;
                    }
                    final long entrySize = ColumnType.sizeOf(coveredTypes.getQuick(c));
                    Vect.memcpy(
                            sortedCoverAddrs.getQuick(c) + pos * entrySize,
                            coveredAddr + (firstRowId + r) * entrySize,
                            entrySize
                    );
                }
            }
        } finally {
            Unsafe.free(cursorAddr, cursorSize, MemoryTag.NATIVE_TABLE_WRITER);
        }
    }

    /**
     * The one place that decides which covered columns this seal accepts.
     * Rejecting is deliberate: a column whose values did not match its
     * descriptor would be undetectable at read time.
     * <p>
     * Refused, each with its own message:
     * <ol>
     *     <li>a covered column that has been dropped from the table, which has
     *     no type left to write it under;</li>
     *     <li>a var-size covered column, whose key-ordered gather needs an aux
     *     vector rebuild this seal does not do;</li>
     *     <li>a SYMBOL covered column, whose gather needs the symbol table;</li>
     *     <li>a covered column with rows in this partition but no mapping,
     *     which is a column absent from {@code data.parquet}.</li>
     * </ol>
     * Accepted with nulls, matching the native seal: a covered column with no
     * rows in this partition, that is one added after the partition was created.
     * That case also arrives with no mapping, which is why it is told apart by
     * the column top rather than by the address alone. The top has two spellings
     * for it and both must be honoured: an explicit record at the partition size
     * when the column existed by the time the partition was written, and
     * {@code getColumnTop}'s -1 sentinel when there is no record at all because
     * the column was added later. Checking only the first reads a column that is
     * entirely absent as one that has rows, and refuses it.
     */
    private static void validateCoveredColumns(
            ObjList<CharSequence> coveredNames,
            IntList coveredTypes,
            LongList coveredAddrs,
            LongList coveredColumnTops,
            long partitionSize
    ) {
        for (int slot = 0, n = coveredTypes.size(); slot < n; slot++) {
            final int type = coveredTypes.getQuick(slot);
            if (type <= ColumnType.UNDEFINED) {
                throw CairoException.nonCritical()
                        .put("parquet covering index cannot cover a dropped column [coverSlot=")
                        .put(slot).put(']');
            }
            if (ColumnType.isVarSize(type) || ColumnType.isSymbol(type)) {
                throw CairoException.nonCritical()
                        .put("parquet covering index does not support this covered column type [column=")
                        .put(coveredNames.getQuick(slot))
                        .put(", type=").put(ColumnType.nameOf(type))
                        .put(']');
            }
            final long columnTop = coveredColumnTops.getQuick(slot);
            final boolean isAllNullInPartition = columnTop < 0 || columnTop >= partitionSize;
            if (coveredAddrs.getQuick(slot) == 0 && !isAllNullInPartition) {
                throw CairoException.nonCritical()
                        .put("parquet covering index requires every covered column that has rows in the partition [column=")
                        .put(coveredNames.getQuick(slot))
                        .put(", columnTop=").put(columnTop)
                        .put(", partitionSize=").put(partitionSize)
                        .put(']');
            }
        }
    }

    private static void writeFully(FilesFacade ff, long fd, long addr, long len, long offset, LPSZ fileName) {
        final long written = ff.write(fd, addr, len, offset);
        if (written != len) {
            throw CairoException.critical(ff.errno())
                    .put("could not write [file=").put(fileName)
                    .put(", offset=").put(offset)
                    .put(", size=").put(len)
                    .put(", written=").put(written)
                    .put(']');
        }
    }

    /**
     * @return the committed size of the {@code _im} sidecar
     */
    private static long writeIndexArtifacts(
            CairoConfiguration configuration,
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence indexColumnName,
            long indexTxn,
            int keySpaceSize,
            long rowCount,
            long keyIdsAddr,
            long keyIdsSize,
            long rowIdsAddr,
            long rowIdsSize,
            ObjList<CharSequence> coveredNames,
            IntList coveredTypes,
            IntList coveredWriterIndices,
            LongList sortedCoverAddrs,
            LongList sortedCoverSizes,
            IntList groupFirstKeys,
            LongList groupRowCounts,
            LongList groupRowIdMins,
            LongList groupRowIdMaxs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts,
            LongList dataRowGroupBoundaries,
            PackedPayload payload
    ) {
        final int coverCount = coveredNames.size();
        final int columnCount = FIRST_COVER_COLUMN + coverCount;
        // Under the packed arm a parquet row is a ROW GROUP, so the chunk holds
        // one row per group and each group is flushed with a count of 1. The
        // posting counts stay the _im's, which is what logicalRowCounts carries.
        final long parquetRowCount = payload != null ? groupRowCounts.size() : rowCount;

        DirectUtf8Sink columnNames = null;
        DirectLongList columnMetadata = null;
        DirectLongList columnData = null;
        long writerPtr = 0;
        try {
            columnNames = new DirectUtf8Sink(64, false, MemoryTag.NATIVE_TABLE_WRITER);
            columnMetadata = new DirectLongList(3L * columnCount, MemoryTag.NATIVE_TABLE_WRITER, true);
            columnData = new DirectLongList((long) COL_DATA_ENTRY_LONGS * columnCount, MemoryTag.NATIVE_TABLE_WRITER, true);
            // All three are constructed closed, so nothing below may touch them
            // before the backing allocation exists.
            columnNames.reopen();
            columnMetadata.reopen();
            columnData.reopen();

            // key_id is non-decreasing within a group by construction -- the
            // seal writes it key-major -- so it delta packs to almost nothing.
            // It is never read back (the _im directory answers what it used to),
            // but it is still written, and its chunk statistics are what the
            // key-alignment check reads.
            addSchemaColumn(columnNames, columnMetadata, "key_id", SYNTHETIC_COLUMN_ID, ColumnType.INT,
                    TableUtils.packParquetConfig(
                            Integer.getInteger("questdb.idx.keyid.encoding", ParquetEncoding.ENCODING_DELTA_BINARY_PACKED),
                            ParquetCompression.COMPRESSION_UNCOMPRESSED + 1,
                            -1,
                            false
                    ));
            // row_id stays PLAIN, deliberately. Delta packing shrinks it a lot
            // -- and was tried -- but a delta block has to be decoded from its
            // start, so reading one key's run out of the middle of a group pays
            // for every posting before it. Measured: point reads went from 8-9x
            // slower than native to 40-44x and range reads from 4-9x to 15-32x,
            // while scans, which read a group start to end anyway, were
            // unaffected. Random access is what this column is for.
            //
            // Under the packed arm the same slot holds one BINARY blob per row
            // group instead. PLAIN and UNCOMPRESSED for the same reason, and
            // REQUIRED for a sharper one: a BYTE_ARRAY column that carries
            // definition levels cannot be addressed in the mapping at all, so
            // without it every blob read would decode the page it sits in --
            // which is the cost the arm exists to remove.
            if (payload != null) {
                addSchemaColumn(columnNames, columnMetadata, "row_id_blob", SYNTHETIC_COLUMN_ID, ColumnType.BINARY,
                        TableUtils.packParquetConfig(
                                ParquetEncoding.ENCODING_PLAIN,
                                ParquetCompression.COMPRESSION_UNCOMPRESSED + 1,
                                -1,
                                false
                        ) | PARQUET_CONFIG_REQUIRED_FLAG);
            } else {
                addSchemaColumn(columnNames, columnMetadata, "row_id", SYNTHETIC_COLUMN_ID, ColumnType.LONG,
                        TableUtils.packParquetConfig(
                                ParquetEncoding.ENCODING_PLAIN,
                                ParquetCompression.COMPRESSION_UNCOMPRESSED + 1,
                                -1,
                                false
                        ) | PARQUET_CONFIG_REQUIRED_FLAG);
            }
            for (int slot = 0; slot < coverCount; slot++) {
                if (payload != null) {
                    // One blob per row group, matching row_id_blob, because every
                    // column of a row group shares its row count. REQUIRED PLAIN
                    // uncompressed for the same reason as the row-id blob: a
                    // BYTE_ARRAY column carrying definition levels cannot be
                    // addressed in the mapping, and addressing is the point.
                    //
                    // The descriptor's TYPE is therefore BINARY, not the covered
                    // column's own type. Its ID still carries the covered
                    // column's writer index, which is what the _im resolves a
                    // cover slot by, and the blob header carries the element
                    // width the reader multiplies by.
                    addSchemaColumn(
                            columnNames, columnMetadata, coveredNames.getQuick(slot),
                            coveredWriterIndices.getQuick(slot), ColumnType.BINARY,
                            TableUtils.packParquetConfig(
                                    ParquetEncoding.ENCODING_PLAIN,
                                    ParquetCompression.COMPRESSION_UNCOMPRESSED + 1,
                                    -1,
                                    false
                            ) | PARQUET_CONFIG_REQUIRED_FLAG
                    );
                } else {
                    addSchemaColumn(
                            columnNames, columnMetadata, coveredNames.getQuick(slot),
                            coveredWriterIndices.getQuick(slot), coveredTypes.getQuick(slot)
                    );
                }
            }

            if (payload != null) {
                // key_id is still written, degenerate at one value per group.
                // The _im's key-alignment invariant reads this chunk's min and
                // max statistics, and holding the group's FIRST key makes min
                // and max both equal it -- which satisfies the invariant, but
                // no longer PROVES what it proves under the per-posting arm,
                // namely that the group does not split a key. The planner is
                // what guarantees that here: it closes a group only at a key
                // boundary, and gives an oversized key consecutive groups of
                // its own.
                addChunkColumn(columnData, payload.keyIdAddr, payload.keyIdSize);
                addChunkColumn(columnData, payload.dataAddr, payload.dataSize, payload.auxAddr, payload.auxSize);
            } else {
                addChunkColumn(columnData, keyIdsAddr, keyIdsSize);
                addChunkColumn(columnData, rowIdsAddr, rowIdsSize);
            }
            for (int slot = 0; slot < coverCount; slot++) {
                if (payload != null) {
                    addChunkColumn(columnData,
                            payload.coverDataAddrs[slot], payload.coverDataSizes[slot],
                            payload.coverAuxAddrs[slot], payload.coverAuxSizes[slot]);
                } else {
                    addChunkColumn(columnData, sortedCoverAddrs.getQuick(slot), sortedCoverSizes.getQuick(slot));
                }
            }

            writerPtr = PartitionEncoder.createStreamingParquetWriter(
                    Unsafe.getNativeAllocator(MemoryTag.NATIVE_TABLE_WRITER),
                    columnCount,
                    columnNames.ptr(),
                    columnNames.size(),
                    columnMetadata.getAddress(),
                    -1,
                    false,
                    ParquetCompression.packCompressionCodecLevel(
                            configuration.getPostingIndexParquetCompressionCodec(),
                            configuration.getPartitionEncoderParquetCompressionLevel()
                    ),
                    // The key_id chunk's min and max statistics are what the _im
                    // key directory and its key-alignment check are built from,
                    // and without statistics they are simply absent.
                    true,
                    false,
                    WRITER_ROW_GROUP_ROWS,
                    configuration.getPostingIndexParquetDataPageSize(),
                    ParquetVersion.PARQUET_VERSION_V1,
                    0,
                    0,
                    0.0,
                    configuration.getPartitionEncoderParquetMinCompressionRatio()
            );

            final long parquetFileSize = writeIndexParquet(
                    ff, path, plen, indexColumnName, indexTxn, writerPtr,
                    columnData.getAddress(), parquetRowCount, groupRowCounts, payload != null
            );
            if (parquetFileSize <= 0) {
                throw CairoException.critical(0)
                        .put("index parquet write produced no bytes [column=").put(indexColumnName)
                        .put(", indexTxn=").put(indexTxn).put(']');
            }
            return writeIndexMeta(
                    ff, path, plen, indexColumnName, indexTxn, writerPtr, keySpaceSize,
                    groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs,
                    keyDirEntries, groupKeyDirCounts, dataRowGroupBoundaries, payload != null
            );
        } finally {
            if (writerPtr != 0) {
                PartitionEncoder.closeStreamingParquetWriter(writerPtr);
            }
            Misc.free(columnData);
            Misc.free(columnMetadata);
            Misc.free(columnNames);
            path.trimTo(plen);
        }
    }

    /**
     * Generates the {@code _im} from the finished writer's own thrift metadata
     * and commits it, patching {@code IM_FILE_SIZE} last: until those eight
     * bytes land the file reads as uncommitted rather than as a short file.
     *
     * @return the committed size of the {@code _im}
     */
    private static long writeIndexMeta(
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence indexColumnName,
            long indexTxn,
            long writerPtr,
            int keySpaceSize,
            IntList groupFirstKeys,
            LongList groupRowCounts,
            LongList groupRowIdMins,
            LongList groupRowIdMaxs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts,
            LongList dataRowGroupBoundaries,
            boolean packedPayload
    ) {
        final int groupCount = groupFirstKeys.size();
        final int boundaryCount = dataRowGroupBoundaries.size();
        // Each byte length below is the size the buffer beside it was allocated
        // with, not a recomputed count * element width. The native side rejects
        // a length that disagrees with the count, but a consistent pair of wrong
        // values reads out of bounds and neither side can tell.
        final long boundariesSize = (long) boundaryCount * Long.BYTES;
        final long firstKeysSize = (long) groupCount * Integer.BYTES;
        final long rowIdMaxsSize = (long) groupCount * Long.BYTES;
        final long rowIdMinsSize = (long) groupCount * Long.BYTES;
        final long keyDirSize = (long) keyDirEntries.size() * Integer.BYTES;
        final long keyDirCountsSize = (long) groupKeyDirCounts.size() * Integer.BYTES;

        long boundariesAddr = 0;
        long firstKeysAddr = 0;
        long rowIdMaxsAddr = 0;
        long rowIdMinsAddr = 0;
        long keyDirAddr = 0;
        long keyDirCountsAddr = 0;
        // Postings per row group, needed only when a parquet row is a whole
        // group and the footer's count of 1 says nothing about them.
        final long logicalRowCountsSize = packedPayload ? (long) groupCount * Long.BYTES : 0;
        long logicalRowCountsAddr = 0;
        long resultPtr = 0;
        try {
            boundariesAddr = Unsafe.malloc(boundariesSize, MemoryTag.NATIVE_TABLE_WRITER);
            firstKeysAddr = Unsafe.malloc(firstKeysSize, MemoryTag.NATIVE_TABLE_WRITER);
            rowIdMaxsAddr = Unsafe.malloc(rowIdMaxsSize, MemoryTag.NATIVE_TABLE_WRITER);
            rowIdMinsAddr = Unsafe.malloc(rowIdMinsSize, MemoryTag.NATIVE_TABLE_WRITER);
            keyDirCountsAddr = Unsafe.malloc(keyDirCountsSize, MemoryTag.NATIVE_TABLE_WRITER);
            if (keyDirSize > 0) {
                keyDirAddr = Unsafe.malloc(keyDirSize, MemoryTag.NATIVE_TABLE_WRITER);
                for (int i = 0, n = keyDirEntries.size(); i < n; i++) {
                    Unsafe.putInt(keyDirAddr + (long) i * Integer.BYTES, keyDirEntries.getQuick(i));
                }
            }
            for (int i = 0, n = groupKeyDirCounts.size(); i < n; i++) {
                Unsafe.putInt(keyDirCountsAddr + (long) i * Integer.BYTES, groupKeyDirCounts.getQuick(i));
            }
            for (int i = 0; i < groupCount; i++) {
                Unsafe.putInt(firstKeysAddr + (long) i * Integer.BYTES, groupFirstKeys.getQuick(i));
                Unsafe.putLong(rowIdMinsAddr + (long) i * Long.BYTES, groupRowIdMins.getQuick(i));
                Unsafe.putLong(rowIdMaxsAddr + (long) i * Long.BYTES, groupRowIdMaxs.getQuick(i));
            }
            for (int i = 0; i < boundaryCount; i++) {
                Unsafe.putLong(boundariesAddr + (long) i * Long.BYTES, dataRowGroupBoundaries.getQuick(i));
            }
            if (logicalRowCountsSize > 0) {
                logicalRowCountsAddr = Unsafe.malloc(logicalRowCountsSize, MemoryTag.NATIVE_TABLE_WRITER);
                for (int i = 0; i < groupCount; i++) {
                    Unsafe.putLong(logicalRowCountsAddr + (long) i * Long.BYTES, groupRowCounts.getQuick(i));
                }
            }

            resultPtr = IndexMetaFileWriter.generateIndexMetadata(
                    writerPtr,
                    firstKeysAddr, firstKeysSize,
                    rowIdMinsAddr, rowIdMinsSize,
                    rowIdMaxsAddr, rowIdMaxsSize,
                    boundariesAddr, boundariesSize,
                    keyDirAddr, keyDirSize,
                    keyDirCountsAddr, keyDirCountsSize,
                    groupCount,
                    keySpaceSize,
                    KEY_ID_COLUMN,
                    packedPayload ? -1 : ROW_ID_COLUMN,
                    packedPayload ? ROW_ID_BLOB_COLUMN : -1,
                    FIRST_COVER_COLUMN,
                    packedPayload
                            ? IndexMetaFileWriter.PAYLOAD_ROW_PER_KEY
                            : IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING,
                    // Under the per-posting arm the footer's row count already
                    // IS the posting count and there is nothing to override.
                    logicalRowCountsAddr
            );

            final long dataPtr = IndexMetaFileWriter.resultDataPtr(resultPtr);
            final long dataLen = IndexMetaFileWriter.resultDataLen(resultPtr);
            final LPSZ imFile = indexMetaFileName(path.trimTo(plen), indexColumnName, indexTxn);
            ff.removeQuiet(imFile);
            final long fd = ff.openRW(imFile, CairoConfiguration.O_NONE);
            if (fd < 0) {
                throw CairoException.critical(ff.errno()).put("could not open _im [file=").put(imFile).put(']');
            }
            try {
                writeFully(ff, fd, dataPtr + IM_FILE_SIZE_BYTES, dataLen - IM_FILE_SIZE_BYTES, IM_FILE_SIZE_BYTES, imFile);
                ff.fsync(fd);
                writeFully(ff, fd, dataPtr, IM_FILE_SIZE_BYTES, 0, imFile);
                ff.fsync(fd);
            } finally {
                ff.close(fd);
            }
            return dataLen;
        } finally {
            if (resultPtr != 0) {
                IndexMetaFileWriter.destroyResult(resultPtr);
            }
            freeIfSet(logicalRowCountsAddr, logicalRowCountsSize);
            freeIfSet(keyDirCountsAddr, keyDirCountsSize);
            freeIfSet(keyDirAddr, keyDirSize);
            freeIfSet(rowIdMinsAddr, rowIdMinsSize);
            freeIfSet(rowIdMaxsAddr, rowIdMaxsSize);
            freeIfSet(firstKeysAddr, firstKeysSize);
            freeIfSet(boundariesAddr, boundariesSize);
            path.trimTo(plen);
        }
    }

    /**
     * Streams the key-sorted postings out as the index parquet, closing a row
     * group exactly where the plan says. The whole chunk is submitted once and
     * carved by {@link PartitionEncoder#flushRowGroup}, so a boundary may fall
     * anywhere inside it.
     *
     * @return the parquet file's size
     */
    private static long writeIndexParquet(
            FilesFacade ff,
            Path path,
            int plen,
            CharSequence indexColumnName,
            long indexTxn,
            long writerPtr,
            long columnDataAddr,
            long rowCount,
            LongList groupRowCounts,
            boolean packedPayload
    ) {
        final LPSZ pidxFile = indexParquetFileName(path.trimTo(plen), indexColumnName, indexTxn);
        ff.removeQuiet(pidxFile);
        final long fd = ff.openRW(pidxFile, CairoConfiguration.O_NONE);
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not open index parquet [file=").put(pidxFile).put(']');
        }
        try {
            long fileOffset = appendStreamedBuffer(
                    ff, fd,
                    PartitionEncoder.writeStreamingParquetChunk(writerPtr, columnDataAddr, rowCount),
                    0, pidxFile
            );
            for (int i = 0, n = groupRowCounts.size(); i < n; i++) {
                // One parquet row per group under the packed arm, whatever the
                // group's posting count is.
                PartitionEncoder.flushRowGroup(writerPtr, packedPayload ? 1 : groupRowCounts.getQuick(i));
                fileOffset = drainStreamedRowGroups(ff, fd, writerPtr, fileOffset, pidxFile);
            }
            fileOffset = appendStreamedBuffer(
                    ff, fd, PartitionEncoder.finishStreamingParquetWrite(writerPtr), fileOffset, pidxFile
            );
            ff.fsync(fd);
            return fileOffset;
        } finally {
            ff.close(fd);
            path.trimTo(plen);
        }
    }
}
