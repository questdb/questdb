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
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
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
    // Bytes the streaming writer prefixes each drained buffer with:
    // [data length][rows written to row groups].
    private static final int STREAM_BUFFER_HEADER_SIZE = 16;
    // Field id of a column belonging to no QuestDB column. The _im writer
    // requires exactly this on the synthetic columns.
    private static final int SYNTHETIC_COLUMN_ID = -1;
    // Rows a row group aims for. Only a key boundary at or past this closes a
    // group, so groups land near it rather than exactly on it.
    private static final long TARGET_ROW_GROUP_ROWS = 100_000;
    // The streaming writer's fixed threshold stays live and would split a key
    // wherever it fired, so it is put beyond any partition's posting count.
    private static final long WRITER_ROW_GROUP_ROWS = Long.MAX_VALUE;

    private ParquetIndexSeal() {
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

        final long keyIdsSize = rowCount * Integer.BYTES;
        final long rowIdsSize = rowCount * Long.BYTES;
        long imFileSize;
        long keyIdsAddr = 0;
        long rowIdsAddr = 0;
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
                    keyDirEntries, groupKeyDirCounts
            );
            imFileSize = writeIndexArtifacts(
                    configuration, ff, path, plen, indexColumnName, indexTxn, keySpaceSize,
                    rowCount, keyIdsAddr, keyIdsSize, rowIdsAddr, rowIdsSize,
                    coveredNames, coveredTypes, coveredWriterIndices,
                    sortedCoverAddrs, sortedCoverSizes,
                    groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs,
                    keyDirEntries, groupKeyDirCounts,
                    dataRowGroupBoundaries
            );
        } finally {
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
        columnData.add(0);
        columnData.add(dataAddr);
        columnData.add(dataSize);
        columnData.add(0);
        columnData.add(0);
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
        final int start = columnNames.size();
        columnNames.put(name);
        columnMetadata.add(columnNames.size() - start);
        columnMetadata.add((long) writerIndex << 32 | (columnType & 0xFFFFFFFFL));
        columnMetadata.add(0);
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
            IntList groupKeyDirCounts
    ) {
        long groupLo = 0;
        long keyLo = 0;
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
            }
            // A key of its own is larger than the target: give it consecutive
            // dedicated groups. Every one of them holds only this key, which the
            // key-alignment invariant permits.
            while (keyHi - groupLo > TARGET_ROW_GROUP_ROWS) {
                final long splitHi = groupLo + TARGET_ROW_GROUP_ROWS;
                closeRowGroup(keyIdsAddr, rowIdsAddr, groupLo, splitHi, groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts);
                groupLo = splitHi;
            }
            if (keyHi - groupLo >= TARGET_ROW_GROUP_ROWS) {
                closeRowGroup(keyIdsAddr, rowIdsAddr, groupLo, keyHi, groupFirstKeys, groupRowCounts, groupRowIdMins, groupRowIdMaxs, keyDirEntries, groupKeyDirCounts);
                groupLo = keyHi;
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
            LongList dataRowGroupBoundaries
    ) {
        final int coverCount = coveredNames.size();
        final int columnCount = FIRST_COVER_COLUMN + coverCount;

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

            addSchemaColumn(columnNames, columnMetadata, "key_id", SYNTHETIC_COLUMN_ID, ColumnType.INT);
            addSchemaColumn(columnNames, columnMetadata, "row_id", SYNTHETIC_COLUMN_ID, ColumnType.LONG);
            for (int slot = 0; slot < coverCount; slot++) {
                addSchemaColumn(
                        columnNames, columnMetadata, coveredNames.getQuick(slot),
                        coveredWriterIndices.getQuick(slot), coveredTypes.getQuick(slot)
                );
            }

            addChunkColumn(columnData, keyIdsAddr, keyIdsSize);
            addChunkColumn(columnData, rowIdsAddr, rowIdsSize);
            for (int slot = 0; slot < coverCount; slot++) {
                addChunkColumn(columnData, sortedCoverAddrs.getQuick(slot), sortedCoverSizes.getQuick(slot));
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
                            configuration.getPartitionEncoderParquetCompressionCodec(),
                            configuration.getPartitionEncoderParquetCompressionLevel()
                    ),
                    // The key_id chunk's min and max statistics are what the _im
                    // key directory and its key-alignment check are built from,
                    // and without statistics they are simply absent.
                    true,
                    false,
                    WRITER_ROW_GROUP_ROWS,
                    configuration.getPartitionEncoderParquetDataPageSize(),
                    ParquetVersion.PARQUET_VERSION_V1,
                    0,
                    0,
                    0.0,
                    configuration.getPartitionEncoderParquetMinCompressionRatio()
            );

            final long parquetFileSize = writeIndexParquet(
                    ff, path, plen, indexColumnName, indexTxn, writerPtr,
                    columnData.getAddress(), rowCount, groupRowCounts
            );
            if (parquetFileSize <= 0) {
                throw CairoException.critical(0)
                        .put("index parquet write produced no bytes [column=").put(indexColumnName)
                        .put(", indexTxn=").put(indexTxn).put(']');
            }
            return writeIndexMeta(
                    ff, path, plen, indexColumnName, indexTxn, writerPtr, keySpaceSize,
                    groupFirstKeys, groupRowIdMins, groupRowIdMaxs,
                    keyDirEntries, groupKeyDirCounts, dataRowGroupBoundaries
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
            LongList groupRowIdMins,
            LongList groupRowIdMaxs,
            IntList keyDirEntries,
            IntList groupKeyDirCounts,
            LongList dataRowGroupBoundaries
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
                    ROW_ID_COLUMN,
                    FIRST_COVER_COLUMN,
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING
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
            LongList groupRowCounts
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
                PartitionEncoder.flushRowGroup(writerPtr, groupRowCounts.getQuick(i));
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
