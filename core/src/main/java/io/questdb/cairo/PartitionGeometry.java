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

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Resolves a COMPOSITE partition's PIECES on demand, and is the only thing in the engine that knows a partition can
 * have more than one.
 */
public class PartitionGeometry implements Closeable, Mutable {
    public static final int NO_PARTITION = -1;
    private static final long FLAG_DIRTY = 1L;
    /**
     * Stride of {@link #pieces}: the four longs of the on-disk piece entry, then the piece's cumulative row - the
     * running sum of the row counts before it.
     */
    private static final int LONGS_PER_PIECE = 5;
    private static final int MIN_PIECE_HOLES = 1024;
    private static final int MIN_RESOLVED_BEFORE_EVICT = 256;
    /**
     * Stride of {@link #resolved}, kept sorted by {@link #RES_PARTITION_TS} so the cache is keyed on values that never
     * change for a directory - unlike a partition index, which shifts whenever a partition is inserted or removed.
     */
    private static final int LONGS_PER_RESOLVED = 11;
    private static final int PIECE_CUMULATIVE_LO = 4;
    private static final int PIECE_ROW_COUNT = 3;
    private static final int PIECE_ROW_OFFSET = 2;
    private static final int PIECE_TS_HI = 1;
    private static final int PIECE_TS_LO = 0;
    private static final int RES_COMMITTED_RECORD_SIZE = 7;
    private static final int RES_PARTITION_TS = 0;
    private static final int RES_E = 4;
    private static final int RES_FLAGS = 9;
    private static final int RES_GEOMETRY_REF = 8;
    private static final int RES_LAST_WRITE_MICROS = 5;
    private static final int RES_NAME_TXN = 1;
    private static final int RES_PIECE_COUNT = 3;
    private static final int RES_PIECE_LO = 2;
    private static final int RES_SEQ_TXN = 10;
    private static final int RES_WRITER_TXN = 6;
    /**
     * The piece array being built by {@link #beginUpdate}/{@link #addPiece}, not yet installed.
     */
    private final LongList pending = new LongList();
    private final LongList pieces = new LongList();
    private final LongList resolved = new LongList();
    private final LongList scratch = new LongList();
    private int dirtyCount;
    private int pendingRec = NO_PARTITION;
    private FilesFacade ff;
    private PartitionGeometryFile geometryFile;
    private int memoryTag;
    private int partitionBy;
    /**
     * Longs of {@link #pieces} no resolved directory points at any more.
     */
    private int pieceHoles;
    /**
     * Slot count {@link #resolved} has to reach before {@link #evictRetiredDirectories} walks it again.
     */
    private int resolvedEvictWatermark = MIN_RESOLVED_BEFORE_EVICT;
    private String tableRoot;
    private int timestampType;
    private TxReader txReader;

    @Override
    public void clear() {
        assert dirtyCount == 0 : "geometry cleared with " + dirtyCount + " dirty directories";
        discard();
    }

    @Override
    public void close() {
        discard();
        geometryFile = Misc.free(geometryFile);
    }

    /**
     * Drops every cached resolution without the dirty assertion, for a rollback that abandons the
     * in-memory state wholesale and is about to re-read {@code _txn}.
     */
    public void discard() {
        pieces.clear();
        resolved.clear();
        pieceHoles = 0;
        dirtyCount = 0;
        resolvedEvictWatermark = MIN_RESOLVED_BEFORE_EVICT;
    }

    /**
     * The ordinal of the piece owning {@code ts} inside {@code partitionIndex}, by the same floor rule the record level
     * uses: the piece at or below the timestamp.
     */
    public int findPiece(int partitionIndex, long ts) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return 0;
        }
        int lo = 0;
        int hi = (int) resolved.getQuick(res + RES_PIECE_COUNT) - 1;
        int found = 0;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (pieceLong(res, mid, PIECE_TS_LO) <= ts) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found;
    }

    /**
     * The ordinal of the piece holding directory-cumulative row {@code row}.
     */
    public int findPieceByRow(int partitionIndex, long row) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return 0;
        }
        final int count = (int) resolved.getQuick(res + RES_PIECE_COUNT);
        int lo = 0;
        int hi = count - 1;
        int found = count - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (pieceLong(res, mid, PIECE_CUMULATIVE_LO) + pieceLong(res, mid, PIECE_ROW_COUNT) > row) {
                found = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return found;
    }

    public long getCommittedRecordSize(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 0 : resolved.getQuick(res + RES_COMMITTED_RECORD_SIZE);
    }

    /**
     * {@code E}, the furthest file row this directory has ever held, live or dead.
     */
    public long getE(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? txReader.getPartitionSize(partitionIndex) : resolved.getQuick(res + RES_E);
    }

    public long getLastWriteMicros(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 0 : resolved.getQuick(res + RES_LAST_WRITE_MICROS);
    }

    public long getLiveRows(int partitionIndex) {
        return txReader.getPartitionSize(partitionIndex);
    }

    public int getPieceCount(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 1 : (int) resolved.getQuick(res + RES_PIECE_COUNT);
    }

    /**
     * Cumulative row at which piece {@code ordinal} of {@code partitionIndex} starts, in the directory's own
     * {@code [0, liveRows)} space.
     */
    public long getPieceCumulativeLo(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return 0;
        }
        return pieceLong(res, ordinal, PIECE_CUMULATIVE_LO);
    }

    public long getPieceRowCount(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return txReader.getPartitionSize(partitionIndex);
        }
        return pieceLong(res, ordinal, PIECE_ROW_COUNT);
    }

    public long getPieceRowOffset(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return 0;
        }
        return pieceLong(res, ordinal, PIECE_ROW_OFFSET);
    }

    /**
     * The signed shift that turns a directory-cumulative row into a file row for this piece: {@code rowOffset -
     * cumulativeLo}.
     */
    public long getPieceShift(int partitionIndex, int ordinal) {
        return getPieceRowOffset(partitionIndex, ordinal) - getPieceCumulativeLo(partitionIndex, ordinal);
    }

    /**
     * Splits the directory-cumulative row range {@code [rowLo, rowHi)} of {@code partitionIndex} into the FILE row
     * ranges of the pieces it overlaps, appending them to {@code out} as {@code (fileLo, fileHi)} pairs, {@code fileHi}
     * exclusive, in ascending cumulative-row order.
     */
    public void collectPieceFileRanges(int partitionIndex, long rowLo, long rowHi, LongList out) {
        final int pieceCount = getPieceCount(partitionIndex);
        for (int ordinal = findPieceByRow(partitionIndex, rowLo); ordinal < pieceCount && rowLo < rowHi; ordinal++) {
            final long pieceCumLo = getPieceCumulativeLo(partitionIndex, ordinal);
            final long pieceCumHi = pieceCumLo + getPieceRowCount(partitionIndex, ordinal);
            final long subLo = Math.max(rowLo, pieceCumLo);
            final long subHi = Math.min(rowHi, pieceCumHi);
            if (subLo < subHi) {
                final long shift = getPieceShift(partitionIndex, ordinal);
                out.add(subLo + shift, subHi + shift);
                rowLo = subHi;
            }
        }
    }

    public long getPieceTimestampHi(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return Numbers.LONG_NULL;
        }
        return pieceLong(res, ordinal, PIECE_TS_HI);
    }

    public long getPieceTimestampLo(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return txReader.getPartitionTimestampByIndex(partitionIndex);
        }
        return pieceLong(res, ordinal, PIECE_TS_LO);
    }

    /**
     * The partition's last-modifying seqTxn, as recorded in its committed {@code _geometry} record, or -1 when unknown.
     */
    public long getSeqTxn(int partitionIndex) {
        final int res = findResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
        return res < 0 ? -1 : resolved.getQuick(res + RES_SEQ_TXN);
    }

    public long getWriterTxn(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? -1 : resolved.getQuick(res + RES_WRITER_TXN);
    }

    public boolean hasDirty() {
        return dirtyCount > 0;
    }

    public boolean isComposite(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return false;
        }
        return resolved.getQuick(res + RES_PIECE_COUNT) > 1
                || resolved.getQuick(res + RES_E) > txReader.getPartitionSize(partitionIndex)
                || pieceLong(res, 0, PIECE_ROW_OFFSET) > 0;
    }

    /**
     * {@code memoryTag} should name the caller's own subsystem - {@code NATIVE_TABLE_READER} for a
     * {@link TableReader}, {@code NATIVE_TABLE_WRITER} for a {@link TxWriter}, {@code NATIVE_O3} for a
     * job-scoped consumer - so the scratch buffer this resolves into is attributed to whoever holds it.
     */
    public PartitionGeometry of(FilesFacade ff, TxReader txReader, String tableRoot, int timestampType, int partitionBy, int memoryTag) {
        this.ff = ff;
        this.txReader = txReader;
        this.tableRoot = tableRoot;
        this.timestampType = timestampType;
        this.partitionBy = partitionBy;
        this.memoryTag = memoryTag;
        discard();
        return this;
    }

    /**
     * Makes {@code partitionIndex}'s pieces resident.
     */
    public void resolve(int partitionIndex) {
        resolveInternal(partitionIndex);
    }

    public void beginUpdate(int partitionIndex) {
        resolveInternal(partitionIndex);
        pending.clear();
        pendingRec = partitionIndex;
    }

    /**
     * Drops what {@link #beginUpdate} opened without installing it.
     */
    public void abandonUpdate() {
        pending.clear();
        pendingRec = NO_PARTITION;
    }

    public void addPiece(long tsLo, long tsHi, long rowOffset, long rowCount) {
        assert pendingRec != NO_PARTITION : "addPiece outside beginUpdate/commitUpdate";
        assert pending.size() == 0 || tsLo > pending.getQuick(pending.size() - LONGS_PER_PIECE + PIECE_TS_LO)
                : "pieces must ascend by tsLo";
        final long cumulativeLo = pending.size() == 0
                ? 0
                : pending.getQuick(pending.size() - LONGS_PER_PIECE + PIECE_CUMULATIVE_LO)
                  + pending.getQuick(pending.size() - LONGS_PER_PIECE + PIECE_ROW_COUNT);
        pending.add(tsLo, tsHi, rowOffset, rowCount);
        pending.add(cumulativeLo);
    }

    /**
     * Replaces {@code partitionIndex}'s piece array with what {@link #addPiece} built and raises its {@code E}.
     */
    public void commitUpdate(int partitionIndex, long e) {
        assert pendingRec == partitionIndex : "commitUpdate for a record that beginUpdate did not open";
        int slot = findResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
        if (slot < 0) {
            slot = insertResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
            resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, 0);
            resolved.setQuick(slot + RES_GEOMETRY_REF, -1L);
            resolved.setQuick(slot + RES_WRITER_TXN, -1L);
            resolved.setQuick(slot + RES_SEQ_TXN, -1L);
        } else {
            pieceHoles += (int) resolved.getQuick(slot + RES_PIECE_COUNT) * LONGS_PER_PIECE;
        }
        final int lo = pieces.size();
        pieces.add(pending);
        resolved.setQuick(slot + RES_PIECE_LO, lo);
        resolved.setQuick(slot + RES_PIECE_COUNT, pending.size() / LONGS_PER_PIECE);
        resolved.setQuick(slot + RES_E, Math.max(resolved.getQuick(slot + RES_E), e));
        if ((resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) == 0) {
            resolved.setQuick(slot + RES_FLAGS, resolved.getQuick(slot + RES_FLAGS) | FLAG_DIRTY);
            dirtyCount++;
        }
        pending.clear();
        pendingRec = NO_PARTITION;
    }

    public boolean isDirty(int partitionIndex) {
        final int slot = findResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
        return slot > -1 && (resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) != 0;
    }

    /**
     * Appends {@code partitionIndex}'s geometry as one full-snapshot record and returns the slot-3 word {@code _txn}
     * must publish for it.
     */
    public long publish(int partitionIndex, long writerTxn, long seqTxn, long nowMicros, int commitMode) {
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
        final int slot = findResolved(partitionTimestamp, nameTxn);
        assert slot > -1 : "publish of an unresolved directory";
        if (geometryFile == null) {
            geometryFile = new PartitionGeometryFile(memoryTag);
        }
        final int count = (int) resolved.getQuick(slot + RES_PIECE_COUNT);
        final int lo = (int) resolved.getQuick(slot + RES_PIECE_LO);
        long liveRows = 0;
        geometryFile.beginRecord(writerTxn, seqTxn, count);
        for (int p = 0; p < count; p++) {
            final int at = lo + p * LONGS_PER_PIECE;
            geometryFile.addPiece(
                    pieces.getQuick(at + PIECE_TS_LO),
                    pieces.getQuick(at + PIECE_TS_HI),
                    pieces.getQuick(at + PIECE_ROW_OFFSET),
                    pieces.getQuick(at + PIECE_ROW_COUNT)
            );
            liveRows += pieces.getQuick(at + PIECE_ROW_COUNT);
        }
        final long e = resolved.getQuick(slot + RES_E);
        geometryFile.setPhysicalRows(e);
        geometryFile.setLiveRows(liveRows);
        geometryFile.setLastWriteMicros(nowMicros);

        final long committedRef = resolved.getQuick(slot + RES_GEOMETRY_REF);
        int generation = committedRef == -1L ? 0 : TxReader.geometryGeneration(committedRef);
        long offset = committedRef == -1L
                ? 0
                : TxReader.geometryOffset(committedRef) + resolved.getQuick(slot + RES_COMMITTED_RECORD_SIZE);
        // Every record is a full snapshot (see the class doc), so rotating costs nothing beyond starting a fresh file:
        // this record, not a copy of what came before, is what the new generation opens with.
        if (committedRef != -1L && offset + geometryFile.getRecordSize() > PartitionGeometryFile.MAX_FILE_SIZE) {
            generation++;
            offset = 0;
            if (generation > TxReader.PARTITION_GEOMETRY_MAX_GENERATION) {
                throw CairoException.critical(0)
                        .put("partition geometry generations exhausted [partitionTimestamp=").put(partitionTimestamp)
                        .put(", nameTxn=").put(nameTxn)
                        .put(']');
            }
        }
        assert (offset & ((1L << TxReader.PARTITION_GEOMETRY_OFFSET_UNIT_SHIFT) - 1)) == 0
                : "geometry offset must be 8-byte aligned";
        final long packedOffset = offset >>> TxReader.PARTITION_GEOMETRY_OFFSET_UNIT_SHIFT;
        if ((packedOffset & ~TxReader.PARTITION_GEOMETRY_OFFSET_MASK) != 0) {
            throw CairoException.critical(0)
                    .put("partition geometry file is full [partitionTimestamp=").put(partitionTimestamp)
                    .put(", nameTxn=").put(nameTxn)
                    .put(", offset=").put(offset)
                    .put(']');
        }
        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long size = geometryFile.append(ff, path, generation, offset, commitMode);

        resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, size);
        resolved.setQuick(slot + RES_WRITER_TXN, writerTxn);
        resolved.setQuick(slot + RES_SEQ_TXN, seqTxn);
        resolved.setQuick(slot + RES_LAST_WRITE_MICROS, nowMicros);
        final long ref = TxReader.PARTITION_COMPOSITE_FLAG
                | ((long) generation << TxReader.PARTITION_GEOMETRY_GENERATION_BIT_OFFSET)
                | packedOffset;
        resolved.setQuick(slot + RES_GEOMETRY_REF, ref);
        if ((resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) != 0) {
            resolved.setQuick(slot + RES_FLAGS, resolved.getQuick(slot + RES_FLAGS) & ~FLAG_DIRTY);
            dirtyCount--;
        }
        compactPiecesIfNeeded();
        return ref;
    }

    private void compactPieces() {
        scratch.clear();
        for (int i = 0, n = resolved.size(); i < n; i += LONGS_PER_RESOLVED) {
            final int lo = (int) resolved.getQuick(i + RES_PIECE_LO);
            final int count = (int) resolved.getQuick(i + RES_PIECE_COUNT);
            final int newLo = scratch.size();
            for (int p = 0, m = count * LONGS_PER_PIECE; p < m; p++) {
                scratch.add(pieces.getQuick(lo + p));
            }
            resolved.setQuick(i + RES_PIECE_LO, newLo);
        }
        pieces.clear();
        pieces.add(scratch);
        pieceHoles = 0;
    }

    /**
     * Amortised reclaim of {@link #pieces}.
     */
    private void compactPiecesIfNeeded() {
        if (pieceHoles > MIN_PIECE_HOLES && pieceHoles > pieces.size() - pieceHoles) {
            compactPieces();
        }
    }

    /**
     * Drops resolutions for directories {@code _txn} no longer names - a partition that was dropped, or one a rewrite
     * retired under a fresh {@code nameTxn}.
     */
    private void evictRetiredDirectories() {
        int keep = 0;
        for (int i = 0, n = resolved.size(); i < n; i += LONGS_PER_RESOLVED) {
            final long partitionTimestamp = resolved.getQuick(i + RES_PARTITION_TS);
            final int indexRaw = txReader.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);
            if (indexRaw > -1 && txReader.getPartitionNameTxnByRawIndex(indexRaw) == resolved.getQuick(i + RES_NAME_TXN)) {
                if (keep != i) {
                    for (int f = 0; f < LONGS_PER_RESOLVED; f++) {
                        resolved.setQuick(keep + f, resolved.getQuick(i + f));
                    }
                }
                keep += LONGS_PER_RESOLVED;
            } else {
                pieceHoles += (int) resolved.getQuick(i + RES_PIECE_COUNT) * LONGS_PER_PIECE;
            }
        }
        resolved.setPos(keep);
        resolvedEvictWatermark = Math.max(MIN_RESOLVED_BEFORE_EVICT, 2 * (keep / LONGS_PER_RESOLVED));
    }

    /**
     * Binary search of {@link #resolved} by {@code partitionTimestamp}, then a linear walk over the equal-timestamp
     * run for {@code nameTxn}. Returns the slot, or {@code -1}.
     */
    private int findResolved(long partitionTimestamp, long nameTxn) {
        final int blocks = resolved.size() / LONGS_PER_RESOLVED;
        int lo = 0;
        int hi = blocks - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long ts = resolved.getQuick(mid * LONGS_PER_RESOLVED + RES_PARTITION_TS);
            if (ts < partitionTimestamp) {
                lo = mid + 1;
            } else if (ts > partitionTimestamp) {
                hi = mid - 1;
            } else {
                int i = mid;
                while (i > 0 && resolved.getQuick((i - 1) * LONGS_PER_RESOLVED + RES_PARTITION_TS) == partitionTimestamp) {
                    i--;
                }
                for (; i < blocks && resolved.getQuick(i * LONGS_PER_RESOLVED + RES_PARTITION_TS) == partitionTimestamp; i++) {
                    if (resolved.getQuick(i * LONGS_PER_RESOLVED + RES_NAME_TXN) == nameTxn) {
                        return i * LONGS_PER_RESOLVED;
                    }
                }
                return -1;
            }
        }
        return -1;
    }

    private int insertResolved(long partitionTimestamp, long nameTxn) {
        final int n = resolved.size();
        int at = n;
        for (int i = 0; i < n; i += LONGS_PER_RESOLVED) {
            final long ts = resolved.getQuick(i + RES_PARTITION_TS);
            if (ts > partitionTimestamp || (ts == partitionTimestamp && resolved.getQuick(i + RES_NAME_TXN) > nameTxn)) {
                at = i;
                break;
            }
        }
        resolved.insert(at, LONGS_PER_RESOLVED);
        for (int s = 0; s < LONGS_PER_RESOLVED; s++) {
            resolved.setQuick(at + s, 0);
        }
        // The key the slot is found by, stamped here rather than by each caller: a slot carrying a zeroed
        // key is a slot findResolved cannot return, and the caller that inserted it would be the only one
        // ever able to reach it.
        resolved.setQuick(at + RES_PARTITION_TS, partitionTimestamp);
        resolved.setQuick(at + RES_NAME_TXN, nameTxn);
        return at;
    }

    private long pieceLong(int res, int ordinal, int field) {
        assert ordinal > -1 && ordinal < resolved.getQuick(res + RES_PIECE_COUNT);
        final int lo = (int) resolved.getQuick(res + RES_PIECE_LO);
        return pieces.getQuick(lo + ordinal * LONGS_PER_PIECE + field);
    }

    private int readInto(int slot, long partitionTimestamp, long nameTxn, long ref) {
        if (geometryFile == null) {
            geometryFile = new PartitionGeometryFile(memoryTag);
        }
        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        geometryFile.read(ff, path, TxReader.geometryGeneration(ref), TxReader.geometryOffset(ref));
        final int count = geometryFile.getPieceCount();
        final int lo = pieces.size();
        pieces.setPos(lo + count * LONGS_PER_PIECE);
        long cumulativeLo = 0;
        for (int p = 0; p < count; p++) {
            final int at = lo + p * LONGS_PER_PIECE;
            pieces.setQuick(at + PIECE_TS_LO, geometryFile.getPieceTimestampLo(p));
            pieces.setQuick(at + PIECE_TS_HI, geometryFile.getPieceTimestampHi(p));
            pieces.setQuick(at + PIECE_ROW_OFFSET, geometryFile.getPieceRowOffset(p));
            pieces.setQuick(at + PIECE_ROW_COUNT, geometryFile.getPieceRowCount(p));
            pieces.setQuick(at + PIECE_CUMULATIVE_LO, cumulativeLo);
            cumulativeLo += geometryFile.getPieceRowCount(p);
        }
        if (slot < 0) {
            slot = insertResolved(partitionTimestamp, nameTxn);
        }
        resolved.setQuick(slot + RES_PIECE_LO, lo);
        resolved.setQuick(slot + RES_PIECE_COUNT, count);
        resolved.setQuick(slot + RES_E, geometryFile.getPhysicalRows());
        resolved.setQuick(slot + RES_LAST_WRITE_MICROS, geometryFile.getLastWriteMicros());
        resolved.setQuick(slot + RES_WRITER_TXN, geometryFile.getWriterTxn());
        resolved.setQuick(slot + RES_SEQ_TXN, geometryFile.getSeqTxn());
        resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, PartitionGeometryFile.recordSize(count));
        resolved.setQuick(slot + RES_GEOMETRY_REF, ref);
        resolved.setQuick(slot + RES_FLAGS, 0);
        return slot;
    }

    /**
     * Finds the resolved slot for {@code partitionIndex}, reading {@code _geometry} when it is not resident at the
     * record's current {@code geometryRef}.
     */
    private int resolveInternal(int partitionIndex) {
        if (!txReader.isPartitionComposite(partitionIndex)) {
            return -1;
        }
        // Reclaim BEFORE any slot or piece index is taken below - both caches renumber themselves.
        if (pendingRec == NO_PARTITION && dirtyCount == 0 && resolved.size() / LONGS_PER_RESOLVED > resolvedEvictWatermark) {
            evictRetiredDirectories();
        }
        compactPiecesIfNeeded();
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
        final long ref = txReader.getGeometryRef(partitionIndex);
        final int slot = findResolved(partitionTimestamp, nameTxn);
        if (slot > -1) {
            if (resolved.getQuick(slot + RES_GEOMETRY_REF) == ref) {
                return slot;
            }
            // Resident at a superseded geometry: re-read in place, and the old piece span becomes a hole.
            pieceHoles += (int) resolved.getQuick(slot + RES_PIECE_COUNT) * LONGS_PER_PIECE;
        }
        return readInto(slot, partitionTimestamp, nameTxn, ref);
    }
}
