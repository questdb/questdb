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
 * Resolves a COMPOSITE partition's PIECES on demand, and is the only thing in the engine that knows a
 * partition can have more than one.
 * <p>
 * The model is parquet's, exactly. {@link TxReader} holds what {@code _txn} holds - one 4-long record per
 * partition - and knows nothing about pieces, precisely as it holds one record per parquet partition and
 * knows nothing about row groups. Loading {@code _txn} performs ZERO {@code _geometry} I/O. A partition's
 * geometry is read the first time a query or a commit actually lands on THAT partition, and stays cached
 * until its record stops naming the same {@code (partitionTimestamp, nameTxn, geometryRef)}.
 * <p>
 * So the cost is proportional to what is touched, not to what exists: a table with no composite partition
 * never opens the file at all, and a query over one partition of a thousand opens one file. Every index
 * here is the ordinary partition index - there is no second index space, and no piece ever appears in an
 * API outside this class.
 * <p>
 * A partition with no chain needs no read ever. Its record already says everything: one piece starting at
 * its own timestamp, rooted at file row 0, with {@code E == liveRows ==} slot 1.
 * <p>
 * <b>Row numbering.</b> A partition's rows are numbered CUMULATIVELY over its pieces in {@code tsLo}
 * order: piece 0 owns {@code [0, n0)}, piece 1 owns {@code [n0, n0+n1)}, and the partition owns
 * {@code [0, liveRows)}. Pieces ascend by {@code tsLo} and never overlap, so that order is also TIMESTAMP
 * order, which is what lets a partition be scanned, binary-searched and framed as one unit however many
 * pieces it has. For a row {@code r} falling in piece {@code p}:
 * <pre>
 * shift(p) = rowOffset(p) - cumulativeLo(p)
 * file_row = r + shift(p) - columnTop
 * </pre>
 * {@code shift} is SIGNED: a piece rewritten at the tail of the shared files keeps its position in
 * timestamp order while moving to a higher file row, so {@code rowOffset} is not monotone across pieces
 * and a later piece can sit at a lower file row than an earlier one.
 * <p>
 * Not thread safe. One instance belongs to one {@link TxWriter}, one {@link TableReader} or one
 * job-scoped consumer, and a resolve never runs on a worker thread.
 */
public class PartitionGeometry implements Closeable, Mutable {
    /**
     * A partition index that resolves to nothing.
     */
    public static final int NO_PARTITION = -1;
    private static final long FLAG_DIRTY = 1L;
    /**
     * Stride of {@link #pieces}: the four longs of the on-disk piece entry, then the piece's cumulative
     * row - the running sum of the row counts before it. The cumulative row is derived, never stored, and
     * exists so that every per-piece lookup is a binary search or a constant-time read. Summing it on
     * demand made {@link #getPieceCumulativeLo} linear in the ordinal, which made the frame cursors
     * quadratic in the piece count.
     */
    private static final int LONGS_PER_PIECE = 5;
    /**
     * Stride of {@link #resolved}, kept sorted by {@link #RES_PARTITION_TS} so the cache is keyed on values
     * that never change for a directory - unlike a partition index, which shifts whenever a partition is
     * inserted or removed.
     */
    private static final int LONGS_PER_RESOLVED = 10;
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
    private int partitionBy;
    /**
     * Longs of {@link #pieces} no resolved directory points at any more. A directory update writes its
     * new array at the tail rather than in place, because the array can change length; the holes are
     * reclaimed by {@link #compactPieces()} at a safe point.
     */
    private int pieceHoles;
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
    }

    /**
     * The ordinal of the piece owning {@code ts} inside {@code partitionIndex}, by the same floor rule the record
     * level uses: the piece at or below the timestamp. Returns 0 when {@code ts} falls below every piece,
     * which is a head-insert into this directory rather than a miss - the record already owns the range.
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
     * The ordinal of the piece holding directory-cumulative row {@code row}. Rows at or above the
     * directory's live count belong to the last piece, which is what the callers that address the append
     * point expect.
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

    /**
     * The byte size of the geometry record currently COMMITTED for this directory. The next append goes
     * at {@code committedOffset + committedRecordSize}, and the size has to come from the record that was
     * actually committed - the live piece count is what this commit is about to change. Never from the
     * file length: bytes past the committed offset can be a rolled-back append nothing ever referenced.
     */
    public long getCommittedRecordSize(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 0 : resolved.getQuick(res + RES_COMMITTED_RECORD_SIZE);
    }

    /**
     * {@code E}, the furthest file row this directory has ever held, live or dead. Implied by the record
     * for a directory with no chain; read from {@code _geometry} otherwise.
     */
    public long getE(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? txReader.getPartitionSize(partitionIndex) : resolved.getQuick(res + RES_E);
    }

    public long getLastWriteMicros(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 0 : resolved.getQuick(res + RES_LAST_WRITE_MICROS);
    }

    /**
     * Live rows summed over the directory's pieces. Always the record's slot 1 - the {@code _geometry}
     * header's copy is a cross-check, not the source - so this costs no read.
     */
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
     * The signed shift that turns a directory-cumulative row into a file row for this piece:
     * {@code rowOffset - cumulativeLo}. Negative when a merge-append has relocated an earlier piece above
     * a later one.
     */
    public long getPieceShift(int partitionIndex, int ordinal) {
        return getPieceRowOffset(partitionIndex, ordinal) - getPieceCumulativeLo(partitionIndex, ordinal);
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

    public long getWriterTxn(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? -1 : resolved.getQuick(res + RES_WRITER_TXN);
    }

    public boolean hasDirty() {
        return dirtyCount > 0;
    }

    /**
     * Whether ANY record in the table has ever had a {@code _geometry} chain. Resident, zero I/O, and the
     * thing that keeps an unsplit table off {@code _geometry} entirely. Deliberately an
     * over-approximation: a directory keeps its chain after folding back to one piece, so this can be
     * true for a table with no composite directory left.
     */
    public boolean hasGeometryChain() {
        for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
            if (txReader.hasGeometryChain(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Exact: more than one piece, or dead space above the live rows, or rows starting above file row 0.
     * Resolves the directory, unlike {@link TxReader#hasGeometryChain(int)}.
     */
    public boolean isComposite(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return false;
        }
        return resolved.getQuick(res + RES_PIECE_COUNT) > 1
                || resolved.getQuick(res + RES_E) > txReader.getPartitionSize(partitionIndex)
                || pieceLong(res, 0, PIECE_ROW_OFFSET) > 0;
    }

    public PartitionGeometry of(FilesFacade ff, TxReader txReader, String tableRoot, int timestampType, int partitionBy) {
        this.ff = ff;
        this.txReader = txReader;
        this.tableRoot = tableRoot;
        this.timestampType = timestampType;
        this.partitionBy = partitionBy;
        discard();
        return this;
    }

    /**
     * Makes {@code partitionIndex}'s pieces resident. A no-op for a partition already resolved at the same
     * {@code geometryRef}, and for a partition with no geometry chain, which needs no file at all.
     */
    public void resolve(int partitionIndex) {
        resolveInternal(partitionIndex);
    }

    /**
     * Starts building the partition's new piece array. Follow with {@link #addPiece} calls in ascending
     * {@code tsLo} order and finish with {@link #commitUpdate}. The directory's current array stays
     * readable until then, so a plan can be computed from it while the new one is assembled.
     */
    public void beginUpdate(int partitionIndex) {
        resolveInternal(partitionIndex);
        pending.clear();
        pendingRec = partitionIndex;
    }

    /**
     * Drops what {@link #beginUpdate} opened without installing it. For a partition whose planned shape
     * turns out to be the ordinary one - a single piece over the whole of its files - which needs no
     * geometry record at all.
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
     * The array is written at the tail of {@link #pieces} rather than in place, because it can change
     * length; the span it vacates becomes a hole {@link #compactPieces()} reclaims at publish time.
     * <p>
     * {@code E} is GROW-ONLY: the region above the live rows is dead space a pinned reader may still
     * address, so a directory that loses its furthest piece keeps the extent it reached.
     */
    public void commitUpdate(int partitionIndex, long e) {
        assert pendingRec == partitionIndex : "commitUpdate for a record that beginUpdate did not open";
        int slot = findResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
        if (slot < 0) {
            slot = insertResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
            resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, 0);
            resolved.setQuick(slot + RES_GEOMETRY_REF, -1L);
            resolved.setQuick(slot + RES_WRITER_TXN, -1L);
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
     * must publish for it. The caller writes that word into the record; this class never touches
     * {@code _txn}, so the two writes stay one transaction under the caller's control.
     * <p>
     * The append goes at {@code committedOffset + committedRecordSize} - the offset out of the COMMITTED
     * slot 3, the size out of the record this directory was resolved from. Never the file length: bytes
     * past the committed offset can be a rolled-back append no transaction ever referenced.
     * <p>
     * Ordering is the crash-consistency contract and belongs to the caller: append here and fsync, THEN
     * commit {@code _txn}. A crash between the two leaves an unreferenced record, which is harmless; the
     * reverse order is durably inconsistent.
     */
    public long publish(int partitionIndex, long writerTxn, long nowMicros, int commitMode) {
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
        final int slot = findResolved(partitionTimestamp, nameTxn);
        assert slot > -1 : "publish of an unresolved directory";
        if (geometryFile == null) {
            geometryFile = new PartitionGeometryFile();
        }
        final int count = (int) resolved.getQuick(slot + RES_PIECE_COUNT);
        final int lo = (int) resolved.getQuick(slot + RES_PIECE_LO);
        long liveRows = 0;
        geometryFile.beginRecord(writerTxn, count);
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
        final int generation = committedRef == -1L ? 0 : TxReader.geometryGeneration(committedRef);
        final long offset = committedRef == -1L
                ? 0
                : TxReader.geometryOffset(committedRef) + resolved.getQuick(slot + RES_COMMITTED_RECORD_SIZE);
        if ((offset & ~TxReader.PARTITION_GEOMETRY_OFFSET_MASK) != 0) {
            throw CairoException.critical(0)
                    .put("partition geometry file is full [partitionTimestamp=").put(partitionTimestamp)
                    .put(", nameTxn=").put(nameTxn)
                    .put(", offset=").put(offset)
                    .put("]; generation rotation is not implemented");
        }
        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long size = geometryFile.append(ff, path, generation, offset, commitMode);

        resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, size);
        resolved.setQuick(slot + RES_WRITER_TXN, writerTxn);
        resolved.setQuick(slot + RES_LAST_WRITE_MICROS, nowMicros);
        final long ref = TxReader.PARTITION_COMPOSITE_FLAG
                | ((long) generation << TxReader.PARTITION_GEOMETRY_GENERATION_BIT_OFFSET)
                | offset;
        resolved.setQuick(slot + RES_GEOMETRY_REF, ref);
        if ((resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) != 0) {
            resolved.setQuick(slot + RES_FLAGS, resolved.getQuick(slot + RES_FLAGS) & ~FLAG_DIRTY);
            dirtyCount--;
        }
        compactPieces();
        return ref;
    }

    /**
     * Reclaims the holes left by in-place directory updates. Safe only when nothing holds a piece span
     * index across the call, which is why it runs at publish time and nowhere else.
     */
    private void compactPieces() {
        if (pieceHoles == 0) {
            return;
        }
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
            geometryFile = new PartitionGeometryFile();
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
        resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, PartitionGeometryFile.recordSize(count));
        resolved.setQuick(slot + RES_GEOMETRY_REF, ref);
        resolved.setQuick(slot + RES_FLAGS, 0);
        return slot;
    }

    /**
     * Finds the resolved slot for {@code partitionIndex}, reading {@code _geometry} when it is not resident at the
     * record's current {@code geometryRef}. Returns {@code -1} for a record that implies its own single
     * piece, which is every record of an unsplit table.
     */
    private int resolveInternal(int partitionIndex) {
        if (!txReader.hasGeometryChain(partitionIndex)) {
            return -1;
        }
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
